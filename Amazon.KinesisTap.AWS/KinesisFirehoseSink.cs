/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
using Amazon.KinesisTap.Core;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using Amazon.KinesisFirehose;
using Amazon.KinesisFirehose.Model;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Diagnostics;
using Amazon.KinesisTap.Core.Metrics;
using Amazon.Runtime;
using System.Net;

namespace Amazon.KinesisTap.AWS
{
    public class KinesisFirehoseSink : KinesisSink<Record>
    {
        private IAmazonKinesisFirehose _firehoseClient;
        private readonly string _deliveryStreamName;

        //http://docs.aws.amazon.com/firehose/latest/dev/limits.html
        public KinesisFirehoseSink(IPlugInContext context,
            IAmazonKinesisFirehose firehoseClient
            ) : base(context, 1, 500, 4 * 1024 * 1024)
        {
            if (_count > 500)
            {
                throw new ArgumentException("The maximum buffer size for firehose is 500");
            }

            //Set Defaults
            if (!int.TryParse(_config[ConfigConstants.RECORDS_PER_SECOND], out _maxRecordsPerSecond))
            {
                _maxRecordsPerSecond = 5000;
            }

            if (!long.TryParse(_config[ConfigConstants.BYTES_PER_SECOND], out _maxBytesPerSecond))
            {
                _maxBytesPerSecond = 5 * 1024 * 1024;
            }

            _firehoseClient = firehoseClient;
            _deliveryStreamName = ResolveVariables(_config["StreamName"]);

            _throttle = new AdaptiveThrottle(
                new TokenBucket[]
                {
                                new TokenBucket(_count, _maxRecordsPerSecond), 
                                new TokenBucket(_maxBatchSize, _maxBytesPerSecond) 
                },
                _backoffFactor,
                _recoveryFactor,
                _minRateAdjustmentFactor);
        }

        public override void Start()
        {
            base.Start();
            _metrics?.InitializeCounters(this.Id, MetricsConstants.CATEGORY_SINK, CounterTypeEnum.Increment,
                 new Dictionary<string,  MetricValue> ()
                 {
                        { MetricsConstants.KINESIS_FIREHOSE_PREFIX + MetricsConstants.BYTES_ATTEMPTED, MetricValue.ZeroBytes },
                        { MetricsConstants.KINESIS_FIREHOSE_PREFIX + MetricsConstants.RECORDS_ATTEMPTED, MetricValue.ZeroCount },
                        { MetricsConstants.KINESIS_FIREHOSE_PREFIX + MetricsConstants.RECORDS_FAILED_NONRECOVERABLE, MetricValue.ZeroCount },
                        { MetricsConstants.KINESIS_FIREHOSE_PREFIX + MetricsConstants.RECORDS_FAILED_RECOVERABLE, MetricValue.ZeroCount },
                        { MetricsConstants.KINESIS_FIREHOSE_PREFIX + MetricsConstants.RECORDS_SUCCESS, MetricValue.ZeroCount },
                        { MetricsConstants.KINESIS_FIREHOSE_PREFIX + MetricsConstants.RECOVERABLE_SERVICE_ERRORS, MetricValue.ZeroCount },
                        { MetricsConstants.KINESIS_FIREHOSE_PREFIX + MetricsConstants.NONRECOVERABLE_SERVICE_ERRORS, MetricValue.ZeroCount }
                 });
            _logger?.LogInformation($"KinesisFirehoseSink id {this.Id} for StreamName {_deliveryStreamName} started.");
        }

        public override void Stop()
        {
            base.Stop();
            _logger?.LogInformation($"KinesisFirehoseSink id {this.Id} for StreamName {_deliveryStreamName} stopped.");
        }

        protected override Record CreateRecord(string record, IEnvelope envelope)
        {
            //Use a newline delimiter per recommendation of http://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html
            return new Record()
            {
                Data = Utility.StringToStream(record, ConfigConstants.NEWLINE)
            };
        }

        protected override long GetRecordSize(Envelope<Record> record)
        {
            long recordSize = record.Data.Data.Length;
            const long ONE_THOUSAND_KILOBYTES = 1000 * 1024;
            if (recordSize > ONE_THOUSAND_KILOBYTES)
            {
                _recordsFailedNonrecoverable++;
                throw new ArgumentException("The maximum record size is 1000 KB. Record discarded.");
            }
            return recordSize;
        }

        protected override async Task OnNextAsync(List<Envelope<Record>> records, long batchBytes)
        {
            _logger?.LogDebug($"KinesisFirehoseSink {this.Id} sending {records.Count} records {batchBytes} bytes.");

            DateTime utcNow = DateTime.UtcNow;
            _clientLatency = (long)records.Average(r => (utcNow - r.Timestamp).TotalMilliseconds);

            long elapsedMilliseconds = Utility.GetElapsedMilliseconds();
            try
            {
                _recordsAttempted += records.Count;
                _bytesAttempted += batchBytes;
                PutRecordBatchResponse response = await _firehoseClient.PutRecordBatchAsync(_deliveryStreamName, 
                    records.Select(r => r.Data).ToList());
                _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                if (response.FailedPutCount > 0 && response.RequestResponses != null)
                {
                    _throttle.SetError();
                    _recoverableServiceErrors++;
                    _recordsSuccess += records.Count - response.FailedPutCount;
                    _logger?.LogError($"KinesisFirehoseSink client {this.Id} BatchRecordCount={records.Count} FailedPutCount={response.FailedPutCount} Attempt={_throttle.ConsecutiveErrorCount}");
                    List<Envelope<Record>> requeueRecords = new List<Envelope<Record>>();
                    for (int i = 0;  i < response.RequestResponses.Count; i++)
                    {
                        var reqResponse = response.RequestResponses[i];
                        if (!string.IsNullOrEmpty(reqResponse.ErrorCode))
                        {
                            requeueRecords.Add(records[i]);
                            //When there is error, reqResponse.RecordId would be null. So we have to use the sequence number within the batch here.
                            if (_throttle.ConsecutiveErrorCount >= _maxAttempts)
                            {
                                _logger?.LogError($"Record {i} error {reqResponse.ErrorCode}: {reqResponse.ErrorMessage}");
                            }
                        }
                    }
                    if (_buffer.Requeue(requeueRecords, _throttle.ConsecutiveErrorCount < _maxAttempts))
                    {
                        _recordsFailedRecoverable += response.FailedPutCount;
                    }
                    else
                    {
                        _recordsFailedNonrecoverable += response.FailedPutCount;
                        throw new AmazonKinesisFirehoseException($"Messages descarded after {_throttle.ConsecutiveErrorCount} attempts.");
                    }
                }
                else
                {
                    _throttle.SetSuccess();
                    _recordsSuccess += records.Count;
                    _logger?.LogDebug($"KinesisFirehoseSink {this.Id} succesfully sent {records.Count} records {batchBytes} bytes.");
                }
            }
            catch (Exception ex)
            {
                _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                _throttle.SetError();
                if (IsRecoverableException(ex) 
                    && _buffer.Requeue(records, _throttle.ConsecutiveErrorCount < _maxAttempts))
                {
                    _recoverableServiceErrors++;
                    _recordsFailedRecoverable += records.Count;
                    _logger?.LogWarning($"KinesisFirehoseSink client {this.Id} Service Unavailable. Request requeued. Attempts {_throttle.ConsecutiveErrorCount}");
                }
                else
                {
                    _nonrecoverableServiceErrors++;
                    _recordsFailedNonrecoverable += records.Count;
                    _logger?.LogError($"KinesisFirehoseSink client {this.Id} exception: {ex}");
                }
            }
            PublishMetrics(MetricsConstants.KINESIS_FIREHOSE_PREFIX);
        }

        protected override ISerializer<List<Envelope<Record>>> GetSerializer()
        {
            return AWSSerializationUtility.FirehoseRecordListBinarySerializer;
        }

        private bool IsRecoverableException(Exception ex)
        {
            return ex is ServiceUnavailableException
                || (ex is AmazonServiceException 
                && ex?.InnerException is WebException);
        }
    }
}
