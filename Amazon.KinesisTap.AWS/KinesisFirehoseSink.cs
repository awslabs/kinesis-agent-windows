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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Reactive.Linq;

using Amazon.KinesisFirehose;
using Amazon.KinesisFirehose.Model;
using Amazon.Runtime;
using Microsoft.Extensions.Logging;

using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;

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
                    new TokenBucket(1, _maxRecordsPerSecond/200), //Number of API calls per second. For simplicity, we tie to _maxRecordsPerSecond
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

        /// <summary>
        /// Combining multiple small records to a large record under 5K to save customer cost.
        /// https://aws.amazon.com/kinesis/data-firehose/pricing/
        /// </summary>
        public bool CanCombineRecords { get; set; }

        /// <summary>
        /// Combine multiple records to a single record up to 5 KB
        /// </summary>
        /// <param name="records"></param>
        /// <returns></returns>
        public static List<Record> CombineRecords(List<Record> records)
        {
            List<Record> combinedRecords = new List<Record>();
            Record prevRecord = null;
            foreach (var record in records)
            {
                if (prevRecord == null)
                {
                    prevRecord = record;
                }
                else
                {
                    if (prevRecord.Data.Length + record.Data.Length <= 5000)
                    {
                        Combine(prevRecord, record);
                    }
                    else
                    {
                        prevRecord.Data.Position = 0; //Reset position for the next reader
                        combinedRecords.Add(prevRecord);
                        prevRecord = record;
                    }
                }
            }
            if (prevRecord != null)
            {
                prevRecord.Data.Position = 0;
                combinedRecords.Add(prevRecord);
            }

            return combinedRecords;
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

        protected override async Task OnNextAsync(List<Envelope<Record>> envelopes, long batchBytes)
        {
            _logger?.LogDebug($"KinesisFirehoseSink {this.Id} sending {envelopes.Count} records {batchBytes} bytes.");

            DateTime utcNow = DateTime.UtcNow;
            _clientLatency = (long)envelopes.Average(r => (utcNow - r.Timestamp).TotalMilliseconds);

            long elapsedMilliseconds = Utility.GetElapsedMilliseconds();
            try
            {
                _recordsAttempted += envelopes.Count;
                _bytesAttempted += batchBytes;
                List<Record> records = envelopes.Select(r => r.Data).ToList();
                if (this.CanCombineRecords)
                {
                    records = CombineRecords(records);
                }

                PutRecordBatchResponse response = await _firehoseClient.PutRecordBatchAsync(_deliveryStreamName, records);
                _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                if (response.FailedPutCount > 0 && response.RequestResponses != null)
                {
                    _throttle.SetError();
                    _recoverableServiceErrors++;
                    _recordsSuccess += envelopes.Count - response.FailedPutCount;
                    _logger?.LogError($"KinesisFirehoseSink client {this.Id} BatchRecordCount={envelopes.Count} FailedPutCount={response.FailedPutCount} Attempt={_throttle.ConsecutiveErrorCount}");
                    List<Envelope<Record>> requeueRecords = new List<Envelope<Record>>();
                    for (int i = 0;  i < response.RequestResponses.Count; i++)
                    {
                        var reqResponse = response.RequestResponses[i];
                        if (!string.IsNullOrEmpty(reqResponse.ErrorCode))
                        {
                            requeueRecords.Add(envelopes[i]);
                            //When there is error, reqResponse.RecordId would be null. So we have to use the sequence number within the batch here.
                            if (_throttle.ConsecutiveErrorCount >= _maxAttempts)
                            {
                                _logger?.LogDebug($"Record {i} error {reqResponse.ErrorCode}: {reqResponse.ErrorMessage}");
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
                        throw new AmazonKinesisFirehoseException($"Messages discarded after {_throttle.ConsecutiveErrorCount} attempts.");
                    }
                }
                else
                {
                    _throttle.SetSuccess();
                    _recordsSuccess += envelopes.Count;
                    _logger?.LogDebug($"KinesisFirehoseSink {this.Id} successfully sent {envelopes.Count} records {batchBytes} bytes.");

                    this.SaveBookmarks(envelopes);
                }
            }
            catch (Exception ex)
            {
                _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                _throttle.SetError();
                if (IsRecoverableException(ex) 
                    && _buffer.Requeue(envelopes, _throttle.ConsecutiveErrorCount < _maxAttempts))
                {
                    _recoverableServiceErrors++;
                    _recordsFailedRecoverable += envelopes.Count;
                    if (LogThrottler.ShouldWrite(LogThrottler.CreateLogTypeId(this.GetType().FullName, "OnNextAsync", "Requeued", this.Id), TimeSpan.FromMinutes(5)))
                    {
                        _logger?.LogWarning($"KinesisFirehoseSink {this.Id} requeued request after exception (attempt {_throttle.ConsecutiveErrorCount}): {ex.ToMinimized()}");
                    }
                }
                else
                {
                    _nonrecoverableServiceErrors++;
                    _recordsFailedNonrecoverable += envelopes.Count;
                    _logger?.LogError($"KinesisFirehoseSink {this.Id} client exception after {_throttle.ConsecutiveErrorCount} attempts: {ex.ToMinimized()}");
                }
            }
            PublishMetrics(MetricsConstants.KINESIS_FIREHOSE_PREFIX);
        }

        protected override ISerializer<List<Envelope<Record>>> GetSerializer()
        {
            return AWSSerializationUtility.FirehoseRecordListBinarySerializer;
        }

        protected override bool IsRecoverableException(Exception ex)
        {
            return base.IsRecoverableException(ex) || ex is ServiceUnavailableException;
        }

        private static void Combine(Record prevRecord, Record record)
        {
            prevRecord.Data.Position = prevRecord.Data.Length; //Set the position to the end for append
            prevRecord.Data.Write(record.Data.ToArray(), 0, (int)record.Data.Length);
        }
    }
}
