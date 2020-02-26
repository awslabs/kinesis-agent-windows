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
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Microsoft.Extensions.Logging;

using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;

namespace Amazon.KinesisTap.AWS
{
    public class KinesisStreamSink : KinesisSink<PutRecordsRequestEntry>
    {
        private IAmazonKinesis _kinesisClient; 
        private string _streamName;

        public KinesisStreamSink(
            IPlugInContext context,
            IAmazonKinesis kineisClient
            ) : base(context, 1, 500, 5 * 1024 * 1024)
        {
            if (_count > 500)
            {
                throw new ArgumentException("The maximum buffer size for kinesis stream is 500");
            }

            //Set Defaults for 1 shard
            if (!int.TryParse(_config[ConfigConstants.RECORDS_PER_SECOND], out _maxRecordsPerSecond))
            {
                _maxRecordsPerSecond = 1000;
            }

            if (!long.TryParse(_config[ConfigConstants.BYTES_PER_SECOND], out _maxBytesPerSecond))
            {
                _maxBytesPerSecond = 1024 * 1024;
            }

            _kinesisClient = kineisClient;
            _streamName = ResolveVariables(_config["StreamName"]);

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
                new Dictionary<string, MetricValue>()
                {
                    { MetricsConstants.KINESIS_STREAM_PREFIX + MetricsConstants.BYTES_ATTEMPTED, MetricValue.ZeroBytes },
                    { MetricsConstants.KINESIS_STREAM_PREFIX + MetricsConstants.RECORDS_ATTEMPTED, MetricValue.ZeroCount },
                    { MetricsConstants.KINESIS_STREAM_PREFIX + MetricsConstants.RECORDS_FAILED_NONRECOVERABLE, MetricValue.ZeroCount },
                    { MetricsConstants.KINESIS_STREAM_PREFIX + MetricsConstants.RECORDS_FAILED_RECOVERABLE, MetricValue.ZeroCount },
                    { MetricsConstants.KINESIS_STREAM_PREFIX + MetricsConstants.RECORDS_SUCCESS, MetricValue.ZeroCount },
                    { MetricsConstants.KINESIS_STREAM_PREFIX + MetricsConstants.RECOVERABLE_SERVICE_ERRORS, MetricValue.ZeroCount },
                    { MetricsConstants.KINESIS_STREAM_PREFIX + MetricsConstants.NONRECOVERABLE_SERVICE_ERRORS, MetricValue.ZeroCount }
                });
            _logger?.LogInformation($"KinesisStreamSink id {this.Id} for StreamName {_streamName} started.");
        }

        public override void Stop()
        {
            base.Stop();
            _logger?.LogInformation($"KinesisStreamSink id {this.Id} for StreamName {_streamName} stopped.");
        }

        protected override PutRecordsRequestEntry CreateRecord(string record, IEnvelope envelope)
        {
            return new PutRecordsRequestEntry()
            {
                PartitionKey = "" + (Utility.Random.NextDouble() * 1000000),
                Data = Utility.StringToStream(record, ConfigConstants.NEWLINE)
            };
        }

        protected override long GetRecordSize(Envelope<PutRecordsRequestEntry> record)
        {
            long recordSize = record.Data.Data.Length + UTF8Encoding.UTF8.GetByteCount(record.Data.PartitionKey);
            const long ONE_MEGABYTES = 1024 * 1024;
            if (recordSize > ONE_MEGABYTES)
            {
                _recordsFailedNonrecoverable++;
                throw new ArgumentException("The maximum record size is 1 MB. Record discarded.");
            }
            return recordSize;
        }

        protected override async Task OnNextAsync(List<Envelope<PutRecordsRequestEntry>> records, long batchBytes)
        {
            _logger?.LogDebug($"KinesisStreamSink {this.Id} sending {records.Count} records {batchBytes} bytes.");

            DateTime utcNow = DateTime.UtcNow;
            _clientLatency = (long)records.Average(r => (utcNow - r.Timestamp).TotalMilliseconds);

            long elapsedMilliseconds = Utility.GetElapsedMilliseconds();
            try
            {
                _recordsAttempted += records.Count;
                _bytesAttempted += batchBytes;
                var response = await _kinesisClient.PutRecordsAsync(new PutRecordsRequest()
                {
                    StreamName = _streamName,
                    Records = records.Select(r => r.Data).ToList()
                });
                _throttle.SetSuccess();
                _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                _recordsSuccess += records.Count;
                _logger?.LogDebug($"KinesisStreamSink {this.Id} successfully sent {records.Count} records {batchBytes} bytes.");

                this.SaveBookmarks(records);
            }
            catch (Exception ex)
            {
                _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                _throttle.SetError();
                if (this.IsRecoverableException(ex) && _buffer.Requeue(records, _throttle.ConsecutiveErrorCount < _maxAttempts))
                {
                    _recoverableServiceErrors++;
                    _recordsFailedRecoverable += records.Count;
                    if (LogThrottler.ShouldWrite(LogThrottler.CreateLogTypeId(this.GetType().FullName, "OnNextAsync", "Requeued", this.Id), TimeSpan.FromMinutes(5)))
                    {
                        _logger?.LogWarning($"KinesisStreamSink {this.Id} requeued request after exception (attempt {_throttle.ConsecutiveErrorCount}): {ex.ToMinimized()}");
                    }
                }
                else
                {
                    _nonrecoverableServiceErrors++;
                    _recordsFailedNonrecoverable += records.Count;
                    _logger?.LogError($"KinesisStreamSink {this.Id} client exception after {_throttle.ConsecutiveErrorCount} attempts: {ex.ToMinimized()}");
                }
            }
            PublishMetrics(MetricsConstants.KINESIS_STREAM_PREFIX);
        }

        protected override ISerializer<List<Envelope<PutRecordsRequestEntry>>> GetSerializer()
        {
            return AWSSerializationUtility.PutRecordsRequestEntryListBinarySerializer;
        }

        protected override bool IsRecoverableException(Exception ex)
        {
            return base.IsRecoverableException(ex) || ex is ProvisionedThroughputExceededException;
        }
    }
}
