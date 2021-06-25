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
using System.Diagnostics;
using System.Linq;
using System.Reactive.Linq;
using System.Threading.Tasks;
using System.Timers;
using Amazon.KinesisFirehose;
using Amazon.KinesisFirehose.Model;
using Amazon.KinesisTap.AWS.Failover;
using Amazon.KinesisTap.AWS.Failover.Strategy;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.AWS
{
    public class KinesisFirehoseSink : KinesisSink<PutRecordBatchRequest, PutRecordBatchResponse, Record>, IFailoverSink<AmazonKinesisFirehoseClient>, IDisposable
    {
        protected virtual IAmazonKinesisFirehose FirehoseClient { get; set; }
        protected readonly string _deliveryStreamName;

        /// <summary>
        /// Maximum wait interval between failback retry.
        /// </summary>
        protected readonly int _maxFailbackRetryIntervalInMinutes;

        /// <summary>
        /// Primary Region Failback Timer.
        /// </summary>
        protected readonly Timer _primaryRegionFailbackTimer;

        /// <summary>
        /// Failover Sink.
        /// </summary>
        protected readonly FailoverSink<AmazonKinesisFirehoseClient> _failoverSink;

        /// <summary>
        /// Sink Regional Strategy.
        /// </summary>
        protected readonly FailoverStrategy<AmazonKinesisFirehoseClient> _failoverSinkRegionStrategy;

        private readonly bool _failoverSinkEnabled = false;

        //http://docs.aws.amazon.com/firehose/latest/dev/limits.html
        public KinesisFirehoseSink(
            IPlugInContext context
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

            string combineRecords = _config["CombineRecords"];
            if (!string.IsNullOrWhiteSpace(combineRecords) && bool.TryParse(combineRecords, out bool canCombineRecords))
            {
                CanCombineRecords = canCombineRecords;
            }

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

        //http://docs.aws.amazon.com/firehose/latest/dev/limits.html
        public KinesisFirehoseSink(IPlugInContext context,
            IAmazonKinesisFirehose firehoseClient
            ) : this(context)
        {
            // Setup Client
            FirehoseClient = firehoseClient;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KinesisFirehoseSink"/> class.
        /// </summary>
        /// <param name="context">The <see cref="IPlugInContext"/> that contains configuration info, logger, metrics etc.</param>
        /// <param name="failoverSink">The <see cref="FailoverSink{AmazonKinesisFirehoseClient}"/> that defines failover sink class.</param>
        /// <param name="failoverSinkRegionStrategy">The <see cref="FailoverStrategy{AmazonKinesisFirehoseClient}"/> that defines failover sink region selection strategy.</param>
        public KinesisFirehoseSink(
            IPlugInContext context,
            FailoverSink<AmazonKinesisFirehoseClient> failoverSink,
            FailoverStrategy<AmazonKinesisFirehoseClient> failoverSinkRegionStrategy)
            : this(context, failoverSinkRegionStrategy.GetPrimaryRegionClient()) // Setup Kinesis Firehose Client with Primary Region
        {
            // Parse or default
            // Max wait interval between failback retry
            if (!int.TryParse(_config[ConfigConstants.MAX_FAILBACK_RETRY_INTERVAL_IN_MINUTES], out _maxFailbackRetryIntervalInMinutes))
            {
                _maxFailbackRetryIntervalInMinutes = ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_RETRY_IN_MINUTES;
            }
            else if (_maxFailbackRetryIntervalInMinutes < ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_RETRY_IN_MINUTES)
            {
                throw new ArgumentException(String.Format("Invalid \"{0}\" value, please provide positive integer greator than \"{1}\".",
                    ConfigConstants.MAX_FAILBACK_RETRY_INTERVAL_IN_MINUTES, ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_RETRY_IN_MINUTES));
            }

            // Failover Sink
            _failoverSink = failoverSink;
            _failoverSinkEnabled = true;
            // Failover Sink Region Strategy
            _failoverSinkRegionStrategy = failoverSinkRegionStrategy;

            // Setup Primary Region Failback Timer
            _primaryRegionFailbackTimer = new System.Timers.Timer(_maxFailbackRetryIntervalInMinutes * 60 * 1000);
            _primaryRegionFailbackTimer.Elapsed += new ElapsedEventHandler(FailbackToPrimaryRegion);
            _primaryRegionFailbackTimer.AutoReset = true;
            _primaryRegionFailbackTimer.Start();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _primaryRegionFailbackTimer?.Stop();
        }

        public override void Start()
        {
            _metrics?.InitializeCounters(Id, MetricsConstants.CATEGORY_SINK, CounterTypeEnum.Increment,
                 new Dictionary<string, MetricValue>()
                 {
                        { MetricsConstants.KINESIS_FIREHOSE_PREFIX + MetricsConstants.BYTES_ACCEPTED, MetricValue.ZeroBytes },
                        { MetricsConstants.KINESIS_FIREHOSE_PREFIX + MetricsConstants.RECORDS_ATTEMPTED, MetricValue.ZeroCount },
                        { MetricsConstants.KINESIS_FIREHOSE_PREFIX + MetricsConstants.RECORDS_FAILED_NONRECOVERABLE, MetricValue.ZeroCount },
                        { MetricsConstants.KINESIS_FIREHOSE_PREFIX + MetricsConstants.RECORDS_FAILED_RECOVERABLE, MetricValue.ZeroCount },
                        { MetricsConstants.KINESIS_FIREHOSE_PREFIX + MetricsConstants.RECORDS_SUCCESS, MetricValue.ZeroCount },
                        { MetricsConstants.KINESIS_FIREHOSE_PREFIX + MetricsConstants.RECOVERABLE_SERVICE_ERRORS, MetricValue.ZeroCount },
                        { MetricsConstants.KINESIS_FIREHOSE_PREFIX + MetricsConstants.NONRECOVERABLE_SERVICE_ERRORS, MetricValue.ZeroCount }
                 });
            _logger?.LogInformation($"KinesisFirehoseSink id {Id} for StreamName {_deliveryStreamName} started.");
        }

        public override void Stop()
        {
            base.Stop();
            _logger?.LogInformation($"KinesisFirehoseSink id {Id} for StreamName {_deliveryStreamName} stopped.");
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

        /// <inheritdoc/>
        public AmazonKinesisFirehoseClient FailbackToPrimaryRegion(Throttle throttle)
        {
            var _firehoseClient = _failoverSink.FailbackToPrimaryRegion(_throttle);
            if (_firehoseClient is not null)
            {
                // Jittered Delay
                var delay = _throttle.GetDelayMilliseconds(new long[] {
                    1, Utility.Random.Next(1, _maxRecordsPerSecond), Utility.Random.Next(1, (int)_maxBytesPerSecond) });
                if (delay > 0)
                {
                    Task.Delay((int)(delay * (1.0d + Utility.Random.NextDouble() * ConfigConstants.DEFAULT_JITTING_FACTOR))).Wait();
                }
                // Dispose
                FirehoseClient.Dispose();
                // Override client
                FirehoseClient = _firehoseClient;
            }
            return null;
        }

        /// <inheritdoc/>
        public AmazonKinesisFirehoseClient FailOverToSecondaryRegion(Throttle throttle)
        {
            var _firehoseClient = _failoverSink.FailOverToSecondaryRegion(_throttle);
            if (_firehoseClient is not null)
            {
                // Jittered Delay
                var delay = _throttle.GetDelayMilliseconds(new long[] {
                    1, Utility.Random.Next(1, _maxRecordsPerSecond), Utility.Random.Next(1, (int)_maxBytesPerSecond) });
                if (delay > 0)
                {
                    Task.Delay((int)(delay * (1.0d + Utility.Random.NextDouble() * ConfigConstants.DEFAULT_JITTING_FACTOR))).Wait();
                }
                // Dispose
                FirehoseClient.Dispose();
                // Override client
                FirehoseClient = _firehoseClient;
            }
            return null;
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
            _logger?.LogDebug($"KinesisFirehoseSink {Id} sending {envelopes.Count} records {batchBytes} bytes.");

            DateTime utcNow = DateTime.UtcNow;
            _clientLatency = (long)envelopes.Average(r => (utcNow - r.Timestamp).TotalMilliseconds);

            long elapsedMilliseconds = Utility.GetElapsedMilliseconds();
            try
            {
                _recordsAttempted += envelopes.Count;
                _bytesAttempted += batchBytes;
                List<Record> records = envelopes.Select(r => r.Data).ToList();
                if (CanCombineRecords)
                {
                    records = CombineRecords(records);
                }

                PutRecordBatchResponse response = await SendRequestAsync(new PutRecordBatchRequest()
                {
                    DeliveryStreamName = _deliveryStreamName,
                    Records = records
                });
                _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                if (response.FailedPutCount > 0 && response.RequestResponses != null)
                {
                    _throttle.SetError();
                    _recoverableServiceErrors++;
                    _recordsSuccess += envelopes.Count - response.FailedPutCount;
                    _logger?.LogError($"KinesisFirehoseSink client {Id} BatchRecordCount={envelopes.Count} FailedPutCount={response.FailedPutCount} Attempt={_throttle.ConsecutiveErrorCount}");
                    List<Envelope<Record>> requeueRecords = new List<Envelope<Record>>();
                    for (int i = 0; i < response.RequestResponses.Count; i++)
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
                    _logger?.LogDebug($"KinesisFirehoseSink {Id} successfully sent {envelopes.Count} records {batchBytes} bytes.");

                    await SaveBookmarks(envelopes);
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
                    if (LogThrottler.ShouldWrite(LogThrottler.CreateLogTypeId(GetType().FullName, "OnNextAsync", "Requeued", Id), TimeSpan.FromMinutes(5)))
                    {
                        _logger?.LogWarning($"KinesisFirehoseSink {Id} requeued request after exception (attempt {_throttle.ConsecutiveErrorCount}): {ex.ToMinimized()}");
                    }
                }
                else
                {
                    _nonrecoverableServiceErrors++;
                    _recordsFailedNonrecoverable += envelopes.Count;
                    _logger?.LogError($"KinesisFirehoseSink {Id} client exception after {_throttle.ConsecutiveErrorCount} attempts: {ex.ToMinimized()}");
                }
            }
            PublishMetrics(MetricsConstants.KINESIS_FIREHOSE_PREFIX);
        }

        /// <inheritdoc/>
        protected override async Task<PutRecordBatchResponse> SendRequestAsync(PutRecordBatchRequest putDataRequest)
        {
            // Failover
            if (_failoverSinkEnabled)
            {
                // Failover to Secondary Region
                _ = FailOverToSecondaryRegion(_throttle);
            }

            return await FirehoseClient.PutRecordBatchAsync(putDataRequest);
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

        private void FailbackToPrimaryRegion(Object source, ElapsedEventArgs e)
        {
            // Failover
            if (_failoverSinkEnabled)
            {
                _ = FailbackToPrimaryRegion(_throttle);
            }
        }

        /// <summary>
        /// Check service health.
        /// </summary>
        /// <param name="client">Instance of <see cref="AmazonKinesisFirehoseClient"/> class.</param>
        /// <returns>Success, RountTripTime.</returns>
        public static async Task<(bool, double)> CheckServiceReachable(AmazonKinesisFirehoseClient client)
        {
            var stopwatch = new Stopwatch();
            try
            {
                stopwatch.Start();
                await client.DescribeDeliveryStreamAsync(new DescribeDeliveryStreamRequest
                {
                    DeliveryStreamName = "KinesisTap"
                });
                stopwatch.Stop();
            }
            catch (AmazonKinesisFirehoseException)
            {
                stopwatch.Stop();
                // Any exception is fine, we are currently only looking to
                // check if the service is reachable and what is the RTT.
                return (true, stopwatch.ElapsedMilliseconds);
            }
            catch (Exception)
            {
                stopwatch.Stop();
                return (false, stopwatch.ElapsedMilliseconds);
            }

            return (true, stopwatch.ElapsedMilliseconds);
        }
    }
}
