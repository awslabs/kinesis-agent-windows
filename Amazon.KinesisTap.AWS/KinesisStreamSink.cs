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
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Timers;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.KinesisTap.AWS.Failover;
using Amazon.KinesisTap.AWS.Failover.Strategy;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.AWS
{
    public class KinesisStreamSink : KinesisSink<PutRecordsRequest, PutRecordsResponse, PutRecordsRequestEntry>, IFailoverSink<AmazonKinesisClient>, IDisposable
    {
        protected virtual IAmazonKinesis KinesisClient { get; set; }
        protected readonly string _streamName;

        /// <summary>
        /// Maximum wait interval between failback retry.
        /// </summary>
        protected readonly int _maxFailbackRetryIntervalInMinutes;

        /// <summary>
        /// Primary Region Failback Timer.
        /// </summary>
        protected readonly System.Timers.Timer _primaryRegionFailbackTimer;

        /// <summary>
        /// Failover Sink.
        /// </summary>
        protected readonly FailoverSink<AmazonKinesisClient> _failoverSink;

        /// <summary>
        /// Sink Regional Strategy.
        /// </summary>
        protected readonly FailoverStrategy<AmazonKinesisClient> _failoverSinkRegionStrategy;

        private readonly bool _failoverSinkEnabled = false;

        private const int _parallelism = 3;
        private readonly CancellationTokenSource _cts;
        private readonly Channel<(List<Envelope<PutRecordsRequestEntry>>, long)>[] _channels;
        private readonly Task[] _streamTasks;

        public KinesisStreamSink(
            IPlugInContext context
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

            _cts = new CancellationTokenSource();
            _channels = new Channel<(List<Envelope<PutRecordsRequestEntry>>, long)>[_parallelism];
            _streamTasks = new Task[_parallelism];
        }

        public KinesisStreamSink(
            IPlugInContext context,
            IAmazonKinesis kinesisClient
            ) : this(context)
        {
            // Setup Client
            KinesisClient = kinesisClient;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KinesisStreamSink"/> class.
        /// </summary>
        /// <param name="context">The <see cref="IPlugInContext"/> that contains configuration info, logger, metrics etc.</param>
        /// <param name="failoverSink">The <see cref="FailoverSink{AmazonKinesisClient}"/> that defines failover sink class.</param>
        /// <param name="failoverSinkRegionStrategy">The <see cref="FailoverStrategy{AmazonKinesisClient}"/> that defines failover sink region selection strategy.</param>
        public KinesisStreamSink(
            IPlugInContext context,
            FailoverSink<AmazonKinesisClient> failoverSink,
            FailoverStrategy<AmazonKinesisClient> failoverSinkRegionStrategy)
            : this(context, failoverSinkRegionStrategy.GetPrimaryRegionClient()) // Setup Kinesis Stream Client with Primary Region
        {
            // Parse or default
            // Max wail interval between failback retry
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
            for (var i = 0; i < _parallelism; i++)
            {
                _channels[i] = Channel.CreateBounded<(List<Envelope<PutRecordsRequestEntry>>, long)>(5);
                _streamTasks[i] = StreamTask(i);
            }

            _metrics?.InitializeCounters(Id, MetricsConstants.CATEGORY_SINK, CounterTypeEnum.Increment,
                new Dictionary<string, MetricValue>()
                {
                    { MetricsConstants.KINESIS_STREAM_PREFIX + MetricsConstants.BYTES_ACCEPTED, MetricValue.ZeroBytes },
                    { MetricsConstants.KINESIS_STREAM_PREFIX + MetricsConstants.RECORDS_ATTEMPTED, MetricValue.ZeroCount },
                    { MetricsConstants.KINESIS_STREAM_PREFIX + MetricsConstants.RECORDS_FAILED_NONRECOVERABLE, MetricValue.ZeroCount },
                    { MetricsConstants.KINESIS_STREAM_PREFIX + MetricsConstants.RECORDS_FAILED_RECOVERABLE, MetricValue.ZeroCount },
                    { MetricsConstants.KINESIS_STREAM_PREFIX + MetricsConstants.RECORDS_SUCCESS, MetricValue.ZeroCount },
                    { MetricsConstants.KINESIS_STREAM_PREFIX + MetricsConstants.RECOVERABLE_SERVICE_ERRORS, MetricValue.ZeroCount },
                    { MetricsConstants.KINESIS_STREAM_PREFIX + MetricsConstants.NONRECOVERABLE_SERVICE_ERRORS, MetricValue.ZeroCount }
                });
            _logger?.LogInformation($"KinesisStreamSink id {Id} for StreamName {_streamName} started.");
        }

        public override void Stop()
        {
            base.Stop();
            _cts.Cancel();
            foreach (var t in _streamTasks)
            {
                t.GetAwaiter().GetResult();
            }
            _logger?.LogInformation($"KinesisStreamSink id {Id} for StreamName {_streamName} stopped.");
        }

        /// <inheritdoc/>
        public AmazonKinesisClient FailbackToPrimaryRegion(Throttle throttle)
        {
            var _kinesisClient = _failoverSink.FailbackToPrimaryRegion(_throttle);
            if (_kinesisClient is not null)
            {
                // Jittered Delay
                var delay = _throttle.GetDelayMilliseconds(new long[] {
                    1, Utility.Random.Next(1, _maxRecordsPerSecond), Utility.Random.Next(1, (int)_maxBytesPerSecond) });
                if (delay > 0)
                {
                    Task.Delay((int)(delay * (1.0d + Utility.Random.NextDouble() * ConfigConstants.DEFAULT_JITTING_FACTOR))).Wait();
                }
                // Dispose
                KinesisClient.Dispose();
                // Override client
                KinesisClient = _kinesisClient;
            }
            return null;
        }

        /// <inheritdoc/>
        public AmazonKinesisClient FailOverToSecondaryRegion(Throttle throttle)
        {
            var _kinesisClient = _failoverSink.FailOverToSecondaryRegion(_throttle);
            if (_kinesisClient is not null)
            {
                // Jittered Delay
                var delay = _throttle.GetDelayMilliseconds(new long[] {
                    1, Utility.Random.Next(1, _maxRecordsPerSecond), Utility.Random.Next(1, (int)_maxBytesPerSecond) });
                if (delay > 0)
                {
                    Task.Delay((int)(delay * (1.0d + Utility.Random.NextDouble() * ConfigConstants.DEFAULT_JITTING_FACTOR))).Wait();
                }
                // Dispose
                KinesisClient.Dispose();
                // Override client
                KinesisClient = _kinesisClient;
            }
            return null;
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
            long recordSize = record.Data.Data.Length + Encoding.UTF8.GetByteCount(record.Data.PartitionKey);
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
            try
            {
                foreach (var channel in _channels)
                {
                    if (channel.Writer.TryWrite((records, batchBytes)))
                    {
                        return;
                    }
                }

                await _channels[0].Writer.WaitToWriteAsync(_cts.Token);
            }
            catch (OperationCanceledException)
            {
            }
        }

        protected override async Task<PutRecordsResponse> SendRequestAsync(PutRecordsRequest putDataRequest)
        {
            // Failover
            if (_failoverSinkEnabled)
            {
                // Failover to Secondary Region
                _ = FailOverToSecondaryRegion(_throttle);
            }

            // Upload records to backend.
            return await KinesisClient.PutRecordsAsync(putDataRequest);
        }

        protected override ISerializer<List<Envelope<PutRecordsRequestEntry>>> GetSerializer()
        {
            return AWSSerializationUtility.PutRecordsRequestEntryListBinarySerializer;
        }

        protected override bool IsRecoverableException(Exception ex)
        {
            return base.IsRecoverableException(ex) || ex is ProvisionedThroughputExceededException;
        }

        private async Task StreamTask(int index)
        {
            var channel = _channels[index];
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    await channel.Reader.WaitToReadAsync(_cts.Token);
                    while (channel.Reader.TryRead(out var item))
                    {
                        _logger?.LogDebug("Channel {0} sending", index);
                        await SendAsync(item.Item1, item.Item2);
                        _cts.Token.ThrowIfCancellationRequested();
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Encountered error while processing EventRecord");
                }
            }
        }

        private async Task SendAsync(List<Envelope<PutRecordsRequestEntry>> records, long batchBytes)
        {
            _logger?.LogDebug($"KinesisStreamSink {Id} sending {records.Count} records {batchBytes} bytes.");
            DateTime utcNow = DateTime.UtcNow;
            _clientLatency = (long)records.Average(r => (utcNow - r.Timestamp).TotalMilliseconds);

            var elapsedMilliseconds = Utility.GetElapsedMilliseconds();
            try
            {
                _recordsAttempted += records.Count;
                _bytesAttempted += batchBytes;
                var response = await SendRequestAsync(new PutRecordsRequest()
                {
                    StreamName = _streamName,
                    Records = records.Select(r => r.Data).ToList()
                });
                _throttle.SetSuccess();
                _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                _recordsSuccess += records.Count;
                _logger?.LogDebug($"KinesisStreamSink {Id} successfully sent {records.Count} records {batchBytes} bytes.");

                await SaveBookmarks(records);
            }
            catch (Exception ex)
            {
                _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                _throttle.SetError();
                if (IsRecoverableException(ex) && _buffer.Requeue(records, _throttle.ConsecutiveErrorCount < _maxAttempts))
                {
                    _recoverableServiceErrors++;
                    _recordsFailedRecoverable += records.Count;
                    if (LogThrottler.ShouldWrite(LogThrottler.CreateLogTypeId(GetType().FullName, "OnNextAsync", "Requeued", Id), TimeSpan.FromMinutes(5)))
                    {
                        _logger?.LogWarning(ex, $"KinesisStreamSink {Id} requeued request after exception (attempt {_throttle.ConsecutiveErrorCount})");
                    }
                }
                else
                {
                    _nonrecoverableServiceErrors++;
                    _recordsFailedNonrecoverable += records.Count;
                    _logger?.LogError(ex, $"KinesisStreamSink {Id} client exception after {_throttle.ConsecutiveErrorCount} attempts");
                }
            }
            PublishMetrics(MetricsConstants.KINESIS_STREAM_PREFIX);
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
        /// <param name="client">Instance of <see cref="AmazonKinesisClient"/> class.</param>
        /// <returns>Success, RountTripTime.</returns>
        public static async Task<(bool, double)> CheckServiceReachable(AmazonKinesisClient client)
        {
            var stopwatch = new Stopwatch();
            try
            {
                stopwatch.Start();
                await client.DescribeStreamAsync(new DescribeStreamRequest
                {
                    StreamName = "KinesisTap"
                });
                stopwatch.Stop();
            }
            catch (AmazonKinesisException)
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
