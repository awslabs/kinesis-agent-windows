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
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using Amazon.CloudWatchLogs;
using Amazon.CloudWatchLogs.Model;
using Amazon.KinesisTap.AWS.Failover;
using Amazon.KinesisTap.AWS.Failover.Strategy;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.AWS
{
    /// <summary>
    /// A sink that sends data to CloudWatch Logs.
    /// For limits relating to CloudWatch Logs, see http://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html
    /// </summary>
    public class CloudWatchLogsSink : AWSBufferedEventSink<InputLogEvent>, IFailoverSink<AmazonCloudWatchLogsClient>, IDisposable
    {
        protected virtual IAmazonCloudWatchLogs CloudWatchLogsClient { get; set; }
        protected readonly string _logGroupName;
        protected readonly string _logStreamName;
        protected readonly Throttle _throttle;

        protected string _sequenceToken;

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
        protected readonly FailoverSink<AmazonCloudWatchLogsClient> _failoverSink;

        /// <summary>
        /// Sink Regional Strategy.
        /// </summary>
        protected readonly FailoverStrategy<AmazonCloudWatchLogsClient> _failoverSinkRegionStrategy;

        private readonly bool _failoverSinkEnabled = false;

        private const long CloudWatchOverhead = 26L;
        private const long TwoHundredFiftySixKilobytes = 256 * 1024;

        private readonly TimeSpan _batchMaximumTimeSpan = TimeSpan.FromHours(24);

        public CloudWatchLogsSink(
            IPlugInContext context
            ) : base(context, 5, 500, 1024 * 1024)
        {
            if (_count > 10000)
                throw new ArgumentException("The maximum buffer size for CloudWatchLogs is 10000");

            _logGroupName = ResolveVariables(_config["LogGroup"]);
            _logStreamName = ResolveVariables(_config["LogStream"]);

            if (string.IsNullOrWhiteSpace(_logGroupName) || _logGroupName.Equals("LogGroup"))
                throw new ArgumentException("'LogGroup' setting in config file cannot be null, whitespace, or 'LogGroup'");

            if (string.IsNullOrWhiteSpace(_logStreamName) || _logStreamName.Equals("LogStream"))
                throw new ArgumentException("'LogStream' setting in config file cannot be null, whitespace or 'LogStream'");

            // Set throttle at 5 requests per second
            _throttle = new AdaptiveThrottle(new TokenBucket(1, 5), _backoffFactor, _recoveryFactor, _minRateAdjustmentFactor);
        }

        public CloudWatchLogsSink(
            IPlugInContext context,
            IAmazonCloudWatchLogs cloudWatchLogsClient
            ) : this(context)
        {
            // Setup Client
            CloudWatchLogsClient = cloudWatchLogsClient;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudWatchLogsSink"/> class.
        /// </summary>
        /// <param name="context">The <see cref="IPlugInContext"/> that contains configuration info, logger, metrics etc.</param>
        /// <param name="failoverSink">The <see cref="FailoverSink{AmazonCloudWatchLogsClient}"/> that defines failover sink class.</param>
        /// <param name="failoverSinkRegionStrategy">The <see cref="FailoverStrategy{AmazonCloudWatchLogsClient}"/> that defines failover sink region selection strategy.</param>
        public CloudWatchLogsSink(
            IPlugInContext context,
            FailoverSink<AmazonCloudWatchLogsClient> failoverSink,
            FailoverStrategy<AmazonCloudWatchLogsClient> failoverSinkRegionStrategy)
            : this(context, failoverSinkRegionStrategy.GetPrimaryRegionClient()) // Setup CloudWatch Logs Client with Primary Region
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
            _primaryRegionFailbackTimer = new Timer(_maxFailbackRetryIntervalInMinutes * 60 * 1000);
            _primaryRegionFailbackTimer.Elapsed += new ElapsedEventHandler(FailbackToPrimaryRegion);
            _primaryRegionFailbackTimer.AutoReset = true;
            _primaryRegionFailbackTimer.Start();
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            _primaryRegionFailbackTimer.Stop();
        }

        public override void Start()
        {
            _metrics?.InitializeCounters(Id, MetricsConstants.CATEGORY_SINK, CounterTypeEnum.Increment,
            new Dictionary<string, MetricValue>()
            {
                { MetricsConstants.CLOUDWATCHLOG_PREFIX + MetricsConstants.BYTES_ACCEPTED, new MetricValue(0, MetricUnit.Bytes) },
                { MetricsConstants.CLOUDWATCHLOG_PREFIX + MetricsConstants.RECORDS_ATTEMPTED, MetricValue.ZeroCount },
                { MetricsConstants.CLOUDWATCHLOG_PREFIX + MetricsConstants.RECORDS_FAILED_NONRECOVERABLE, MetricValue.ZeroCount },
                { MetricsConstants.CLOUDWATCHLOG_PREFIX + MetricsConstants.RECORDS_FAILED_RECOVERABLE, MetricValue.ZeroCount },
                { MetricsConstants.CLOUDWATCHLOG_PREFIX + MetricsConstants.RECORDS_SUCCESS, MetricValue.ZeroCount },
                { MetricsConstants.CLOUDWATCHLOG_PREFIX + MetricsConstants.RECOVERABLE_SERVICE_ERRORS, MetricValue.ZeroCount },
                { MetricsConstants.CLOUDWATCHLOG_PREFIX + MetricsConstants.NONRECOVERABLE_SERVICE_ERRORS, MetricValue.ZeroCount }
            });
            _logger?.LogInformation("CloudWatchLogsSink id {0} for log group {1} log stream {2} started.", Id, _logGroupName, _logStreamName);
        }

        public override void Stop()
        {
            base.Stop();
            _logger?.LogInformation("CloudWatchLogsSink id {0} for log group {1} log stream {2} stopped.", Id, _logGroupName, _logStreamName);
        }

        /// <inheritdoc/>
        public AmazonCloudWatchLogsClient FailbackToPrimaryRegion(Throttle throttle)
        {
            var _cloudWatchLogsClient = _failoverSink.FailbackToPrimaryRegion(_throttle);
            if (_cloudWatchLogsClient is not null)
            {
                // Jittered Delay
                var delay = _throttle.GetDelayMilliseconds(1);
                if (delay > 0)
                {
                    Task.Delay((int)(delay * (1.0d + Utility.Random.NextDouble() * ConfigConstants.DEFAULT_JITTING_FACTOR))).Wait();
                }
                // Dispose
                CloudWatchLogsClient.Dispose();
                // Override client
                CloudWatchLogsClient = _cloudWatchLogsClient;

                // Reset CloudWatch Logs sequence token
                _sequenceToken = null;
                GetSequenceTokenAsync(ResolveTimestampInLogStreamName(DateTime.UtcNow), true).Wait();
            }
            return null;
        }

        /// <inheritdoc/>
        public AmazonCloudWatchLogsClient FailOverToSecondaryRegion(Throttle throttle)
        {
            var _cloudWatchLogsClient = _failoverSink.FailOverToSecondaryRegion(_throttle);
            if (_cloudWatchLogsClient is not null)
            {
                // Jittered Delay
                var delay = _throttle.GetDelayMilliseconds(1);
                if (delay > 0)
                {
                    Task.Delay((int)(delay * (1.0d + Utility.Random.NextDouble() * ConfigConstants.DEFAULT_JITTING_FACTOR))).Wait();
                }
                // Dispose
                CloudWatchLogsClient.Dispose();
                // Override client
                CloudWatchLogsClient = _cloudWatchLogsClient;

                // Reset CloudWatch Logs sequence token
                _sequenceToken = null;
                GetSequenceTokenAsync(ResolveTimestampInLogStreamName(DateTime.UtcNow), true).Wait();
            }
            return null;
        }

        private async Task SendBatchAsync(List<Envelope<InputLogEvent>> records)
        {
            var batchBytes = records.Sum(r => GetRecordSize(r));

            try
            {
                _logger?.LogDebug("CloudWatchLogsSink client {0} sending {1} records {2} bytes.", Id, records.Count, batchBytes);

                var logStreamName = ResolveTimestampInLogStreamName(records[0].Timestamp);

                var request = new PutLogEventsRequest
                {
                    LogGroupName = _logGroupName,
                    LogStreamName = logStreamName,
                    SequenceToken = _sequenceToken,
                    LogEvents = records
                        .Select(e => e.Data)
                        .ToList()
                };

                int invalidSequenceTokenCount = 0;
                while (true)
                {
                    var utcNow = DateTime.UtcNow;
                    _clientLatency = (long)records.Average(r => (utcNow - r.Timestamp).TotalMilliseconds);

                    long elapsedMilliseconds = Utility.GetElapsedMilliseconds();
                    try
                    {
                        // If the sequence token is null, try to get it.
                        // If the log stream doesn't exist, create it (by specifying "true" in the second parameter).
                        // This should be the only place where a log stream is created.
                        // This method will ensure that both the log group and stream exists,
                        // so there is no need to handle a ResourceNotFound exception later.
                        if (string.IsNullOrEmpty(_sequenceToken))
                        {
                            await GetSequenceTokenAsync(logStreamName, true);
                        }

                        var response = await SendRequestAsync(request);
                        _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                        _throttle.SetSuccess();
                        _sequenceToken = response.NextSequenceToken;
                        _recordsAttempted += records.Count;
                        _bytesAttempted += batchBytes;

                        var rejectedLogEventsInfo = response.RejectedLogEventsInfo;
                        if (rejectedLogEventsInfo != null)
                        {
                            // Don't do the expensive string building unless we know the logger isn't null.
                            if (_logger != null)
                            {
                                var sb = new StringBuilder()
                                    .AppendFormat("CloudWatchLogsSink client {0} encountered some rejected logs.", Id)
                                    .AppendFormat(" ExpiredLogEventEndIndex {0}", rejectedLogEventsInfo.ExpiredLogEventEndIndex)
                                    .AppendFormat(" TooNewLogEventStartIndex {0}", rejectedLogEventsInfo.TooNewLogEventStartIndex)
                                    .AppendFormat(" TooOldLogEventEndIndex {0}", rejectedLogEventsInfo.TooOldLogEventEndIndex);
                                _logger.LogError(sb.ToString());
                            }

                            var recordCount = records.Count - rejectedLogEventsInfo.ExpiredLogEventEndIndex - rejectedLogEventsInfo.TooOldLogEventEndIndex;
                            if (rejectedLogEventsInfo.TooOldLogEventEndIndex > 0)
                                recordCount -= records.Count - rejectedLogEventsInfo.TooNewLogEventStartIndex;

                            _recordsFailedNonrecoverable += (records.Count - recordCount);
                        }

                        _logger?.LogDebug("CloudWatchLogsSink client {0} successfully sent {1} records {2} bytes.", Id, records.Count, batchBytes);
                        _recordsSuccess += records.Count;
                        await SaveBookmarks(records);

                        break;
                    }
                    catch (AmazonCloudWatchLogsException ex)
                    {
                        _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                        _throttle.SetError();

                        // InvalidSequenceTokenExceptions are thrown when a PutLogEvents call doesn't have a valid sequence token.
                        // This is usually recoverable, so we'll try twice before requeuing events.
                        if (ex is InvalidSequenceTokenException invalidSequenceTokenException && invalidSequenceTokenCount < 2)
                        {
                            // Increment the invalid sequence token counter, to limit the "instant retries" that we attempt.
                            invalidSequenceTokenCount++;

                            // The exception from CloudWatch contains the sequence token, so we'll try to parse it out.
                            _sequenceToken = invalidSequenceTokenException.GetExpectedSequenceToken();

                            // Sometimes we get a sequence token with a string value of "null".
                            // This is invalid so we'll fetch it again and retry immediately.
                            // If credentials have expired or this request is being throttled,
                            // the wrapper try/catch will capture it and data will
                            if (AWSConstants.NullString.Equals(_sequenceToken))
                            {
                                _sequenceToken = null;
                                await GetSequenceTokenAsync(logStreamName, false);
                            }

                            // Reset the sequence token in the request and immediately retry (without requeuing),
                            // so that the sequence token does not become invalid again.
                            request.SequenceToken = _sequenceToken;
                            continue;
                        }

                        // Retry if one of the following was true:
                        // - The exception was thrown because an invalid sequence token was used (more than twice in a row)
                        // - The service was unavailable (transient error or service outage)
                        // - The security token in the credentials has expired (previously this was handled as an unrecoverable error)
                        if (IsRecoverableException(ex))
                        {
                            // Try to requeue the records into the buffer.
                            // This will mean that the events in the buffer are now out of order :(
                            // There's nothing we can do about that short of rewriting all the buffering logic.
                            // Having out of order events isn't that bad, because the service that we're uploading
                            // to will be storing them based on their event time anyway. However, this can affect
                            // the persistent bookmarking behavior, since bookmarks are updated based on the
                            // position/eventId in the last batch sent, not what's currently in the buffer.
                            if (_buffer.Requeue(records, _throttle.ConsecutiveErrorCount < _maxAttempts))
                            {
                                _logger?.LogWarning("CloudWatchLogsSink client {0} attempt={1} exception={2}. Will retry.", Id, _throttle.ConsecutiveErrorCount, ex.Message);
                                _recoverableServiceErrors++;
                                _recordsFailedRecoverable += records.Count;
                                break;
                            }
                        }

                        _recordsFailedNonrecoverable += records.Count;
                        _nonrecoverableServiceErrors++;
                        throw;
                    }
                    catch (Exception)
                    {
                        _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                        _throttle.SetError();
                        _recordsFailedNonrecoverable += records.Count;
                        _nonrecoverableServiceErrors++;
                        throw;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError("CloudWatchLogsSink client {0} exception (attempt={1}): {2}", Id, _throttle.ConsecutiveErrorCount, ex.ToMinimized());
            }

            PublishMetrics(MetricsConstants.CLOUDWATCHLOG_PREFIX);
        }

        protected override async Task OnNextAsync(List<Envelope<InputLogEvent>> records, long batchBytes)
        {
            if (records == null || records.Count == 0)
            {
                return;
            }

            records.Sort((r1, r2) => r1.Timestamp.CompareTo(r2.Timestamp));
            var batch = new List<Envelope<InputLogEvent>>();
            var idx = 0;
            while (idx < records.Count)
            {
                var earliestTime = records[idx].Timestamp;
                batch.Clear();
                while (idx < records.Count && records[idx].Timestamp - earliestTime < _batchMaximumTimeSpan)
                {
                    batch.Add(records[idx]);
                    idx++;
                }

                await SendBatchAsync(batch);
            }
        }

        protected override bool IsRecoverableException(Exception ex) => base.IsRecoverableException(ex) ||
            ex is InvalidSequenceTokenException ||
            ex is ServiceUnavailableException ||
            ex.Message.Contains(AWSConstants.SecurityTokenExpiredError) ||
            ex.Message.Contains("Rate exceeded", StringComparison.OrdinalIgnoreCase);

        protected override long GetRecordSize(Envelope<InputLogEvent> record)
        {
            long recordSize = Encoding.UTF8.GetByteCount(record.Data.Message) + CloudWatchOverhead;
            if (recordSize > TwoHundredFiftySixKilobytes)
            {
                _recordsFailedNonrecoverable++;
                throw new ArgumentException("The maximum record size is 256KB. Record discarded.");
            }
            return recordSize;
        }

        protected override InputLogEvent CreateRecord(string record, IEnvelope envelope)
        {
            return new InputLogEvent
            {
                Timestamp = envelope.Timestamp,
                Message = record
            };
        }

        protected override long GetDelayMilliseconds(int recordCount, long batchBytes)
        {
            return _throttle.GetDelayMilliseconds(1); //One call
        }

        protected override ISerializer<List<Envelope<InputLogEvent>>> GetSerializer()
        {
            return AWSSerializationUtility.InputLogEventListBinarySerializer;
        }

        protected virtual async Task GetSequenceTokenAsync(string logStreamName, bool createStreamIfNotExists)
        {
            // Failover
            if (_failoverSinkEnabled)
            {
                // Failover to Secondary Region
                _ = FailOverToSecondaryRegion(_throttle);
            }

            try
            {
                var request = new DescribeLogStreamsRequest
                {
                    LogGroupName = _logGroupName,
                    LogStreamNamePrefix = logStreamName
                };

                DescribeLogStreamsResponse describeLogsStreamsResponse = null;
                try
                {
                    describeLogsStreamsResponse = await CloudWatchLogsClient.DescribeLogStreamsAsync(request);
                }
                catch (ResourceNotFoundException rex)
                {
                    // Create the log group if it doesn't exist.
                    if (rex.Message.IndexOf("log group does not exist") > -1)
                    {
                        _logger?.LogInformation("Log group {0} does not exist. Creating it.", _logGroupName);
                        await CreateLogGroupAsync();

                        if (createStreamIfNotExists) await CreateLogStreamAsync(logStreamName);
                        return;
                    }
                }

                var logStream = describeLogsStreamsResponse.LogStreams
                    .FirstOrDefault(s => s.LogStreamName.Equals(logStreamName, StringComparison.CurrentCultureIgnoreCase));

                if (logStream == null)
                {
                    if (createStreamIfNotExists)
                        await CreateLogStreamAsync(logStreamName);
                }
                else
                {
                    _sequenceToken = logStream.UploadSequenceToken;
                }
            }
            catch (Exception)
            {
                // Set error count
                _throttle.SetError();
                throw;
            }
        }

        protected string ResolveTimestampInLogStreamName(DateTime timestamp)
        {
            return Utility.ResolveVariables(_logStreamName,
                v => Utility.ResolveTimestampVariable(v, timestamp));
        }

        protected virtual async Task CreateLogGroupAsync()
        {
            _logger?.LogInformation("CloudWatchLogsSink creating log group {0}", _logGroupName);
            try
            {
                await CloudWatchLogsClient.CreateLogGroupAsync(new CreateLogGroupRequest(_logGroupName));
                _throttle.SetSuccess();
            }
            catch (Exception ex)
            {
                _logger?.LogError("CloudWatchLogsSink create log group {0} exception: {1}", _logGroupName, ex.ToMinimized());
                // Set error count
                _throttle.SetError();
                throw;
            }
        }

        protected virtual async Task CreateLogStreamAsync(string logStreamName)
        {
            _logger?.LogInformation("CloudWatchLogsSink creating log stream {0} in log group {1}", logStreamName, _logGroupName);
            try
            {
                var response = await CloudWatchLogsClient.CreateLogStreamAsync(new CreateLogStreamRequest
                {
                    LogGroupName = _logGroupName,
                    LogStreamName = logStreamName
                });
                _throttle.SetSuccess();
            }
            catch (Exception ex)
            {
                _logger?.LogError("CloudWatchLogsSink create log stream {0} exception: {1}", logStreamName, ex.ToMinimized());
                // Set error count
                _throttle.SetError();
                throw;
            }
        }

        /// <inheritdoc/>
        protected async Task<PutLogEventsResponse> SendRequestAsync(PutLogEventsRequest putDataRequest)
        {
            // Failover
            if (_failoverSinkEnabled)
            {
                // Failover to Secondary Region
                _ = FailOverToSecondaryRegion(_throttle);
            }

            // Update CloudWatch Logs sequence token
            putDataRequest.SequenceToken = _sequenceToken;
            return await CloudWatchLogsClient.PutLogEventsAsync(putDataRequest);
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
        /// <param name="client">Instance of <see cref="AmazonCloudWatchLogsClient"/> class.</param>
        /// <returns>Success, RountTripTime.</returns>
        public static async Task<(bool, double)> CheckServiceReachable(AmazonCloudWatchLogsClient client)
        {
            var stopwatch = new Stopwatch();
            try
            {
                stopwatch.Start();
                await client.DescribeLogGroupsAsync(new DescribeLogGroupsRequest
                {
                    LogGroupNamePrefix = "KinesisTap"
                });
                stopwatch.Stop();
            }
            catch (AmazonCloudWatchLogsException)
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
