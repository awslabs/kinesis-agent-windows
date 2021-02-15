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
namespace Amazon.KinesisTap.AWS
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reactive.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Amazon.CloudWatchLogs;
    using Amazon.CloudWatchLogs.Model;
    using Amazon.KinesisTap.Core;
    using Amazon.KinesisTap.Core.Metrics;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// A sink that sends data to CloudWatch Logs.
    /// For limits relating to CloudWatch Logs, see http://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html
    /// </summary>
    public class CloudWatchLogsSink : AWSBufferedEventSink<InputLogEvent>
    {
        private const long CloudWatchOverhead = 26L;
        private const long TwoHundredFiftySixKilobytes = 256 * 1024;
        private readonly TimeSpan BatchMaximumTimeSpan = TimeSpan.FromHours(24);

        private readonly IAmazonCloudWatchLogs _client;
        private readonly string _logGroupName;
        private readonly string _logStreamName;
        protected readonly Throttle _throttle;

        private string _sequenceToken;

        public CloudWatchLogsSink(IPlugInContext context, IAmazonCloudWatchLogs client)
            : base(context, 5, 500, 1024 * 1024)
        {
            if (_count > 10000)
                throw new ArgumentException("The maximum buffer size for CloudWatchLogs is 10000");

            _client = client;
            _logGroupName = ResolveVariables(_config["LogGroup"]);
            _logStreamName = ResolveVariables(_config["LogStream"]);

            if (string.IsNullOrWhiteSpace(_logGroupName) || _logGroupName.Equals("LogGroup"))
                throw new ArgumentException("'LogGroup' setting in config file cannot be null, whitespace, or 'LogGroup'");

            if (string.IsNullOrWhiteSpace(_logStreamName) || _logStreamName.Equals("LogStream"))
                throw new ArgumentException("'LogStream' setting in config file cannot be null, whitespace or 'LogStream'");

            // Set throttle at 5 requests per second
            _throttle = new AdaptiveThrottle(new TokenBucket(1, 5), _backoffFactor, _recoveryFactor, _minRateAdjustmentFactor);
        }

        public override void Start()
        {
            try
            {
                this.StartAsync().Wait();
            }
            catch (AggregateException aex) when (aex.InnerException is AmazonCloudWatchLogsException acle && acle.Message?.Contains("Rate exceeded") == true)
            {
                throw new RateExceededException("CloudWatchLogs quota is exceeded", acle);
            }
        }

        public async Task StartAsync()
        {
            var logStreamName = this.ResolveTimestampInLogStreamName(DateTime.UtcNow);

            // Preload the sequence token. Create the log group if it doesn't exist, but don't create the stream.
            // Creating the stream here will result in a duplicate call to "GetSequenceTokenAsync" in OnNextAsync, since the sequence
            // token value of a new stream is null (i.e. we can't differentiate between a non-existent stream and an empty one)
            await this.GetSequenceTokenAsync(logStreamName, false);

            base.Start();
            _metrics?.InitializeCounters(this.Id, MetricsConstants.CATEGORY_SINK, CounterTypeEnum.Increment,
                new Dictionary<string, MetricValue>()
                {
                    { MetricsConstants.CLOUDWATCHLOG_PREFIX + MetricsConstants.BYTES_ATTEMPTED, new MetricValue(0, MetricUnit.Bytes) },
                    { MetricsConstants.CLOUDWATCHLOG_PREFIX + MetricsConstants.RECORDS_ATTEMPTED, MetricValue.ZeroCount },
                    { MetricsConstants.CLOUDWATCHLOG_PREFIX + MetricsConstants.RECORDS_FAILED_NONRECOVERABLE, MetricValue.ZeroCount },
                    { MetricsConstants.CLOUDWATCHLOG_PREFIX + MetricsConstants.RECORDS_FAILED_RECOVERABLE, MetricValue.ZeroCount },
                    { MetricsConstants.CLOUDWATCHLOG_PREFIX + MetricsConstants.RECORDS_SUCCESS, MetricValue.ZeroCount },
                    { MetricsConstants.CLOUDWATCHLOG_PREFIX + MetricsConstants.RECOVERABLE_SERVICE_ERRORS, MetricValue.ZeroCount },
                    { MetricsConstants.CLOUDWATCHLOG_PREFIX + MetricsConstants.NONRECOVERABLE_SERVICE_ERRORS, MetricValue.ZeroCount }
                });
            _logger?.LogInformation("CloudWatchLogsSink id {0} for log group {1} log stream {2} started.", this.Id, _logGroupName, _logStreamName);
        }

        public override void Stop()
        {
            base.Stop();
            _logger?.LogInformation("CloudWatchLogsSink id {0} for log group {1} log stream {2} stopped.", this.Id, _logGroupName, _logStreamName);
        }

        private async Task SendBatchAsync(List<Envelope<InputLogEvent>> records)
        {
            var batchBytes = records.Sum(r => GetRecordSize(r));

            try
            {
                _logger?.LogDebug("CloudWatchLogsSink client {0} sending {1} records {2} bytes.", this.Id, records.Count, batchBytes);

                var logStreamName = this.ResolveTimestampInLogStreamName(records[0].Timestamp);

                // If the sequence token is null, try to get it.
                // If the log stream doesn't exist, create it (by specifying "true" in the second parameter).
                // This should be the only place where a log stream is created.
                // This method will ensure that both the log group and stream exists,
                // so there is no need to handle a ResourceNotFound exception later.
                if (string.IsNullOrEmpty(_sequenceToken))
                    await this.GetSequenceTokenAsync(logStreamName, true);

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
                        var response = await _client.PutLogEventsAsync(request);
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
                                    .AppendFormat("CloudWatchLogsSink client {0} encountered some rejected logs.", this.Id)
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

                        _logger?.LogDebug("CloudWatchLogsSink client {0} successfully sent {1} records {2} bytes.", this.Id, records.Count, batchBytes);
                        _recordsSuccess += records.Count;
                        this.SaveBookmarks(records);

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
                                await this.GetSequenceTokenAsync(logStreamName, false);
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
                        if (this.IsRecoverableException(ex))
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
                                _logger?.LogWarning("CloudWatchLogsSink client {0} attempt={1} exception={2}. Will retry.", this.Id, _throttle.ConsecutiveErrorCount, ex.Message);
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
                _logger?.LogError("CloudWatchLogsSink client {0} exception (attempt={1}): {2}", this.Id, _throttle.ConsecutiveErrorCount, ex.ToMinimized());
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
                while (idx < records.Count && records[idx].Timestamp - earliestTime < BatchMaximumTimeSpan)
                {
                    batch.Add(records[idx]);
                    idx++;
                }

                await SendBatchAsync(batch);
            }
        }

        protected override bool IsRecoverableException(Exception ex)
        {
            return ex is InvalidSequenceTokenException || ex is ServiceUnavailableException || ex.Message.Contains(AWSConstants.SecurityTokenExpiredError) || base.IsRecoverableException(ex);
        }

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

        private async Task GetSequenceTokenAsync(string logStreamName, bool createStreamIfNotExists)
        {
            var request = new DescribeLogStreamsRequest
            {
                LogGroupName = _logGroupName,
                LogStreamNamePrefix = logStreamName
            };

            DescribeLogStreamsResponse describeLogsStreamsResponse = null;
            try
            {
                describeLogsStreamsResponse = await _client.DescribeLogStreamsAsync(request);
            }
            catch (ResourceNotFoundException rex)
            {
                // Create the log group if it doesn't exist.
                if (rex.Message.IndexOf("log group does not exist") > -1)
                {
                    _logger?.LogInformation("Log group {0} does not exist. Creating it.", _logGroupName);
                    await this.CreateLogGroupAsync();

                    if (createStreamIfNotExists) await this.CreateLogStreamAsync(logStreamName);
                    return;
                }
            }

            var logStream = describeLogsStreamsResponse.LogStreams
                .FirstOrDefault(s => s.LogStreamName.Equals(logStreamName, StringComparison.CurrentCultureIgnoreCase));

            if (logStream == null)
            {
                if (createStreamIfNotExists)
                    await this.CreateLogStreamAsync(logStreamName);
            }
            else
            {
                _sequenceToken = logStream.UploadSequenceToken;
            }
        }

        private string ResolveTimestampInLogStreamName(DateTime timestamp)
        {
            return Utility.ResolveVariables(_logStreamName,
                v => Utility.ResolveTimestampVariable(v, timestamp));
        }

        private async Task CreateLogGroupAsync()
        {
            _logger?.LogInformation("CloudWatchLogsSink creating log group {0}", _logGroupName);
            try
            {
                await _client.CreateLogGroupAsync(new CreateLogGroupRequest(_logGroupName));
            }
            catch (Exception ex)
            {
                _logger?.LogError("CloudWatchLogsSink create log group {0} exception: {1}", _logGroupName, ex.ToMinimized());
                throw;
            }
        }

        private async Task CreateLogStreamAsync(string logStreamName)
        {
            _logger?.LogInformation("CloudWatchLogsSink creating log stream {0} in log group {1}", logStreamName, _logGroupName);
            try
            {
                var response = await _client.CreateLogStreamAsync(new CreateLogStreamRequest
                {
                    LogGroupName = _logGroupName,
                    LogStreamName = logStreamName
                });
            }
            catch (Exception ex)
            {
                _logger?.LogError("CloudWatchLogsSink create log stream {0} exception: {1}", logStreamName, ex.ToMinimized());
                throw;
            }
        }
    }
}
