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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.CloudWatchLogs;
using Amazon.CloudWatchLogs.Model;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.AWS
{
    /// <summary>
    /// Sink that sends data to CloudWatch Logs.
    /// </summary>
    internal class AsyncCloudWatchLogsSink : AWSBufferedSink<InputLogEvent>, IDisposable
    {
        private const long CloudWatchOverhead = 26L;
        private const long TwoHundredFiftySixKilobytes = 256 * 1024;
        private static readonly TimeSpan _batchMaximumTimeSpan = TimeSpan.FromHours(24);

        protected readonly string _logGroup;
        protected readonly string _logStream;
        private readonly IAmazonCloudWatchLogs _cloudWatchLogsClient;
        protected readonly Throttle _throttle;

        private Task _processingTask;
        protected string _sequenceToken;
        private bool _disposed;

        public AsyncCloudWatchLogsSink(string id, string sessionName, string logGroup, string logStream,
            IAmazonCloudWatchLogs cloudWatchLogsClient,
            ILogger logger,
            IMetrics metrics,
            IBookmarkManager bookmarkManager,
            NetworkStatus networkStatus,
            AWSBufferedSinkOptions options)
            : base(id, sessionName, logger, metrics, bookmarkManager, networkStatus, options)
        {
            Id = id;
            _logGroup = ResolveVariables(logGroup);
            _logStream = ResolveVariables(logStream);
            _cloudWatchLogsClient = cloudWatchLogsClient;
            // Set throttle at 5 requests per second
            _throttle = new AdaptiveThrottle(new TokenBucket(1, 5), _backoffFactor, _recoveryFactor, _minRateAdjustmentFactor);
        }

        /// <inheritdoc/>
        public override async ValueTask StartAsync(CancellationToken stopToken)
        {
            await base.StartAsync(stopToken);

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

            _processingTask = ProcessingTask(stopToken);

            _logger.LogInformation("CloudWatchLogsSink id {0} for log group {1} log stream {2} started.", Id, _logGroup, _logStream);
        }

        /// <inheritdoc/>
        public override async ValueTask StopAsync(CancellationToken gracefulStopToken)
        {
            _logger.LogInformation("Stopped");
            if (_processingTask is not null && !_processingTask.IsCompleted)
            {
                await _processingTask;
            }
        }

        /// <inheritdoc/>
        protected override int GetDelayMilliseconds(int recordCount, long batchBytes) => (int)_throttle.GetDelayMilliseconds(1);

        private async Task ProcessingTask(CancellationToken stopToken)
        {
            var events = new List<Envelope<InputLogEvent>>();

            while (!stopToken.IsCancellationRequested)
            {
                try
                {
                    if (events.Count == 0)
                    {
                        await _queue.GetNextBatchAsync(events, _bufferIntervalMs, stopToken);
                        events.Sort((r1, r2) => r1.Timestamp.CompareTo(r2.Timestamp));
                    }

                    await ThrottleAsync(events, stopToken);

                    await SendBatchAsync(events, stopToken);
                    PublishMetrics(MetricsConstants.CLOUDWATCHLOG_PREFIX);
                }
                catch (OperationCanceledException) when (stopToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _throttle.SetError();

                    if (IsRecoverableException(ex))
                    {
                        _logger.LogWarning("Encountered recoverable error of type {0}", ex.GetType());
                        Interlocked.Increment(ref _recoverableServiceErrors);
                        Interlocked.Add(ref _recordsFailedRecoverable, events.Count);

                        if (_throttle.ConsecutiveErrorCount > _maxAttempts && _queue.HasSecondaryQueue())
                        {
                            // Re-queue into the secondary queue when we've reached attempt limits
                            // This means that records will be out of order
                            _logger.LogDebug("Requeueing to secondary queue");
                            await _queue.PushSecondaryAsync(events);
                            events.Clear();
                        }
                        continue;
                    }

                    _logger.LogError(ex, "Error sending events (attempt={0})", _throttle.ConsecutiveErrorCount);
                    Interlocked.Increment(ref _nonrecoverableServiceErrors);
                    Interlocked.Add(ref _recordsFailedNonrecoverable, events.Count);
                    events.Clear();
                    continue;
                }
            }
        }

        /// <summary>
        /// Determines if this exception can be considered 'recoverable'
        /// </summary>
        /// <param name="ex">Exception encoutered.</param>
        protected override bool IsRecoverableException(Exception ex)
        {
            return base.IsRecoverableException(ex) ||
                    ex is ServiceUnavailableException ||
                    ex.Message.Contains(AWSConstants.SecurityTokenExpiredError) ||
                    ex.Message.Contains("Rate exceeded", StringComparison.OrdinalIgnoreCase);
        }

        private async Task SendBatchAsync(List<Envelope<InputLogEvent>> records, CancellationToken stopToken)
        {
            if (records.Count == 0)
            {
                return;
            }

            var logStreamName = ResolveTimestampInLogStreamName(records[0].Timestamp);
            var batchBytes = records.Sum(r => GetRecordSize(r));

            Interlocked.Add(ref _recordsAttempted, records.Count);
            Interlocked.Add(ref _bytesAttempted, batchBytes);
            Interlocked.Exchange(ref _clientLatency, (long)records.Average(r => (DateTime.UtcNow - r.Timestamp).TotalMilliseconds));

            _logger.LogDebug("Sending {0} records {1} bytes.", records.Count, batchBytes);

            var sendCount = 0;
            if (records[records.Count - 1].Timestamp - records[0].Timestamp <= _batchMaximumTimeSpan)
            {
                sendCount = records.Count;
            }
            else
            {
                while (sendCount < records.Count && records[sendCount].Timestamp - records[0].Timestamp <= _batchMaximumTimeSpan)
                {
                    sendCount++;
                }
            }

            var recordsToSend = records.Take(sendCount).Select(e => e.Data).ToList();
            var beforeSendTimestamp = Utility.GetElapsedMilliseconds();

            // If the sequence token is null, try to get it.
            // If the log stream doesn't exist, create it (by specifying "true" in the second parameter).
            // This should be the only place where a log stream is created.
            // This method will ensure that both the log group and stream exists,
            // so there is no need to handle a ResourceNotFound exception later.
            if (string.IsNullOrEmpty(_sequenceToken) || AWSConstants.NullString.Equals(_sequenceToken))
            {
                await GetSequenceTokenAsync(logStreamName, true, stopToken);
            }

            var request = new PutLogEventsRequest
            {
                LogGroupName = _logGroup,
                LogStreamName = logStreamName,
                SequenceToken = _sequenceToken,
                LogEvents = recordsToSend
            };

            try
            {
                // try sending the records and mark them as sent
                var response = await _cloudWatchLogsClient.PutLogEventsAsync(request, stopToken);
                Interlocked.Exchange(ref _latency, Utility.GetElapsedMilliseconds() - beforeSendTimestamp);

                // update sequence token
                _sequenceToken = response.NextSequenceToken;

                var recordsSent = recordsToSend.Count;
                var rejectedLogEventsInfo = response.RejectedLogEventsInfo;
                if (rejectedLogEventsInfo is not null)
                {
                    var sb = new StringBuilder()
                        .Append("CloudWatchLogsSink encountered some rejected logs.")
                        .AppendFormat(" ExpiredLogEventEndIndex {0}", rejectedLogEventsInfo.ExpiredLogEventEndIndex)
                        .AppendFormat(" TooNewLogEventStartIndex {0}", rejectedLogEventsInfo.TooNewLogEventStartIndex)
                        .AppendFormat(" TooOldLogEventEndIndex {0}", rejectedLogEventsInfo.TooOldLogEventEndIndex);
                    _logger.LogWarning(sb.ToString());
                    if (rejectedLogEventsInfo.TooNewLogEventStartIndex >= 0)
                    {
                        recordsSent -= recordsToSend.Count - rejectedLogEventsInfo.TooNewLogEventStartIndex;
                    }
                    var tooOldIndex = Math.Max(rejectedLogEventsInfo.TooNewLogEventStartIndex, rejectedLogEventsInfo.ExpiredLogEventEndIndex);
                    if (tooOldIndex > 0)
                    {
                        recordsSent -= tooOldIndex;
                    }
                    if (recordsSent < 0)
                    {
                        recordsSent = 0;
                    }
                }


                Interlocked.Add(ref _recordsSuccess, recordsSent);
                Interlocked.Add(ref _recordsFailedNonrecoverable, recordsToSend.Count - recordsSent);
                _logger.LogDebug("Successfully sent {0} records.", recordsSent);

                // save the bookmarks only for the records that were processed
                await SaveBookmarksAsync(records.Take(sendCount).ToList());

                records.RemoveRange(0, sendCount);
            }
            catch (AmazonCloudWatchLogsException acle)
            {
                Interlocked.Exchange(ref _latency, Utility.GetElapsedMilliseconds() - beforeSendTimestamp);

                // handle the types of exceptions we know how to handle
                // then return so that the records can be re-sent
                switch (acle)
                {
                    case InvalidSequenceTokenException iste:
                        _sequenceToken = iste.ExpectedSequenceToken;
                        break;
                    case ResourceNotFoundException:
                        await GetSequenceTokenAsync(logStreamName, true, stopToken);
                        break;
                    case DataAlreadyAcceptedException:
                        // this batch won't be accepted, skip it
                        await SaveBookmarksAsync(records.Take(sendCount).ToList());
                        records.RemoveRange(0, sendCount);
                        break;
                    case InvalidParameterException ipme:
                        // this can happens due to a log event being too large
                        // we already checked for this when creating the record,
                        // so best thing we can do here is to skip this batch and moveon
                        _logger.LogError(ipme, "Error sending records to CloudWatchLogs");
                        records.RemoveRange(0, sendCount);
                        break;
                    default:
                        // for other exceptions we rethrow so the main loop can catch it
                        throw;

                }
            }
        }

        private async Task GetSequenceTokenAsync(string logStreamName, bool createStreamIfNotExists, CancellationToken stopToken)
        {
            var request = new DescribeLogStreamsRequest
            {
                LogGroupName = _logGroup,
                LogStreamNamePrefix = logStreamName
            };

            try
            {
                var describeLogsStreamsResponse = await _cloudWatchLogsClient.DescribeLogStreamsAsync(request, stopToken);

                var logStream = describeLogsStreamsResponse.LogStreams
                    .FirstOrDefault(s => s.LogStreamName.Equals(logStreamName, StringComparison.CurrentCultureIgnoreCase));

                if (logStream == null)
                {
                    if (createStreamIfNotExists)
                    {
                        await CreateLogStreamAsync(logStreamName, stopToken);
                    }
                }
                else
                {
                    _sequenceToken = logStream.UploadSequenceToken;
                }
                _throttle.SetSuccess();
            }
            catch (ResourceNotFoundException rex)
            {
                // Create the log group if it doesn't exist.
                if (rex.Message.Contains("log group does not exist", StringComparison.OrdinalIgnoreCase))
                {
                    _logger?.LogInformation("Log group {0} does not exist. Creating it.", _logGroup);
                    await CreateLogGroupAsync(stopToken);

                    if (createStreamIfNotExists)
                    {
                        await CreateLogStreamAsync(logStreamName, stopToken);
                    }
                    return;
                }
            }
        }

        private async Task CreateLogStreamAsync(string logStreamName, CancellationToken stopToken)
        {
            _logger?.LogInformation("CloudWatchLogsSink creating log stream {0} in log group {1}", logStreamName, _logGroup);
            _ = await _cloudWatchLogsClient.CreateLogStreamAsync(new CreateLogStreamRequest
            {
                LogGroupName = _logGroup,
                LogStreamName = logStreamName
            }, stopToken);
            _throttle.SetSuccess();
        }

        protected virtual async Task CreateLogGroupAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("CloudWatchLogsSink creating log group {0}", _logGroup);

            await _cloudWatchLogsClient.CreateLogGroupAsync(new CreateLogGroupRequest(_logGroup), cancellationToken);
            _throttle.SetSuccess();
        }

        protected string ResolveTimestampInLogStreamName(DateTime timestamp)
            => Utility.ResolveVariables(_logStream, v => Utility.ResolveTimestampVariable(v, timestamp));

        /// <inheritdoc/>
        protected override long GetRecordSize(Envelope<InputLogEvent> record)
        {
            long recordSize = Encoding.UTF8.GetByteCount(record.Data.Message) + CloudWatchOverhead;
            if (recordSize > TwoHundredFiftySixKilobytes)
            {
                throw new ArgumentException("The maximum record size is 256KB. Record discarded.");
            }
            return recordSize;
        }

        /// <inheritdoc/>
        protected override InputLogEvent FormRecord(string stringRecord, IEnvelope envelope) => new InputLogEvent
        {
            Timestamp = envelope.Timestamp,
            Message = stringRecord
        };

        /// <inheritdoc/>
        protected override ISerializer<List<Envelope<InputLogEvent>>> GetPersistentQueueSerializer()
            => AWSSerializationUtility.InputLogEventListBinarySerializer;

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _cloudWatchLogsClient.Dispose();
                }

                _disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
