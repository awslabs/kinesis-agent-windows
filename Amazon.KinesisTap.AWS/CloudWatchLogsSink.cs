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
using Amazon.CloudWatchLogs;
using Amazon.CloudWatchLogs.Model;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging;
using Amazon.KinesisTap.Core.Metrics;

namespace Amazon.KinesisTap.AWS
{
    public class CloudWatchLogsSink : AWSBufferedEventSink<InputLogEvent>
    {
        IAmazonCloudWatchLogs _client;
        private string _logGroupName;
        private string _logStreamName;
        protected Throttle _throttle;

        private string _sequenceToken;

        //http://docs.aws.amazon.com/firehose/latest/dev/limits.html and http://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html
        public CloudWatchLogsSink(IPlugInContext context,
            IAmazonCloudWatchLogs client
            ) : base(context, 5, 500, 1024 * 1024)
        {
            if (_count > 10000)
            {
                throw new ArgumentException("The maximum buffer size for CloudWatchLog is 10000");
            }

            _client = client;

            _logGroupName = ResolveVariables(_config["LogGroup"]);
            _logStreamName = ResolveVariables(_config["LogStream"]);

            if (string.IsNullOrWhiteSpace(_logGroupName))
                throw new ArgumentException("'LogGroup' setting in config file cannot be null, whitespace, or 'LogGroup'");

            if (string.IsNullOrWhiteSpace(_logStreamName) || _logStreamName.Equals("LogStream"))
                throw new ArgumentException("'LogStream' setting in config file cannot be null, whitespace or 'LogStream'");

            _throttle = new AdaptiveThrottle(
                new TokenBucket(1, 5), //5 requests per second
                _backoffFactor,
                _recoveryFactor,
                _minRateAdjustmentFactor);
        }

        public override void Start()
        {
            StartAsync().Wait();
        }

        public async Task StartAsync()
        {
            string logStreamName = ResolveTimestampInLogStreamName(DateTime.UtcNow);
            try
            {
                await this.GetSequenceTokenAsync(logStreamName);
            }
            catch (ResourceNotFoundException rex)
            {
                if (rex.Message.IndexOf("log group does not exist") > -1)
                {
                    _logger?.LogInformation($"Log group {_logGroupName} does not exist. Attempt to create it.");
                    await CreateLogGroupAsync();
                    await CreateLogStreamAsync(logStreamName);
                }
            }

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
            _logger?.LogInformation($"CloudWatchLogsSink id {this.Id} for loggroup {_logGroupName} logstream {_logStreamName} started.");
        }

        public override void Stop()
        {
            base.Stop();
            _logger?.LogInformation($"CloudWatchLogsSink id {this.Id} for loggroup {_logGroupName} logstream {_logStreamName} stopped.");
        }

        protected override async Task OnNextAsync(List<Envelope<InputLogEvent>> records, long batchBytes)
        {
            if (records == null || records.Count == 0) return;

            try
            {
                _logger?.LogDebug($"CloudWatchLogsSink client {this.Id} sending {records.Count} records {batchBytes} bytes.");
                DateTime timestamp = records[0].Timestamp;
                string logStreamName = ResolveTimestampInLogStreamName(timestamp);
                if (string.IsNullOrEmpty(_sequenceToken))
                {
                    await GetSequenceTokenAsync(logStreamName);
                }

                var request = new PutLogEventsRequest
                {
                    LogGroupName = _logGroupName,
                    LogStreamName = logStreamName,
                    SequenceToken = _sequenceToken,
                    LogEvents = records
                        .Select(e => e.Data)
                        .OrderBy(e => e.Timestamp) //Added sort here in case messages are from multiple streams and they are not merged in order
                        .ToList()
                };

                bool attemptedCreatingLogStream = false;
                int invalidSequenceTokenCount = 0;
                while (true)
                {
                    DateTime utcNow = DateTime.UtcNow;
                    _clientLatency = (long)records.Average(r => (utcNow - r.Timestamp).TotalMilliseconds);

                    long elapsedMilliseconds = Utility.GetElapsedMilliseconds();
                    try
                    {
                        PutLogEventsResponse response = await _client.PutLogEventsAsync(request);
                        _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                        _throttle.SetSuccess();
                        _sequenceToken = response.NextSequenceToken;
                        RejectedLogEventsInfo rejectedLogEventsInfo = response.RejectedLogEventsInfo;
                        _recordsAttempted += records.Count;
                        _bytesAttempted += batchBytes;
                        if (rejectedLogEventsInfo != null)
                        {
                            StringBuilder sb = new StringBuilder();
                            sb.Append($"CloudWatchLogsSink client {this.Id} some of the logs where rejected.");
                            sb.Append($" ExpiredLogEventEndIndex {rejectedLogEventsInfo.ExpiredLogEventEndIndex}");
                            sb.Append($" TooNewLogEventStartIndex {rejectedLogEventsInfo.TooNewLogEventStartIndex}");
                            sb.Append($" TooOldLogEventEndIndex {rejectedLogEventsInfo.TooOldLogEventEndIndex}");
                            _logger?.LogError(sb.ToString());
                            long recordCount = records.Count - rejectedLogEventsInfo.ExpiredLogEventEndIndex - rejectedLogEventsInfo.TooOldLogEventEndIndex;
                            if (rejectedLogEventsInfo.TooOldLogEventEndIndex > 0)
                            {
                                recordCount -= records.Count - rejectedLogEventsInfo.TooNewLogEventStartIndex;
                            }
                            _recordsSuccess += recordCount;
                            _recordsFailedNonrecoverable += (records.Count - recordCount);
                        }
                        else
                        {
                            _recordsSuccess += records.Count;
                            _logger?.LogDebug($"CloudWatchLogsSink client {this.Id} succesfully sent {records.Count} records {batchBytes} bytes.");
                        }
                        break;
                    }
                    catch (ResourceNotFoundException)
                    {
                        _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                        _throttle.SetError();
                        //Logstream does not exist
                        if (attemptedCreatingLogStream)
                        {
                            _nonrecoverableServiceErrors++;
                            _recordsFailedNonrecoverable += records.Count;
                            throw;
                        }
                        else
                        {
                            _recoverableServiceErrors++;
                            _recordsFailedRecoverable += records.Count;
                            await CreateLogStreamAsync(logStreamName);
                        }
                    }
                    catch (AmazonCloudWatchLogsException ex)
                    {
                        _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                        _throttle.SetError();
                        if (ex is InvalidSequenceTokenException || ex is ServiceUnavailableException)
                        {
                            if (ex is InvalidSequenceTokenException invalidSequenceTokenException)
                            {
                                invalidSequenceTokenCount++;
                                _sequenceToken = invalidSequenceTokenException.GetExpectedSequenceToken();
                                //Sometimes we get a sequence token just say "null". This is obviously invalid
                                if ("null".Equals(_sequenceToken))
                                {
                                    _sequenceToken = null;
                                }
                                if (_sequenceToken != null && invalidSequenceTokenCount < 2)
                                {
                                    continue; //Immediately try so that the sequence token does not become invaid again
                                }
                            }
                            if (_buffer.Requeue(records, _throttle.ConsecutiveErrorCount < _maxAttempts))
                            {
                                _logger?.LogWarning($"CloudWatchLogsSink client {this.Id} attempt={_throttle.ConsecutiveErrorCount} exception={ex.Message}. Will retry.");
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
                _logger?.LogError($"CloudWatchLogsSink client {this.Id} exception (attempt={_throttle.ConsecutiveErrorCount}): {ex}");
            }

            PublishMetrics(MetricsConstants.CLOUDWATCHLOG_PREFIX);
        }

        protected override long GetRecordSize(Envelope<InputLogEvent> record)
        {
            const long CLOUDWATCH_OVERHEAD = 26L;
            const long TWO_FIFTY_SIX_KILOBYTES = 256 * 1024;
            long recordSize = Encoding.UTF8.GetByteCount(record.Data.Message) + CLOUDWATCH_OVERHEAD;
            if (recordSize > TWO_FIFTY_SIX_KILOBYTES)
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

        private async Task GetSequenceTokenAsync(string logStreamName)
        {
            var request = new DescribeLogStreamsRequest
            {
                LogGroupName = _logGroupName,
                LogStreamNamePrefix = logStreamName
            };

            var describeLogsStreamsResponse = await _client.DescribeLogStreamsAsync(request);

            LogStream logStream = describeLogsStreamsResponse.LogStreams
                .FirstOrDefault(s => s.LogStreamName.Equals(logStreamName, StringComparison.CurrentCultureIgnoreCase));

            if (logStream == null)
            {
                await CreateLogStreamAsync(logStreamName);
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
            _logger?.LogInformation($"CloudWatchLogsSink creating loggroup {_logGroupName}");
            try
            {
                await _client.CreateLogGroupAsync(new CreateLogGroupRequest(_logGroupName));
            }
            catch (Exception ex)
            {
                _logger?.LogError($"CloudWatchLogsSink create logroup {_logGroupName} exception: {ex}");
                throw;
            }
        }

        private async Task CreateLogStreamAsync(string logStreamName)
        {
            _logger?.LogInformation($"CloudWatchLogsSink creating logstream {_logGroupName}/{logStreamName}");
            try
            {
                var response = await _client.CreateLogStreamAsync(new CreateLogStreamRequest()
                {
                    LogGroupName = _logGroupName,
                    LogStreamName = logStreamName
                });
            }
            catch (Exception ex)
            {
                _logger?.LogError($"CloudWatchLogsSink create logstream {_logGroupName}/{logStreamName} exception: {ex}");
                throw;
            }
        }
    }
}
