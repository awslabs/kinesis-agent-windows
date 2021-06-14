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
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Amazon.Runtime;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.AWS
{
    /// <summary>
    /// Base class for high-perf async AWS sinks.
    /// </summary>
    /// <typeparam name="T">Type of records to be sent to AWS</typeparam>
    public abstract class AWSBufferedSink<T> : IEventSink
    {
        protected readonly IEnvelopeEvaluator<string> _textDecorationEvaluator;
        protected readonly IEnvelopeEvaluator<IDictionary<string, string>> _objectDecorationEvaluator;
        protected readonly ILogger _logger;
        protected readonly AsyncBatchQueue<Envelope<T>> _queue;
        protected readonly IMetrics _metrics;
        private readonly IBookmarkManager _bookmarkManager;
        private readonly NetworkStatus _networkStatus;
        protected CancellationToken _stopToken = default;

        protected readonly int _bufferIntervalMs;
        protected readonly string _format;
        protected readonly int _maxSecondaryQueueBatches;
        protected readonly int _maxAttempts;
        protected readonly double _jittingFactor;
        protected readonly double _backoffFactor;
        protected readonly double _recoveryFactor;
        protected readonly double _minRateAdjustmentFactor;
        protected readonly int _uploadNetworkPriority;

        protected long _recoverableServiceErrors;
        protected long _nonrecoverableServiceErrors;
        protected long _recordsAttempted;
        protected long _bytesAttempted;
        protected long _recordsSuccess;
        protected long _recordsFailedRecoverable;
        protected long _recordsFailedNonrecoverable;
        protected long _latency;
        protected long _clientLatency;
        protected int _hasBookmarkableSource = -1;

        public AWSBufferedSink(string id, string sessionName,
            ILogger logger,
            IMetrics metrics,
            IBookmarkManager bookmarkManager,
            NetworkStatus networkStatus,
            AWSBufferedSinkOptions options)
        {
            Id = id;
            _logger = logger;
            _metrics = metrics;
            _bookmarkManager = bookmarkManager;
            _networkStatus = networkStatus;
            _bufferIntervalMs = options.BufferIntervalMs;
            _format = options.Format;
            _maxSecondaryQueueBatches = options.QueueMaxBatches;
            var secondaryQueue = CreateSecondaryQueue(options, sessionName, logger);
            _queue = new AsyncBatchQueue<Envelope<T>>(options.QueueSizeItems,
                new long[] { options.MaxBatchSize, options.MaxBatchBytes },
                new Func<Envelope<T>, long>[] { r => 1, GetRecordSize },
                secondaryQueue);

            _maxAttempts = options.MaxAttempts;
            _jittingFactor = options.JittingFactor;
            _backoffFactor = options.BackoffFactor;
            _recoveryFactor = options.RecoveryFactor;
            _minRateAdjustmentFactor = options.MinRateAdjustmentFactor;
            _uploadNetworkPriority = options.UploadNetworkPriority;

            if (options.TextDecoration is not null)
            {
                _textDecorationEvaluator = new TextDecorationEvaluator(options.TextDecoration, ResolveRecordVariables);
            }

            if (options.TextDecorationEx is not null)
            {
                _textDecorationEvaluator = new TextDecorationExEvaluator(options.TextDecorationEx, EvaluateVariable, ResolveRecordVariable, logger);
            }

            if (options.ObjectDecoration is not null)
            {
                _objectDecorationEvaluator = new ObjectDecorationEvaluator(options.ObjectDecoration, ResolveRecordVariables);
            }

            if (options.ObjectDecorationEx is not null)
            {
                _objectDecorationEvaluator = new ObjectDecorationExEvaluator(options.ObjectDecorationEx, EvaluateVariable, ResolveRecordVariable, logger);
            }
        }

        private ISimpleQueue<List<Envelope<T>>> CreateSecondaryQueue(AWSBufferedSinkOptions options, string sessionName, ILogger logger)
        {
            if (options.SecondaryQueueType is null || options.QueueMaxBatches < 1)
            {
                return null;
            }

            if (options.SecondaryQueueType.Equals(ConfigConstants.QUEUE_TYPE_MEMORY, StringComparison.OrdinalIgnoreCase))
            {
                return new InMemoryQueue<List<Envelope<T>>>(options.QueueMaxBatches);
            }

            if (options.SecondaryQueueType.Equals(ConfigConstants.QUEUE_TYPE_FILE, StringComparison.OrdinalIgnoreCase))
            {
                var queuePath = Path.Combine(Utility.GetSessionQueuesDirectory(sessionName), Id);
                return new FilePersistentQueue<List<Envelope<T>>>(options.QueueMaxBatches, queuePath, GetPersistentQueueSerializer(), logger);
            }

            return null;
        }

        /// <inheritdoc/>
        public void OnNext(IEnvelope value)
        {
            try
            {
                var record = CreateRecord(value);
                var valTask = _queue.PushAsync(new Envelope<T>(record, value.Timestamp, value.BookmarkData, value.Position), _stopToken);
                if (!valTask.IsCompleted)
                {
                    // AsTask() allocates memory so we only calls that when the task could not complete synchronously
                    valTask.AsTask().GetAwaiter().GetResult();
                }
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing events");
            }
        }

        /// <summary>
        /// When implemented, create a record of type <typeparamref name="T"/> that the sink can work with.
        /// </summary>
        /// <param name="envelope">The record's envelope from the source.</param>
        /// <returns>Sink's record type.</returns>
        protected virtual T CreateRecord(IEnvelope envelope)
        {
            // TODO always forming a string is a bad idea. We should try to optimize this.
            var stringRecord = FormatToString(envelope);
            return FormRecord(stringRecord, envelope);
        }


        protected virtual bool IsRecoverableException(Exception ex)
        {
            return ex is HttpRequestException ||
                (ex is AmazonServiceException && ex.InnerException is HttpRequestException);
        }

        protected virtual async Task ThrottleAsync(List<Envelope<T>> records, CancellationToken stopToken)
        {
            var recordCount = records.Count;
            var batchBytes = records.Sum(r => GetRecordSize(r));
            var delay = GetDelayMilliseconds(recordCount, batchBytes);
            if (delay > 0)
            {
                await Task.Delay((int)(delay * (1.0d + Utility.Random.NextDouble() * _jittingFactor)), stopToken);
            }

            //Implement the network check after the throttle in case that the network becomes unavailable after throttle delay
            if (_networkStatus.DefaultProvider is not null)
            {
                var waitCount = 0;
                while (!_networkStatus.CanUpload(_uploadNetworkPriority))
                {
                    if (waitCount % 30 == 0) //Reduce the log entries
                    {
                        _logger.LogWarning("Network not available. Will retry.");
                    }
                    waitCount++;
                    await Task.Delay(10000, stopToken); //Wait 10 seconds
                }
            }
        }

        /// <summary>
        /// Saves bookmarks given a list of <see cref="Envelope{T}"/> records.
        /// It will group and order the records so that if they contain references to multiple bookmarks,
        /// all of those bookmarks will be updated with the maximum "Position" value of all records in the set.
        /// </summary>
        /// <param name="envelopes">A list of <see cref="Envelope{T}"/> objects that will be scanned for bookmark information.</param>
        protected async ValueTask SaveBookmarksAsync(List<Envelope<T>> envelopes)
        {
            // Ordering records is computationally expensive, so we only want to do it if bookmarking is enabled.
            // It's much cheaper to check a boolean property than to order the records and check if they have a bookmarkId.
            // Unfortunately, we don't know if the source is bookmarkable until we get some records, so we have to set this up
            // as a nullable property and set it's value on the first incoming batch of records.
            if (_hasBookmarkableSource < 0)
            {
                var hasBookmarkableSource = envelopes.Any(e => e.BookmarkData is not null);
                Interlocked.Exchange(ref _hasBookmarkableSource, hasBookmarkableSource ? 1 : 0);
            }

            // If this is not a bookmarkable source, return immediately.
            if (_hasBookmarkableSource == 0)
            {
                return;
            }

            // The events may not be in order, and we might have records from multiple sources, so we need to do a grouping.
            var bookmarks = envelopes
                .Select(e => e.BookmarkData)
                .Where(b => b is not null)
                .GroupBy(b => b.SourceKey);

            foreach (var bm in bookmarks)
            {
                // If the bookmarkId is 0 then bookmarking isn't enabled on the source, so we'll drop it.
                await _bookmarkManager.BookmarkCallback(bm.Key, bm);
            }
        }

        protected abstract int GetDelayMilliseconds(int recordCount, long batchBytes);

        //Can throw if record too long
        protected abstract long GetRecordSize(Envelope<T> record);

        /// <inheritdoc/>
        public string Id { get; set; }

        /// <inheritdoc/>
        public void OnCompleted() { }

        /// <inheritdoc/>
        public void OnError(Exception error) => _logger.LogError(error, "Error processing events");

        public virtual string FormatToString(IEnvelope envelope)
        {
            var stringFormat = envelope.GetMessage(_format);
            switch ((_format ?? string.Empty).ToLower())
            {
                case ConfigConstants.FORMAT_JSON:
                    if (_objectDecorationEvaluator != null)
                    {
                        IDictionary<string, string> attributes = _objectDecorationEvaluator.Evaluate(envelope);
                        stringFormat = JsonUtility.DecorateJson(stringFormat, attributes);
                    }
                    break;
                case ConfigConstants.FORMAT_XML:
                case ConfigConstants.FORMAT_XML_2:
                case ConfigConstants.FORMAT_RENDERED_XML:
                    //Do nothing until someone request this to be implemented
                    break;
                default:
                    if (_textDecorationEvaluator != null)
                    {
                        stringFormat = _textDecorationEvaluator.Evaluate(envelope);
                    }
                    break;
            }
            return stringFormat;
        }

        /// <summary>
        /// When implemented, transform the string-form of the record to the type <typeparamref name="T"/>
        /// which the sink can work with.
        /// </summary>
        /// <param name="stringRecord">Record as string.</param>
        /// <param name="envelope">Original record envelope from source.</param>
        /// <returns>Sink's record.</returns>
        protected abstract T FormRecord(string stringRecord, IEnvelope envelope);

        /// <inheritdoc/>
        public virtual ValueTask StartAsync(CancellationToken stopToken)
        {
            _stopToken = stopToken;
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc/>
        public abstract ValueTask StopAsync(CancellationToken gracefulStopToken);

        private string ResolveRecordVariables(string format, IEnvelope envelope)
        {
            string record = Utility.ResolveVariables(format, envelope, ResolveRecordVariable);
            record = ResolveVariables(record);
            return record;
        }

        protected abstract ISerializer<List<Envelope<T>>> GetPersistentQueueSerializer();

        protected virtual string ResolveVariables(string value)
        {
            return Utility.ResolveVariables(value, EvaluateVariable);
        }

        protected virtual string EvaluateVariable(string value)
        {
            var evaluated = Utility.ResolveVariable(value);
            if (string.IsNullOrEmpty(evaluated)) return evaluated;
            try
            {
                return AWSUtilities.EvaluateAWSVariable(evaluated);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex.ToMinimized());
                throw;
            }
        }

        private object ResolveRecordVariable(string variable, IEnvelope envelope)
        {
            if (variable.StartsWith("{"))
            {
                variable = variable.Substring(1, variable.Length - 2);
            }

            if (variable.StartsWith("$"))  //Local variable started with $
            {
                return envelope.ResolveLocalVariable(variable);
            }
            else if (variable.StartsWith("_"))  //Meta variable started with _
            {
                return envelope.ResolveMetaVariable(variable);
            }

            //Legacy timestamp, e.g., {timestamp:yyyyMmddHHmmss}. Add the curly braces back
            return Utility.ResolveTimestampVariable($"{{{variable}}}", envelope.Timestamp);
        }

        protected void PublishMetrics(string prefix)
        {
            _metrics?.PublishCounters(Id, MetricsConstants.CATEGORY_SINK, CounterTypeEnum.Increment, new Dictionary<string, MetricValue>()
            {
                { prefix + MetricsConstants.BYTES_ACCEPTED, new MetricValue(_bytesAttempted, MetricUnit.Bytes) },
                { prefix + MetricsConstants.RECORDS_ATTEMPTED, new MetricValue(_recordsAttempted) },
                { prefix + MetricsConstants.RECORDS_FAILED_NONRECOVERABLE, new MetricValue(_recordsFailedNonrecoverable) },
                { prefix + MetricsConstants.RECORDS_FAILED_RECOVERABLE, new MetricValue(_recordsFailedRecoverable) },
                { prefix + MetricsConstants.RECORDS_SUCCESS, new MetricValue(_recordsSuccess) },
                { prefix + MetricsConstants.RECOVERABLE_SERVICE_ERRORS, new MetricValue(_recoverableServiceErrors) },
                { prefix + MetricsConstants.NONRECOVERABLE_SERVICE_ERRORS, new MetricValue(_nonrecoverableServiceErrors) }
            });

            var currentBufSize = _queue.EstimateSize();
            var currentSecondaryQueueSize = _queue.EstimateSecondaryQueueSize();
            var bufferFull = _queue.Capacity <= currentBufSize;
            var secondaryQueueFull = _maxSecondaryQueueBatches <= _queue.EstimateSecondaryQueueSize();

            _metrics?.PublishCounters(Id, MetricsConstants.CATEGORY_SINK, CounterTypeEnum.CurrentValue, new Dictionary<string, MetricValue>()
            {
                { prefix + MetricsConstants.LATENCY, new MetricValue(_latency, MetricUnit.Milliseconds) },
                { prefix + MetricsConstants.CLIENT_LATENCY, new MetricValue(_clientLatency, MetricUnit.Milliseconds) },
                { prefix + MetricsConstants.BATCHES_IN_MEMORY_BUFFER, new MetricValue(currentBufSize, MetricUnit.Count) },
                { prefix + MetricsConstants.BATCHES_IN_PERSISTENT_QUEUE, new MetricValue(currentSecondaryQueueSize, MetricUnit.Count) },
                { prefix + MetricsConstants.IN_MEMORY_BUFFER_FULL, new MetricValue(bufferFull?1:0, MetricUnit.Count) },
                { prefix + MetricsConstants.PERSISTENT_QUEUE_FULL, new MetricValue(secondaryQueueFull ? 1:0, MetricUnit.Count) }
            });
            ResetIncrementalCounters();
        }

        protected void ResetIncrementalCounters()
        {
            Interlocked.Exchange(ref _recoverableServiceErrors, 0);
            Interlocked.Exchange(ref _nonrecoverableServiceErrors, 0);
            Interlocked.Exchange(ref _recordsAttempted, 0);
            Interlocked.Exchange(ref _bytesAttempted, 0);
            Interlocked.Exchange(ref _recordsSuccess, 0);
            Interlocked.Exchange(ref _recordsFailedRecoverable, 0);
            Interlocked.Exchange(ref _recordsFailedNonrecoverable, 0);
        }
    }
}
