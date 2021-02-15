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
    using System.Net;
    using System.Reactive.Linq;
    using System.Threading.Tasks;
    using Amazon.CognitoIdentity.Model;
    using Amazon.KinesisTap.Core;
    using Amazon.KinesisTap.Core.Metrics;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging;

    public abstract class AWSBufferedEventSink<TRecord> : BatchEventSink<TRecord>
    {
        protected readonly int _maxAttempts;
        protected readonly double _jittingFactor;
        protected readonly double _backoffFactor;
        protected readonly double _recoveryFactor;
        protected readonly double _minRateAdjustmentFactor;
        protected readonly int _uploadNetworkPriority;

        //metrics
        protected long _recoverableServiceErrors;
        protected long _nonrecoverableServiceErrors;
        protected long _recordsAttempted;
        protected long _bytesAttempted;
        protected long _recordsSuccess;
        protected long _recordsFailedRecoverable;
        protected long _recordsFailedNonrecoverable;
        protected long _latency;
        protected long _clientLatency;

        protected bool? _hasBookmarkableSource;

        public AWSBufferedEventSink(
            IPlugInContext context,
            int defaultInterval,
            int defaultRecordCount,
            long maxBatchSize
        ) : base(context, defaultInterval, defaultRecordCount, maxBatchSize)
        {
            if (!int.TryParse(_config["MaxAttempts"], out _maxAttempts))
            {
                _maxAttempts = ConfigConstants.DEFAULT_MAX_ATTEMPTS;
            }

            if (!double.TryParse(_config["JittingFactor"], out _jittingFactor))
            {
                _jittingFactor = ConfigConstants.DEFAULT_JITTING_FACTOR;
            }

            if (!double.TryParse(_config["BackoffFactor"], out _backoffFactor))
            {
                _backoffFactor = ConfigConstants.DEFAULT_BACKOFF_FACTOR;
            }

            if (!double.TryParse(_config["RecoveryFactor"], out _recoveryFactor))
            {
                _recoveryFactor = ConfigConstants.DEFAULT_RECOVERY_FACTOR;
            }

            if (!double.TryParse(_config["MinRateAdjustmentFactor"], out _minRateAdjustmentFactor))
            {
                _minRateAdjustmentFactor = ConfigConstants.DEFAULT_MIN_RATE_ADJUSTMENT_FACTOR;
            }

            if (!int.TryParse(_config[ConfigConstants.UPLOAD_NETWORK_PRIORITY], out _uploadNetworkPriority))
            {
                _uploadNetworkPriority = ConfigConstants.DEFAULT_NETWORK_PRIORITY;
            }
        }

        protected BookmarkManager BookmarkManager => _context.BookmarkManager;

        protected NetworkStatus NetworkStatus => _context.NetworkStatus;

        protected override void OnNextBatch(List<Envelope<TRecord>> records)
        {
            if (records?.Count > 0)
            {
                this._logger?.LogTrace("[{0}] Waiting for new batch to be processed...", nameof(AWSBufferedEventSink<TRecord>.OnNextBatch));
                ThrottledOnNextAsync(records).Wait();
            }
        }

        protected virtual async Task ThrottledOnNextAsync(List<Envelope<TRecord>> records)
        {
            int recordCount = records.Count;
            long batchBytes = records.Select(r => GetRecordSize(r)).Sum();
            long delay = GetDelayMilliseconds(recordCount, batchBytes);
            if (delay > 0)
            {
                await Task.Delay((int)(delay * (1.0d
                    + Utility.Random.NextDouble() * _jittingFactor)));
            }

            //Implement the network check after the throttle in case that the network becomes unavailable after throttle delay
            if (!(NetworkStatus?.DefaultProvider is null))
            {
                int waitCount = 0;
                while (!NetworkStatus.CanUpload(_uploadNetworkPriority))
                {
                    if (waitCount % 30 == 0) //Reduce the log entries
                    {
                        _logger?.LogInformation("Network not available. Will retry.");
                    }
                    waitCount++;
                    await Task.Delay(10000); //Wait 10 seconds
                }
            }

            this._logger?.LogTrace("[{0}] Sending {1} records to sink...", nameof(AWSBufferedEventSink<TRecord>.ThrottledOnNextAsync), records.Count);
            await OnNextAsync(records, batchBytes);
        }

        protected abstract long GetDelayMilliseconds(int recordCount, long batchBytes);

        protected abstract Task OnNextAsync(List<Envelope<TRecord>> records, long batchBytes);

        protected abstract TRecord CreateRecord(string record, IEnvelope envelope);

        protected override string EvaluateVariable(string value)
        {
            string evaluated = base.EvaluateVariable(value);
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

        protected void ResetIncrementalCounters()
        {
            _recoverableServiceErrors = 0;
            _nonrecoverableServiceErrors = 0;
            _recordsAttempted = 0;
            _bytesAttempted = 0;
            _recordsSuccess = 0;
            _recordsFailedRecoverable = 0;
            _recordsFailedNonrecoverable = 0;
        }

        protected void PublishMetrics(string prefix)
        {
            _metrics?.PublishCounters(this.Id, MetricsConstants.CATEGORY_SINK, CounterTypeEnum.Increment, new Dictionary<string, MetricValue>()
            {
                { prefix + MetricsConstants.BYTES_ATTEMPTED, new MetricValue(_bytesAttempted, MetricUnit.Bytes) },
                { prefix + MetricsConstants.RECORDS_ATTEMPTED, new MetricValue(_recordsAttempted) },
                { prefix + MetricsConstants.RECORDS_FAILED_NONRECOVERABLE, new MetricValue(_recordsFailedNonrecoverable) },
                { prefix + MetricsConstants.RECORDS_FAILED_RECOVERABLE, new MetricValue(_recordsFailedRecoverable) },
                { prefix + MetricsConstants.RECORDS_SUCCESS, new MetricValue(_recordsSuccess) },
                { prefix + MetricsConstants.RECOVERABLE_SERVICE_ERRORS, new MetricValue(_recoverableServiceErrors) },
                { prefix + MetricsConstants.NONRECOVERABLE_SERVICE_ERRORS, new MetricValue(_nonrecoverableServiceErrors) }
            });

            _metrics?.PublishCounters(this.Id, MetricsConstants.CATEGORY_SINK, CounterTypeEnum.CurrentValue, new Dictionary<string, MetricValue>()
            {
                { prefix + MetricsConstants.LATENCY, new MetricValue(_latency, MetricUnit.Milliseconds) },
                { prefix + MetricsConstants.CLIENT_LATENCY, new MetricValue(_clientLatency, MetricUnit.Milliseconds) },
                { prefix + MetricsConstants.BATCHES_IN_MEMORY_BUFFER, new MetricValue(_buffer.GetCurrentBufferSize(), MetricUnit.Count) },
                { prefix + MetricsConstants.BATCHES_IN_PERSISTENT_QUEUE, new MetricValue(_buffer.GetCurrentPersistentQueueSize(), MetricUnit.Count) },
                { prefix + MetricsConstants.IN_MEMORY_BUFFER_FULL, new MetricValue(_buffer.IsBufferFull(), MetricUnit.Count) },
                { prefix + MetricsConstants.PERSISTENT_QUEUE_FULL, new MetricValue(_buffer.IsPersistentQueueFull(), MetricUnit.Count) }
            });
            ResetIncrementalCounters();
        }

        protected override TRecord CreateRecord(IEnvelope envelope)
        {
            string record = base.GetRecord(envelope);
            if (string.IsNullOrEmpty(record))
            {
                return default(TRecord);
            }
            else
            {
                return CreateRecord(record, envelope);
            }
        }

        protected virtual bool IsRecoverableException(Exception ex)
        {
            return (ex is AmazonServiceException
                && ex?.InnerException is WebException)
                || ex is NotAuthorizedException;
        }

        /// <summary>
        /// Saves bookmarks given a list of <see cref="Envelope{T}"/> records.
        /// It will group and order the records so that if they contain references to multiple bookmarks,
        /// all of those bookmarks will be updated with the maximum "Position" value of all records in the set.
        /// </summary>
        /// <typeparam name="T">Any object</typeparam>
        /// <param name="envelopes">A list of <see cref="Envelope{T}"/> objects that will be scanned for bookmark information.</param>
        protected void SaveBookmarks<T>(List<Envelope<T>> envelopes)
        {
            // Ordering records is computationally expensive, so we only want to do it if bookmarking is enabled.
            // It's much cheaper to check a boolean property than to order the records and check if they have a bookmarkId.
            // Unfortunately, we don't know if the source is bookmarkable until we get some records, so we have to set this up
            // as a nullable property and set it's value on the first incoming batch of records.
            if (!this._hasBookmarkableSource.HasValue)
                this._hasBookmarkableSource = envelopes.Any(i => i.BookmarkId.HasValue && i.BookmarkId.Value > 0);

            // If this is not a bookmarkable source, return immediately.
            if (!this._hasBookmarkableSource.Value) return;

            // The events may not be in order, and we might have records from multiple sources, so we need to do a grouping.
            var bookmarks = envelopes
                .GroupBy(i => i.BookmarkId)
                .Select(i => new { BookmarkId = i.Key ?? 0, Position = i.Max(j => j.Position) });

            // Start each new task asynchronously so that it doesn't block the sink.
            // We'll pass the sink's logger to the BookmarkManager so that the log entries can be
            // traced back to the sink that triggered the Update callback.
            foreach (var bm in bookmarks)
            {
                // If the bookmarkId is 0 then bookmarking isn't enabled on the source, so we'll drop it.
                if (bm.BookmarkId == 0) continue;
                Task.Run(() => BookmarkManager.SaveBookmark(bm.BookmarkId, bm.Position, this._logger));
            }
        }
    }
}
