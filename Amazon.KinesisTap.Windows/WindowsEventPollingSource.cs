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
using System.Diagnostics;
using System.Diagnostics.Eventing.Reader;
using System.Linq;
using System.Runtime.Versioning;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Windows
{
    /// <summary>
    /// Source that polls the event log for new events matching the specified query at a dynamically updated interval.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal class WindowsEventPollingSource : WindowsEventLogSourceBase<EventRecord>
    {
        private const int BatchSize = int.MaxValue;
        private readonly WindowsEventLogPollingSourceOptions _options;
        private Task _readingTask;
        private Task _processingTask;

        private readonly Channel<EventRecord> _eventChannel = Channel.CreateBounded<EventRecord>(1024);

        public WindowsEventPollingSource(string id, string logName, string query, IBookmarkManager bookmarkManager,
            WindowsEventLogPollingSourceOptions options, IPlugInContext context)
            : base(id, logName, query, bookmarkManager, context)
        {
            _options = options;

            InitialPosition = options.InitialPosition;
            InitialPositionTimestamp = options.InitialPositionTimestamp;
        }

#if DEBUG
        private Task _samplingRoutine;
#endif

        public override async ValueTask StartAsync(CancellationToken stopToken)
        {
            await base.StartAsync(stopToken);

            // start event reading routine
            _readingTask = ReaderTask(stopToken);
            _processingTask = ProcessingTask(stopToken);
#if DEBUG
            _samplingRoutine = SamplingTask(stopToken);
#endif

            _logger.LogInformation($"Started reading log '{_logName}', query {_query}");
            _logger.LogDebug("MaxReaderDelayMs: {0}, MinReaderDelayMs: {1}, DelayThreshold: {2}",
                _options.MaxReaderDelayMs, _options.MinReaderDelayMs, _options.DelayThreshold);
        }

        public override async ValueTask StopAsync(CancellationToken gracefulStopToken)
        {
            if (_readingTask is not null && !_readingTask.IsCompleted)
            {
                await _readingTask;
            }

            if (_processingTask is not null && !_processingTask.IsCompleted)
            {
                await _processingTask;
            }
#if DEBUG
            if (_samplingRoutine is not null && !_samplingRoutine.IsCompleted)
            {
                await _samplingRoutine;
            }
#endif
            _logger.LogInformation("Stopped");
        }

#if DEBUG
        private long _total;

        private async Task SamplingTask(CancellationToken stopToken)
        {
            _logger.LogDebug("Sampling Task started");

            var sw = new Stopwatch();
            sw.Start();
            var old = Interlocked.Read(ref _total);
            var elapsed = sw.ElapsedMilliseconds;
            while (!stopToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(1000, stopToken);
                    var newCount = Interlocked.Read(ref _total);
                    var newEl = sw.ElapsedMilliseconds;
                    var rate = (newCount - old) * 1000 / (newEl - elapsed);
                    _logger.LogDebug("Read rate {0} msg/s", rate);
                    elapsed = newEl;
                    old = newCount;
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
#endif

        private async Task ProcessingTask(CancellationToken stopToken)
        {
            while (!stopToken.IsCancellationRequested)
            {
                try
                {
                    var canRead = await _eventChannel.Reader.WaitToReadAsync(stopToken);
                    if (!canRead)
                    {
                        return;
                    }

                    while (_eventChannel.Reader.TryRead(out var ev))
                    {
                        SendEventRecord(ev);
                        if (!_options.BookmarkOnBufferFlush)
                        {
                            Interlocked.Exchange(ref _bookmarkRecordPosition, ev.RecordId ?? 0);
                        }
                        stopToken.ThrowIfCancellationRequested();
                    }
                }
                catch (OperationCanceledException) when (stopToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Encountered error while processing EventRecord");
                }
            }
        }

        private async Task ReaderTask(CancellationToken stopToken)
        {
            var delay = _options.MinReaderDelayMs;
            EventRecord entry = null;
            long? prevRecordId = null;

            try
            {
                await InitializeBookmarkLocation(stopToken);
            }
            catch (OperationCanceledException) when (stopToken.IsCancellationRequested)
            {
                return;
            }
            await Task.Yield();

            _logger.LogInformation("{0} Id {1} started reading", nameof(WindowsEventPollingSource), Id);
            while (!stopToken.IsCancellationRequested)
            {
                var batchCount = 0;
                try
                {
                    // setting batch size is pretty important for performance, as the .NET wrapper will attempt to query events in batches
                    // see https://referencesource.microsoft.com/#system.core/System/Diagnostics/Eventing/Reader/EventLogReader.cs,149
                    using (var eventLogReader = await CreateEventLogReader(_eventBookmark, stopToken))
                    {
                        while ((entry = DoRead(eventLogReader)) != null)
                        {
                            batchCount++;
#if DEBUG
                            Interlocked.Increment(ref _total);
#endif
                            var newEventBookmark = entry.Bookmark;
                            if (!OnEventRecord(entry, prevRecordId, stopToken))
                            {
                                await EnsureDependencyAvailable(stopToken);
                                break;
                            }

                            _eventBookmark = newEventBookmark;
                            prevRecordId = entry.RecordId;

                            if (batchCount == BatchSize)
                            {
                                _logger.LogDebug($"Read max possible number of events: {BatchSize}");
                                break;
                            }
                            stopToken.ThrowIfCancellationRequested();
                        }
                        if (entry is null)
                        {
                            _logger?.LogDebug("Read {0} events", batchCount);
                        }
                    }
                    delay = GetNextReaderDelayMs(delay, batchCount, _logger);
                    batchCount = 0;
                    _logger.LogDebug("Delaying {0}ms", delay);
                    await Task.Delay(delay, stopToken);
                }
                catch (OperationCanceledException) when (stopToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Event Reader stopped");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error reading events");
                }
            }

            _logger.LogInformation("{0} Id {1} stopped reading", nameof(WindowsEventPollingSource), Id);
        }

        private int GetNextReaderDelayMs(int currentDelay, int batchCount, ILogger logger)
        {
            if (batchCount >= _options.DelayThreshold)
            {
                logger.LogTrace("BatchCount = {0} > {1}, reducing delay", batchCount, _options.DelayThreshold);
                currentDelay /= 2;
                if (currentDelay < _options.MinReaderDelayMs)
                {
                    currentDelay = _options.MinReaderDelayMs;
                }
            }
            else
            {
                logger.LogTrace("BatchCount = {0} < {1}, increasing delay", batchCount, _options.DelayThreshold);
                currentDelay *= 2;
                if (currentDelay > _options.MaxReaderDelayMs)
                {
                    currentDelay = _options.MaxReaderDelayMs;
                }
            }

            logger?.LogTrace("New delay: {0} ms", currentDelay);
            return currentDelay;
        }

        /// <summary>
        /// Handles the incoming <see cref="EventRecord"/>
        /// </summary>
        /// <returns>True iff the reader should continue reading new records.</returns>
        private bool OnEventRecord(EventRecord eventRecord, long? prevRecordId, CancellationToken cancellationToken)
        {
            try
            {
                if (prevRecordId == eventRecord.RecordId)
                {
                    //This code deduplicate the events with the same RecordId
                    _logger.LogInformation("WindowsEventPollingSource Id {0} skipped duplicated log: {1}.", Id, eventRecord.ToXml());
                }

                if (InitialPosition == InitialPositionEnum.Timestamp
                    && eventRecord.TimeCreated.HasValue
                    && InitialPositionTimestamp > eventRecord.TimeCreated.Value.ToUniversalTime())
                {
                    //Don't send if timetamp initial position is in the future
                    return true;
                }

                if (_options.CustomFilters.Length > 0 && _options.CustomFilters.Any(name => !EventInfoFilters.GetFilter(name)(eventRecord)))
                {
                    //Don't send if any filter return false
                    return true;
                }

                var task = _eventChannel.Writer.WriteAsync(eventRecord, cancellationToken);
                if (!task.IsCompleted)
                {
                    // wait only if writing to buffer does not finish synchronously
                    task.AsTask().GetAwaiter().GetResult();
                }
            }
            catch (EventLogException ele)
            {
                if (ele.Message.Contains("The handle is invalid", StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
                else
                {
                    ProcessRecordError(ele);
                }
            }
            catch (Exception ex)
            {
                ProcessRecordError(ex);
            }
            return true;
        }

        private void SendEventRecord(EventRecord eventRecord)
        {
            var envelope = new RawEventRecordEnvelope(
                eventRecord,
                _options.IncludeEventData,
                _options.BookmarkOnBufferFlush && InitialPosition != InitialPositionEnum.EOS
                    ? new IntegerPositionRecordBookmark(Id, Id, eventRecord.RecordId ?? 0)
                    : null);

            _recordSubject.OnNext(envelope);
            _metrics?.PublishCounter(Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment,
                MetricsConstants.EVENTLOG_SOURCE_EVENTS_READ, 1, MetricUnit.Count);
        }

        private void ProcessRecordError(Exception recordEx)
        {
            _logger.LogError(recordEx, "Event log {0} with query {1} encountered error.", _logName, _query);
            _metrics?.PublishCounter(Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment,
                MetricsConstants.EVENTLOG_SOURCE_EVENTS_ERROR, 1, MetricUnit.Count);
        }

        protected override ValueTask BeforeDependencyAvailable(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        protected override ValueTask AfterDependencyAvailable(CancellationToken cancellationToken)
        {
            _eventBookmark = null;
            _bookmarkRecordPosition = -1;
            return ValueTask.CompletedTask;
        }
    }
}
