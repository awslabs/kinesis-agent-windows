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
using System.Diagnostics.Eventing.Reader;
using System.Linq;
using System.Reactive.Linq;
using System.Runtime.Versioning;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Windows
{
    [SupportedOSPlatform("windows")]
    internal class EventLogSource : WindowsEventLogSourceBase<EventInfo>, IBookmarkable
    {
        private readonly EventLogQuery _eventLogQuery;
        private readonly Channel<Exception> _eventExceptionChannel = Channel.CreateBounded<Exception>(10);
        private readonly WindowsEventLogSourceOptions _options;

        private Task _execution;
        private long? _prevRecordId;
        private EventLogWatcher _watcher;
        private int _watcherStarting = 0;
        private bool _logStartedAfterSource = false;
        private CancellationToken _stopToken;

        public EventLogSource(string id, string logName, string query, IBookmarkManager bookmarkManager, WindowsEventLogSourceOptions options, IPlugInContext context)
            : base(id, logName, query, bookmarkManager, context)
        {
            Id = id;
            _options = options;

            _eventLogQuery = string.IsNullOrWhiteSpace(query)
                ? new EventLogQuery(_logName, PathType.LogName)
                : new EventLogQuery(_logName, PathType.LogName, _query);

            InitialPosition = options.InitialPosition;
            InitialPositionTimestamp = options.InitialPositionTimestamp;
        }

        public TimeSpan LastEventLatency { get; private set; }

        private void SaveBookmark(long position) => Utility.InterlockedExchangeIfGreaterThan(ref _bookmarkRecordPosition, position, position);

        public override async ValueTask StartAsync(CancellationToken stopToken)
        {
            await base.StartAsync(stopToken);
            _stopToken = stopToken;

            _execution = Execution(stopToken);
#if DEBUG
            _ = SamplingTask();
#endif
            _metrics?.InitializeCounters(Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment,
            new Dictionary<string, MetricValue>()
            {
                { MetricsConstants.EVENTLOG_SOURCE_EVENTS_ERROR, MetricValue.ZeroCount },
                { MetricsConstants.EVENTLOG_SOURCE_EVENTS_READ, MetricValue.ZeroCount }
            });
            _logger.LogInformation($"Started reading log '{_logName}', query {_query}");
        }

        public override async ValueTask StopAsync(CancellationToken cancellationToken)
        {
            if (_execution is not null)
            {
                await _execution;
            }

            _logger.LogInformation("Stopped");
        }

        private async Task Execution(CancellationToken stopToken)
        {
            try
            {
                await InitializeBookmarkLocation(stopToken);
            }
            catch (OperationCanceledException) when (stopToken.IsCancellationRequested)
            {
                return;
            }

            while (!stopToken.IsCancellationRequested)
            {
                try
                {
                    if (_watcher is not null)
                    {
                        _watcher.EventRecordWritten -= OnEventRecordWritten;
                        _watcher.Dispose();
                        _logger.LogInformation("Resetting event log watcher");
                    }
                    var readExistingEvent = _logStartedAfterSource ||
                        InitialPosition == InitialPositionEnum.BOS ||
                        InitialPosition == InitialPositionEnum.Timestamp ||
                        _eventBookmark != null;

                    _watcher = await CreateWatcher(readExistingEvent, stopToken);
                    _watcher.EventRecordWritten += OnEventRecordWritten;

                    // set the 'watcherStarting' flag so the callback can cancel
                    Interlocked.Exchange(ref _watcherStarting, 1);

                    // yield thread execution before enabling watcher, since this operation can be long running
                    await Task.Yield();

                    _watcher.Enabled = true;
                    Interlocked.Exchange(ref _watcherStarting, 0);
                }
                catch (OperationCanceledException)
                {
                    if (stopToken.IsCancellationRequested)
                    {
                        if (_watcher is not null)
                        {
                            _watcher.Enabled = false;
                            _watcher.EventRecordWritten -= OnEventRecordWritten;
                            _watcher.Dispose();
                        }

                        break;
                    }
                    continue;
                }
                catch (EventLogException ele) when (ele.Message.Contains("handle is invalid"))
                {
                    _logger.LogWarning(ele, "Error while reading log {0}", _logName);
                    continue;
                }

                try
                {
                    await _eventExceptionChannel.Reader.WaitToReadAsync(stopToken);
                    var reset = false;
                    while (_eventExceptionChannel.Reader.TryRead(out var ex))
                    {
                        reset = true;
                    }

                    if (reset)
                    {
                        continue;
                    }
                }
                catch (OperationCanceledException) when (stopToken.IsCancellationRequested)
                {
                    if (_watcher is not null)
                    {
                        _watcher.Enabled = false;
                        _watcher.EventRecordWritten -= OnEventRecordWritten;
                        _watcher.Dispose();
                    }
                    break;
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing) _watcher?.Dispose();
            base.Dispose(disposing);
        }

        private static string GetExceptionContent(Exception e)
        {
            if (e is AggregateException ae)
            {
                if (ae.InnerExceptions != null)
                    return string.Join(", ", ae.InnerExceptions.Select((x) => GetExceptionContent(x)).ToArray());
                else if (e.InnerException != null)
                    return (GetExceptionContent(e.InnerException));
                else
                    return "Empty aggregate exception";
            }

            return e.ToString();
        }

        private async ValueTask<EventLogWatcher> CreateWatcher(bool readExistingEvents, CancellationToken stopToken)
        {
            await EnsureDependencyAvailable(stopToken);

            _logger.LogDebug("Creating watcher for event log {0}, query {1}", _logName, _query);
            var watcher = new EventLogWatcher(_eventLogQuery, _eventBookmark, readExistingEvents);
            return watcher;
        }

#if DEBUG
        private long _total;
        private async Task SamplingTask()
        {
            var sw = new Stopwatch();
            sw.Start();
            var old = Interlocked.Read(ref _total);
            var elapsed = sw.ElapsedMilliseconds;
            while (!_stopToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(1000);
                    var newCount = Interlocked.Read(ref _total);
                    var newEl = sw.ElapsedMilliseconds;
                    var rate = (newCount - old) * 1000 / (newEl - elapsed);
                    _logger.LogInformation("Read rate {0} msg/s", rate);
                    elapsed = newEl;
                    old = newCount;
                }
                catch (OperationCanceledException ex)
                {
                    _logger.LogWarning(0, ex, "Error sampling, operation was canceled.");
                    break;
                }
            }
        }
#endif

        private static bool IsEventServiceException(Exception exception)
        {
            if (exception is null)
            {
                return false;
            }

            return exception is OperationCanceledException ||
                   exception is EventLogNotFoundException ||
                   exception is EventLogException ele && ele.Message.Contains("handle is invalid");
        }

        private void OnEventRecordWritten(object sender, EventRecordWrittenEventArgs args)
        {
            var watcherStarting = _watcherStarting > 0;
            if (watcherStarting)
            {
                _stopToken.ThrowIfCancellationRequested();
            }
            else if (_stopToken.IsCancellationRequested)
            {
                _watcher.Enabled = false;
                return;
            }

            try
            {
                if (IsEventServiceException(args.EventException))
                {
                    if (watcherStarting)
                    {
                        throw args.EventException;
                    }

                    _watcher.Enabled = false;
                    _eventExceptionChannel.Writer.TryWrite(args.EventException);
                    return;
                }

                if (args.EventRecord == null)
                {
                    return;
                }
#if DEBUG
                Interlocked.Increment(ref _total);
#endif
                LastEventLatency = DateTime.Now.Subtract(args.EventRecord.TimeCreated ?? DateTime.Now);
                ProcessRecord(args.EventRecord);
            }
            catch (Exception recordEx)
            {
                ProcessRecordError(recordEx);
            }
            finally
            {
                args.EventRecord?.Dispose();
            }
        }

        private void ProcessRecord(EventRecord eventRecord)
        {
            //EventLogWatcher occasionally fire multiple times. The duplicated events have the same RecordId
            //This code deduplicate the events.
            if (_prevRecordId != eventRecord.RecordId)
            {
                _prevRecordId = eventRecord.RecordId;
                SendEventRecord(eventRecord);
                _eventBookmark = eventRecord.Bookmark;

                if (!_options.BookmarkOnBufferFlush)
                {
                    // if bookmark-on-flush is not enabled, store the bookmark right away
                    SaveBookmark(_prevRecordId ?? 0);
                }
            }
            else
            {
                _logger.LogInformation($"EventLogSource id {Id} skipped duplicated log: {eventRecord.ToXml()}.");
            }
        }

        private void ProcessRecordError(Exception recordEx)
        {
            _logger.LogError(recordEx, $"EventLogSource LogName '{_logName}' with query {_query} has record error");
            _metrics?.PublishCounter(Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment,
                MetricsConstants.EVENTLOG_SOURCE_EVENTS_ERROR, 1, MetricUnit.Count);
        }

        private void SendEventRecord(EventRecord eventRecord)
        {
            if (InitialPosition == InitialPositionEnum.Timestamp
                && eventRecord.TimeCreated.HasValue
                && InitialPositionTimestamp > eventRecord.TimeCreated.Value.ToUniversalTime())
            {
                return; //Don't send if timetamp initial position is in the future
            }

            EventRecordEnvelope envelope;
            if (_options.CustomFilters.Length > 0)
            {
                // Create new envelope with event data for filtering
                envelope = new EventRecordEnvelope(eventRecord, true, _options.BookmarkOnBufferFlush
                    ? new IntegerPositionRecordBookmark(Id, Id, eventRecord.RecordId ?? 0)
                    : null);

                //Don't send if any filter return false
                if (_options.CustomFilters.Any(name => !EventInfoFilters.GetFilter(name)(eventRecord)))
                {
                    return;
                }

                //Strip off Event Data if not configured
                if (!_options.IncludeEventData)
                {
                    envelope.Data.EventData = null;
                }
            }
            else
            {
                envelope = new EventRecordEnvelope(eventRecord, _options.IncludeEventData, _options.BookmarkOnBufferFlush
                    ? new IntegerPositionRecordBookmark(Id, Id, eventRecord.RecordId ?? 0)
                    : null);
            }

            _recordSubject.OnNext(envelope);
            _metrics?.PublishCounter(Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment,
                MetricsConstants.EVENTLOG_SOURCE_EVENTS_READ, 1, MetricUnit.Count);
        }

        protected override ValueTask BeforeDependencyAvailable(CancellationToken cancellationToken)
        {
            _eventBookmark = null;
            _bookmarkRecordPosition = -1;
            return ValueTask.CompletedTask;
        }

        protected override ValueTask AfterDependencyAvailable(CancellationToken cancellationToken)
        {
            _logStartedAfterSource = true;
            _eventBookmark = null;
            _bookmarkRecordPosition = -1;
            return ValueTask.CompletedTask;
        }
    }
}
