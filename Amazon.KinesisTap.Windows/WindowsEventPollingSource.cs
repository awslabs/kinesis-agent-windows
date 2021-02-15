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
namespace Amazon.KinesisTap.Windows
{
    using System;
    using System.Diagnostics;
    using System.Diagnostics.Eventing.Reader;
    using System.IO;
    using System.Linq;
    using System.Reactive.Subjects;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.KinesisTap.Core;
    using Amazon.KinesisTap.Core.Metrics;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    /// <summary>
    /// Source that polls the event log for new events matching the specified query at a dynamically updated interval.
    /// </summary>
    public class WindowsEventPollingSource : DependentEventSource<EventRecord>
    {
        private const string BookmarkFormatString = "{{\"BookmarkText\":\"<BookmarkList>\r\n<Bookmark Channel='{0}' RecordId='{1}' IsCurrent='true'/>\r\n</BookmarkList>\"}}";
        private const string RecordIdRegex = "RecordId='(\\d+)'";

        private const int MaxReaderDelayMs = 5000;
        private const int MinReaderDelayMs = 100;
        private const int BatchSize = int.MaxValue;
        private const int BatchDelaySize = 50000;

        private bool _disposed = false;
        private readonly string _logName;
        private readonly string _query;
        private readonly bool _includeEventData;
        private string _bookmarkPath;
        private Task _readingRoutine;
        private EventBookmark _eventBookmark;
        private readonly bool bookmarkOnBufferFlush;
        private readonly string[] customFilters;
        private readonly ISubject<IEnvelope<EventRecord>> _recordSubject = new Subject<IEnvelope<EventRecord>>();
        private CancellationTokenSource _cts = new CancellationTokenSource();
        private int _bookmarkId;
        private long? _prevRecordId;

        public TimeSpan LastEventLatency { get; private set; }

        /// <summary>
        /// Synchronize access to bookmark-related data structures, including:
        /// <see cref="_lastSavedBookmark"/>, <see cref="_bookmarkFlushInterval\"/>, <see cref="_lastBookmarkFlushedTimestamp"/> and the bookmark file.
        /// </summary>
        private readonly SemaphoreSlim _bookmarkSemaphore = new SemaphoreSlim(1, 1);

        /// <summary>
        /// Period between bookmark flushes to file system.
        /// </summary>
        private TimeSpan _bookmarkFlushInterval;

        /// <summary>
        /// A timestamp of when the bookmark was last flushed to file system.
        /// </summary>
        private DateTime _lastBookmarkFlushedTimestamp = DateTime.MinValue;

        internal long _lastSavedBookmark;

        public WindowsEventPollingSource(string logName, string query, bool includeEventData, IPlugInContext context)
            : base(new ServiceDependency("EventLog"), context)
        {
            _logName = logName;
            _query = query;
            _includeEventData = includeEventData;
            customFilters = context?.Configuration?["CustomFilters"]?.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries) ?? new string[0];
            if (customFilters.Length > 0)
            {
                var badFilters = string.Join(",", customFilters.Where(i => EventInfoFilters.GetFilter(i) == null));
                if (!string.IsNullOrWhiteSpace(badFilters))
                    throw new ConfigurationException($"Custom filter/s '{badFilters}' do not exist. Please check the filter names.");
            }
            if (bool.TryParse(context?.Configuration?["BookmarkOnBufferFlush"] ?? "false", out bool bookmarkOnBufferFlush))
            {
                this.bookmarkOnBufferFlush = bookmarkOnBufferFlush;
            }
        }

#if DEBUG
        private Task _samplingRoutine;
#endif

        public override void Start()
        {
            _bookmarkPath = GetBookmarkFilePath();
            if (!_dependency.IsDependencyAvailable())
            {
                Reset();
                return;
            }

            if (InitialPosition != InitialPositionEnum.EOS)
            {
                LoadSavedBookmark();
            }

            // start event reading routine
            _readingRoutine = ReaderRoutine();
#if DEBUG
            _samplingRoutine = SamplingTask();
#endif
        }

        public override void Stop()
        {
            _cts.Cancel();
            _readingRoutine.GetAwaiter().GetResult();
#if DEBUG
            _samplingRoutine.GetAwaiter().GetResult();
#endif
        }

        public override IDisposable Subscribe(IObserver<IEnvelope<EventRecord>> observer)
        {
            return _recordSubject.Subscribe(observer);
        }

        /// <summary>
        /// Load the saved bookmark into memory from disk, if the bookmark exists.
        /// </summary>
        public void LoadSavedBookmark()
        {
            long position = 0;
            if (File.Exists(_bookmarkPath))
            {
                _bookmarkSemaphore.Wait();
                _bookmarkFlushInterval = TimeSpan.FromSeconds(20);
                try
                {
                    var json = File.ReadAllText(_bookmarkPath);
                    _eventBookmark = JsonConvert.DeserializeObject<EventBookmark>(json);
                    position = GetEventIdFromBookmark(json);
                }
                catch (Exception ex)
                {
                    _logger?.LogError("WindowsEventPollingSource {0} unable to load bookmark: {1}", Id, ex.ToMinimized());
                }
                finally
                {
                    _bookmarkSemaphore.Release();
                }
            }
            else
            {
                // If the bookmark file has been deleted, make sure that the in-memory bookmark is removed.
                BookmarkManager.RemoveBookmark(_bookmarkId);
                _eventBookmark = null;
            }

            if (bookmarkOnBufferFlush)
            {
                // Only register a bookmark in the BookmarkManager if BookmarkOnBufferFlush is enabled.
                _bookmarkId = BookmarkManager.RegisterBookmark(Id, position, (pos) => SaveBookmarkInternal(pos, false)).Id;
            }
        }

        protected override void AfterDependencyAvailable()
        {
            _cts = new CancellationTokenSource();
            this.Start();
        }
#if DEBUG
        private long _total = 0;
        private async Task SamplingTask()
        {
            var sw = new Stopwatch();
            sw.Start();
            var old = Interlocked.Read(ref _total);
            var elapsed = sw.ElapsedMilliseconds;
            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(1000);
                    var newCount = Interlocked.Read(ref _total);
                    var newEl = sw.ElapsedMilliseconds;
                    var rate = (newCount - old) * 1000 / (newEl - elapsed);
                    _logger?.LogDebug("Read rate {0} msg/s", rate);
                    elapsed = newEl;
                    old = newCount;
                }
                catch (TaskCanceledException ex)
                {
                    this._logger?.LogWarning(0, ex, "Error sampling, operation was canceled.");
                    break;
                }
            }
        }
#endif

        private async Task ReaderRoutine()
        {
            var delay = MinReaderDelayMs;
            EventRecord entry = null;

            EventBookmark eventBookmark = null;
            using (var eventLogReader = new EventLogReader(new EventLogQuery(_logName, PathType.LogName, _query)))
            {
                if (eventBookmark == null)
                {
                    eventLogReader.Seek(SeekOrigin.Begin, 0);

                    // now, for some odd reason, Seek to End will set the reader BEFORE the last record,
                    // so we do a random read here 
                    var last = DoRead(eventLogReader);
                    eventBookmark = last?.Bookmark;
                    last?.Dispose();
                }
            }

            await Task.Yield();
            _logger?.LogInformation("{0} Id {1} started reading", nameof(WindowsEventPollingSource), Id);
            while (!_cts.Token.IsCancellationRequested)
            {
                int batchCount = 0;
                try
                {
                    // setting batch size is pretty important for performance, as the .NET wrapper will attempt to query events in batches
                    // see https://referencesource.microsoft.com/#system.core/System/Diagnostics/Eventing/Reader/EventLogReader.cs,149
                    using (var eventLogReader = new EventLogReader(new EventLogQuery(_logName, PathType.LogName, _query), eventBookmark)
                    {
                        BatchSize = 128
                    })
                    {
                        while ((entry = DoRead(eventLogReader)) != null)
                        {
                            batchCount++;
#if DEBUG
                            Interlocked.Increment(ref _total);
#endif
                            eventBookmark = entry.Bookmark;
                            this.LastEventLatency = DateTime.Now.Subtract(entry.TimeCreated ?? DateTime.Now);
                            OnEventRecord(entry);
                            if (batchCount == BatchSize)
                            {
                                this._logger?.LogDebug($"Read max possible number of events: {BatchSize}");
                                break;
                            }
                            _cts.Token.ThrowIfCancellationRequested();
                        }
                        if (entry is null)
                        {
                            _logger?.LogDebug("Read {0} events", batchCount);
                        }
                    }
                    delay = GetNextReaderDelayMs(delay, batchCount, this._logger);
                    batchCount = 0;
                    _logger?.LogDebug("Delaying {0}ms", delay);
                    await Task.Delay(delay, _cts.Token);
                }
                catch (OperationCanceledException ex)
                {
                    _logger?.LogWarning(0, ex, "Error reading events, operation was canceled.");
                    break;
                }
                catch (Exception ex)
                {
                    _logger?.LogError(0, ex, "Error reading events");
                }
            }

            _logger?.LogInformation("{0} Id {1} stopped reading", nameof(WindowsEventPollingSource), Id);
        }

        /// <summary>
        /// The implementation of EventLogReader throws 'InvalidOperationException' or 'EventLogException' when the end of stream is reached
        /// (https://referencesource.microsoft.com/#system.core/System/Diagnostics/Eventing/Reader/EventLogReader.cs,177)
        /// so we capture that and return null instead.
        /// </summary>
        private EventRecord DoRead(EventLogReader reader)
        {
            try
            {
                return reader.ReadEvent();
            }
            catch (InvalidOperationException)
            {
                return null;
            }
            catch (EventLogException)
            {
                return null;
            }
        }

        private static int GetNextReaderDelayMs(int currentDelay, int batchCount, ILogger logger)
        {
            if (batchCount >= BatchDelaySize)
            {
                logger?.LogTrace("BatchCount = {0} > {1}, reducing delay", batchCount, BatchDelaySize);
                currentDelay /= 2;
                if (currentDelay < MinReaderDelayMs)
                {
                    currentDelay = MinReaderDelayMs;
                }
            }
            else
            {
                logger?.LogTrace("BatchCount = {0} < {1}, increasing delay", batchCount, BatchDelaySize);
                currentDelay *= 2;
                if (currentDelay > MaxReaderDelayMs)
                {
                    currentDelay = MaxReaderDelayMs;
                }
            }

            logger?.LogTrace("New delay: {0} ms", currentDelay);
            return currentDelay;
        }

        /// <summary>
        /// Stores the bookmark data with an option of forcing a write to the file system.
        /// </summary>
        /// <param name="position">The new bookmark position.</param>
        /// <param name="forceFlush">Force the bookmark to be saved to the file system.</param>
        internal void SaveBookmarkInternal(long position, bool forceFlush)
        {
            if (InitialPosition == InitialPositionEnum.EOS) return;

            // first, we store the updated bookmark value in 'lastSavedBookmark' if position > lastSavedBookmark
            var originalBookmark = Utility.InterlockedExchangeIfGreaterThan(ref _lastSavedBookmark, position, position);
            if (originalBookmark >= _lastSavedBookmark && !forceFlush)
            {
                // 'lastSavedBookmark' is not updated and we're not required to flush
                return;
            }

            // Try to enter the critical section to flush the bookmark and 'lastBookmarkFlushedTimestamp'
            // If flush is required, wait indefinitely.
            // Otherwise only wait for 2 seconds before abandoning the update attempt.
            // This should never happen, but if it does, it's not a problem
            // because there's likely another Task, started later, waiting
            // to get the Semaphore as well.
            if (!_bookmarkSemaphore.Wait(forceFlush ? Timeout.Infinite : 2000)) return;

            try
            {
                // if flush is required or the last flush happened long enough ago, store the bookmark to file
                if (forceFlush || (DateTime.Now - _lastBookmarkFlushedTimestamp > _bookmarkFlushInterval))
                {
                    // if another thread is saving bookmark concurrently, 'lastSavedBookmark' might already changed
                    // we make sure we're flushing the 'latest' saved bookmark
                    var flushedPosition = Interlocked.Read(ref _lastSavedBookmark);
                    _logger?.LogDebug("Flushing bookmark location {0}", flushedPosition);
                    var json = string.Format(BookmarkFormatString, _logName, flushedPosition);
                    File.WriteAllText(_bookmarkPath, json);
                    _lastBookmarkFlushedTimestamp = DateTime.Now;
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(0, ex, $"WindowsEventPollingSource {Id} unable to save bookmark");
            }
            finally
            {
                _bookmarkSemaphore.Release();
            }
        }


        private void OnEventRecord(EventRecord eventRecord)
        {
            try
            {
                ProcessRecord(eventRecord);
            }
            catch (EventLogException ele)
            {
                if (ele.Message.Contains("The handle is invalid"))
                {
                    Reset();
                }
                else
                {
                    ProcessRecordError(ele);
                }
            }
            catch (Exception recordEx)
            {
                ProcessRecordError(recordEx);
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

                if (!bookmarkOnBufferFlush)
                {
                    // if bookmark-on-flush is not enabled, store the bookmark right away but do not force a flush
                    SaveBookmarkInternal(_prevRecordId ?? 0, false);
                }
            }
            else
            {
                _logger?.LogInformation("WindowsEventPollingSource Id {0} skipped duplicated log: {1}.", Id, eventRecord.ToXml());
            }
        }

        private void SendEventRecord(EventRecord eventRecord)
        {
            if (InitialPosition == InitialPositionEnum.Timestamp
                && InitialPositionTimestamp.HasValue
                && eventRecord.TimeCreated.HasValue
                && InitialPositionTimestamp.Value > eventRecord.TimeCreated.Value.ToUniversalTime())
            {
                return; //Don't send if timetamp initial position is in the future
            }

            if (customFilters.Any(name => !EventInfoFilters.GetFilter(name)(eventRecord)))
            {
                //Don't send if any filter return false
                return;
            }

            var envelope = new RawEventRecordEnvelope(eventRecord, _includeEventData, 0);

            _recordSubject.OnNext(envelope);
            _metrics?.PublishCounter(Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment,
                MetricsConstants.EVENTLOG_SOURCE_EVENTS_READ, 1, MetricUnit.Count);
        }

        private void ProcessRecordError(Exception recordEx)
        {
            _logger?.LogError(0, recordEx, "{0} Id {1} logging {2} EventLog with query {3} encountered error.", nameof(WindowsEventPollingSource), Id, _logName, _query);
            _metrics?.PublishCounter(Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment,
                MetricsConstants.EVENTLOG_SOURCE_EVENTS_ERROR, 1, MetricUnit.Count);
        }

        private static long GetEventIdFromBookmark(string json)
        {
            var match = Regex.Match(json, RecordIdRegex);
            if (match.Success && match.Groups.Count == 2 && long.TryParse(match.Groups[1].Value, out long lastRecordId))
                return lastRecordId;
            return 0;
        }

        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _cts.Dispose();
                }
                _disposed = true;
            }

            base.Dispose(disposing);
        }
    }
}
