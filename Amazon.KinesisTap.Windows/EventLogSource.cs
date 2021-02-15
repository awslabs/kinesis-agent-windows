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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.Eventing.Reader;
    using System.IO;
    using System.Linq;
    using System.Reactive.Linq;
    using System.Reactive.Subjects;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.KinesisTap.Core;
    using Amazon.KinesisTap.Core.Metrics;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;

    public class EventLogSource : DependentEventSource<EventInfo>, IBookmarkable
    {
        private const string BookmarkFormatString = "{{\"BookmarkText\":\"<BookmarkList>\r\n<Bookmark Channel='{0}' RecordId='{1}' IsCurrent='true'/>\r\n</BookmarkList>\"}}";
        private const string RecordIdRegex = "RecordId='(\\d+)'";
        private readonly string bookmarkDirectory;
        private readonly object bookmarkLock = new object();
        private readonly bool bookmarkOnBufferFlush;
        private readonly string[] customFilters;
        private readonly EventLogQuery eventLogQuery;
        private readonly bool includeEventData;
        private readonly string logName;
        private readonly string query;
        private readonly ISubject<IEnvelope<EventInfo>> recordSubject;
        private int bookmarkId;
        private string bookmarkPath;
        private EventBookmark eventBookmark;
        private bool isStartFromReset;
        private long lastSavedBookmark;
        private long? prevRecordId;
        private EventLogWatcher watcher;

        /// <summary>
        /// Synchronize access to bookmark-related data structures, including:
        /// <see cref="lastSavedBookmark"/>, <see cref="bookmarkFlushInterval\"/>, <see cref="lastBookmarkFlushedTimestamp"/> and the bookmark file.
        /// </summary>
        private readonly SemaphoreSlim bookmarkSemaphore = new SemaphoreSlim(1, 1);

        /// <summary>
        /// Period between bookmark flushes to file system.
        /// </summary>
        private TimeSpan bookmarkFlushInterval;

        /// <summary>
        /// A timestamp of when the bookmark was last flushed to file system.
        /// </summary>
        private DateTime lastBookmarkFlushedTimestamp = DateTime.MinValue;

        public EventLogSource(string logName, string query, IPlugInContext context) : base(new ServiceDependency("EventLog"), context)
        {
            Guard.ArgumentNotNullOrEmpty(logName, nameof(logName));
            this.logName = logName;
            this.query = query;
            this.recordSubject = new Subject<IEnvelope<EventInfo>>();
            this.bookmarkDirectory = Path.GetDirectoryName(this.GetBookmarkFilePath());
            if (!Directory.Exists(this.bookmarkDirectory))
                Directory.CreateDirectory(this.bookmarkDirectory);

            if (bool.TryParse(context?.Configuration?["BookmarkOnBufferFlush"] ?? "false", out bool bookmarkOnBufferFlush))
                this.bookmarkOnBufferFlush = bookmarkOnBufferFlush;

            if (string.IsNullOrWhiteSpace(query))
                this.eventLogQuery = new EventLogQuery(this.logName, PathType.LogName);
            else
                this.eventLogQuery = new EventLogQuery(this.logName, PathType.LogName, this.query);

            if (bool.TryParse(context?.Configuration?["IncludeEventData"], out bool incEventData))
                this.includeEventData = incEventData;

            this.customFilters = context?.Configuration?["CustomFilters"]?.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries) ?? new string[0];
            if (this.customFilters.Length > 0)
            {
                var badFilters = string.Join(",", this.customFilters.Where(i => EventInfoFilters.GetFilter(i) == null));
                if (!string.IsNullOrWhiteSpace(badFilters))
                    throw new ConfigurationException($"Custom filter/s '{badFilters}' do not exist. Please check the filter names.");
            }
        }

        /// <summary>
        /// For testing purpose
        /// </summary>
        internal long LastSavedBookmark => this.lastSavedBookmark;

        public TimeSpan LastEventLatency { get; private set; }

        public void LoadSavedBookmark()
        {
            long position = 0;
            if (File.Exists(this.bookmarkPath))
            {
                this.bookmarkSemaphore.Wait();
                bookmarkFlushInterval = TimeSpan.FromSeconds(20);
                try
                {
                    var json = File.ReadAllText(this.bookmarkPath);
                    this.eventBookmark = JsonConvert.DeserializeObject<EventBookmark>(json);
                    position = GetEventIdFromBookmark(json);
                }
                catch (Exception ex)
                {
                    this._logger?.LogError($"Eventlog Source {this.Id} unable to load bookmark: {ex.ToMinimized()}");
                }
                finally
                {
                    this.bookmarkSemaphore.Release();
                }
            }
            else
            {
                // If the bookmark file has been deleted, make sure that the in-memory bookmark is removed.
                BookmarkManager.RemoveBookmark(this.bookmarkId);
                this.eventBookmark = null;
            }

            if (bookmarkOnBufferFlush)
            {
                // Only register a bookmark in the BookmarkManager if BookmarkOnBufferFlush is enabled.
                this.bookmarkId = BookmarkManager.RegisterBookmark(this.Id, position, (pos) => this.SaveBookmarkInternal(pos, false)).Id;
            }
        }

        /// <summary>
        /// Shutdown the watcher for this source, and invoke the base method which will wait until the service this source depends on (EventLog) is back to running state.
        /// The base method will call AfterDependencyRunning once the dependent service is running again.
        /// </summary>
        public override void Reset()
        {
            if (this.watcher != null)
            {
                this._logger?.LogWarning($"Resetting EventLogSource id {this.Id}.");
                try
                {
                    Task.Run(() => this.watcher.Enabled = false).Wait(1000);
                }
                catch (Exception e)
                {
                    this._logger?.LogWarning($"Unable to disable EventLogSource watcher id {this.Id} logging {this.logName} EventLog with query {this.query}.  Errors: {GetExceptionContent(e)}");
                }

                try
                {
                    this.watcher?.Dispose();
                    this.watcher = null;
                    this._logger?.LogInformation($"EventLogSource {this.Id} logging {this.logName} EventLog with query {this.query} stopped during reset.");
                }
                catch (Exception e)
                {
                    this._logger?.LogError($"Disposal of watcher failed during reset of EventLogSource {this.Id} logging {this.logName} with query {this.query}.  Error: {e}");
                }
            }
            base.Reset();
        }

        public void SaveBookmark()
        {
            // this is called when the source stops, so bookmarks need to be flushed.
            this.SaveBookmarkInternal(this.prevRecordId ?? 0, true);
        }

        /// <summary>
        /// Set 'bookmarkFlushInterval' to zero, allowing 'SaveBookmarkInternal' to always flush to disk.
        /// </summary>
        private void AllowBookmarkFlush()
        {
            try
            {
                bookmarkSemaphore.Wait();
                bookmarkFlushInterval = TimeSpan.Zero;
            }
            finally
            {
                bookmarkSemaphore.Release();
            }
        }

        /// <summary>
        /// Stores the bookmark data with an option of forcing a write to the file system.
        /// </summary>
        /// <param name="position">The new bookmark position.</param>
        /// <param name="forceFlush">Force the bookmark to be saved to the file system.</param>
        internal void SaveBookmarkInternal(long position, bool forceFlush)
        {
            if (this.InitialPosition == InitialPositionEnum.EOS) return;

            // first, we store the updated bookmark value in 'lastSavedBookmark' if position > lastSavedBookmark
            var originalBookmark = Utility.InterlockedExchangeIfGreaterThan(ref this.lastSavedBookmark, position, position);
            if (originalBookmark >= this.lastSavedBookmark && !forceFlush)
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
            if (!this.bookmarkSemaphore.Wait(forceFlush ? Timeout.Infinite : 2000)) return;

            try
            {
                // if flush is required or the last flush happened long enough ago, store the bookmark to file
                if (forceFlush || (DateTime.Now - lastBookmarkFlushedTimestamp > bookmarkFlushInterval))
                {
                    // if another thread is saving bookmark concurrently, 'lastSavedBookmark' might already changed
                    // we make sure we're flushing the 'latest' saved bookmark
                    var flushedPosition = Interlocked.Read(ref this.lastSavedBookmark);
                    _logger?.LogDebug("Flushing bookmark location {0}", flushedPosition);
                    var json = string.Format(BookmarkFormatString, this.logName, flushedPosition);
                    File.WriteAllText(this.bookmarkPath, json);
                    lastBookmarkFlushedTimestamp = DateTime.Now;
                }
            }
            catch (Exception ex)
            {
                this._logger?.LogError($"Eventlog Source {this.Id} unable to save bookmark: {ex.ToMinimized()}");
            }
            finally
            {
                this.bookmarkSemaphore.Release();
            }
        }

        public override void Start()
        {
            this.bookmarkPath = this.GetBookmarkFilePath();

            if (!this._dependency.IsDependencyAvailable())
            {
                this.Reset();
                return;
            }

            switch (this.InitialPosition)
            {
                case InitialPositionEnum.BOS:
                case InitialPositionEnum.Timestamp:
                    this.LoadSavedBookmark();
                    this.CreateWatcher(true);
                    break;
                case InitialPositionEnum.Bookmark:
                    this.LoadSavedBookmark();
                    this.CreateWatcher(this.eventBookmark != null);
                    break;
                case InitialPositionEnum.EOS:
                    this.CreateWatcher(this.isStartFromReset && this.eventBookmark != null);
                    break;
                default:
                    throw new NotImplementedException($"InitialPosition {this.InitialPosition} is not implemented.");
            }

            //_watcher.Enabled could potentially run for a long time if position is not future and there are large number of existing records
            //so we send to another thread and wait 1 second.
            //If the configuration is incorrect, we should get error within 1 second
            try
            {
                Task.Run(() => this.watcher.Enabled = true).Wait(1000);

#if DEBUG
                total = 0;
                _ = SamplingTask();
#endif

                this._metrics?.InitializeCounters(this.Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment,
                    new Dictionary<string, MetricValue>()
                    {
                        { MetricsConstants.EVENTLOG_SOURCE_EVENTS_ERROR, MetricValue.ZeroCount },
                        { MetricsConstants.EVENTLOG_SOURCE_EVENTS_READ, MetricValue.ZeroCount }
                    });
                this._logger?.LogInformation($"EventLogSource id {this.Id} logging {this.logName} EventLog with query {this.query} started.");
            }
            catch (AggregateException ae)
            {
                ae.Handle((x) =>
                {
                    if (_required || !(x is EventLogNotFoundException))
                    {
                        this._logger?.LogError($"EventLogSource id {this.Id} logging {this.logName} EventLog with query {this.query} error {x.ToMinimized()}.");
                    }
                    return true;
                });
            }
        }

        public override void Stop()
        {
            this.MaybeCancelPolling();
            if (this.watcher == null) return;

            try
            {
                Task.Run(() => this.watcher.Enabled = false).Wait(1000);
                this.watcher.Dispose();
                this.watcher = null;

                if (!this.bookmarkOnBufferFlush)
                {
                    // Only save the bookmark if bookmarking on buffer flush is disabled.
                    // SaveBookmark is a synchronous call, and will block until the file is updated.
                    // Previously it was calling StartSavingBookmark, an async call, and was causing
                    // a race condition during configuration reloads for accessing the file.
                    // This ensures that the Start method can't be called on the source while it's
                    // still updating a bookmark.
                    _logger?.LogDebug("Source stopping, flushing bookmark");
                    this.SaveBookmark();
                }
                else
                {
                    // If bookmark-on-flush is enabled, we need to signal the source to try to flush the bookmark after the sink has streamed the events.
                    // Otherwise, next time the source starts up (e.g. after a Windows restart),
                    // the bookmark might be out-dated and KT might stream duplicate events. 
                    _logger?.LogDebug("Source stopping, allowing bookmarks to be flushed");
                    AllowBookmarkFlush();
                }

                this._logger?.LogInformation($"EventLogSource id {this.Id} logging {this.logName} EventLog with query {this.query} stopped.");
            }
            catch (AggregateException ae)
            {
                ae.Handle((x) =>
                {
                    this._logger?.LogError($"EventLogSource id {this.Id} logging {this.logName} EventLog with query {this.query} error {x.ToMinimized()}.");
                    return true;
                });
            }
        }

        public override IDisposable Subscribe(IObserver<IEnvelope<EventInfo>> observer)
        {
            return this.recordSubject.Subscribe(observer);
        }

        /// <summary>
        /// This method is invoked when the dependent service (EventLog) is running again.  The source
        /// is started with a special flag indicating that we should use the saved in-memory bookmark if that 
        /// is set.  This avoids losing data due to the EventLog service being stopped.
        /// </summary>
        protected override void AfterDependencyAvailable()
        {
            try
            {
                this.isStartFromReset = true;
                this.Start();
            }
            finally
            {
                this.isStartFromReset = false;
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing) this.watcher?.Dispose();
            base.Dispose(disposing);
        }

        private static long GetEventIdFromBookmark(string json)
        {
            var match = Regex.Match(json, RecordIdRegex);
            if (match.Success && match.Groups.Count == 2 && long.TryParse(match.Groups[1].Value, out long lastRecordId))
                return lastRecordId;
            return 0;
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

        private void CreateWatcher(bool readExistingEvents)
        {
            this.watcher = new EventLogWatcher(this.eventLogQuery, this.eventBookmark, readExistingEvents);
            this.watcher.EventRecordWritten += this.OnEventRecordWritten;
        }

#if DEBUG
        private long total = 0;
        private async Task SamplingTask()
        {
            var sw = new Stopwatch();
            sw.Start();
            var old = Interlocked.Read(ref this.total);
            var elapsed = sw.ElapsedMilliseconds;
            while (this.watcher.Enabled)
            {
                try
                {
                    await Task.Delay(1000);
                    var newCount = Interlocked.Read(ref this.total);
                    var newEl = sw.ElapsedMilliseconds;
                    var rate = (newCount - old) * 1000 / (newEl - elapsed);
                    _logger.LogDebug("Read rate {0} msg/s", rate);
                    elapsed = newEl;
                    old = newCount;
                }
                catch (TaskCanceledException ex)
                {
                    this._logger.LogWarning(0, ex, "Error sampling, operation was canceled.");
                    break;
                }
            }
        }
#endif

        private void OnEventRecordWritten(object sender, EventRecordWrittenEventArgs args)
        {
            try
            {
                if (args.EventException != null)
                {
                    if (args.EventException is OperationCanceledException || args.EventException is EventLogException)
                        this.Reset();
                    else
                        this.recordSubject.OnError(args.EventException);

                    return;
                }

                if (args.EventRecord == null) return;
#if DEBUG
                Interlocked.Increment(ref total);
#endif
                this.LastEventLatency = DateTime.Now.Subtract(args.EventRecord.TimeCreated ?? DateTime.Now);
                this.ProcessRecord(args.EventRecord);
            }
            catch (EventLogException ele)
            {
                if (ele.Message.Contains("The handle is invalid"))
                    this.Reset();
                else
                    this.ProcessRecordError(ele);
            }
            catch (Exception recordEx)
            {
                this.ProcessRecordError(recordEx);
            }
        }

        private void ProcessRecord(EventRecord eventRecord)
        {
            //EventLogWatcher occasionally fire multiple times. The duplicated events have the same RecordId
            //This code deduplicate the events.
            if (this.prevRecordId != eventRecord.RecordId)
            {
                this.prevRecordId = eventRecord.RecordId;
                this.SendEventRecord(eventRecord);
                lock (this.bookmarkLock)
                    this.eventBookmark = eventRecord.Bookmark;

                if (!this.bookmarkOnBufferFlush)
                {
                    // if bookmark-on-flush is not enabled, store the bookmark right away but do not force a flush
                    this.SaveBookmarkInternal(prevRecordId ?? 0, false);
                }
            }
            else
            {
                this._logger?.LogInformation($"EventLogSource id {this.Id} skipped duplicated log: {eventRecord.ToXml()}.");
            }
        }

        private void ProcessRecordError(Exception recordEx)
        {
            this._logger?.LogError($"EventLogSource id {this.Id} logging {this.logName} EventLog with query {this.query} has record error {recordEx.ToMinimized()}.");
            this._metrics?.PublishCounter(this.Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment,
                MetricsConstants.EVENTLOG_SOURCE_EVENTS_ERROR, 1, MetricUnit.Count);
        }

        private void SendEventRecord(EventRecord eventRecord)
        {
            if (this.InitialPosition == InitialPositionEnum.Timestamp
                && this.InitialPositionTimestamp.HasValue
                && eventRecord.TimeCreated.HasValue
                && this.InitialPositionTimestamp.Value > eventRecord.TimeCreated.Value.ToUniversalTime())
            {
                return; //Don't send if timetamp initial position is in the future
            }

            EventRecordEnvelope envelope;
            if (this.customFilters.Length > 0)
            {
                envelope = new EventRecordEnvelope(eventRecord, true, this.bookmarkId); //Need event data for filtering

                //Don't send if any filter return false
                if (this.customFilters.Any(name => !EventInfoFilters.GetFilter(name)(eventRecord))) return;

                //Strip off Event Data if not configured
                if (!this.includeEventData)
                    envelope.Data.EventData = null;
            }
            else
            {
                envelope = new EventRecordEnvelope(eventRecord, this.includeEventData, this.bookmarkId);
            }

            this.recordSubject.OnNext(envelope);
            this._metrics?.PublishCounter(this.Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment,
                MetricsConstants.EVENTLOG_SOURCE_EVENTS_READ, 1, MetricUnit.Count);
        }
    }
}
