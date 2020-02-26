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
        private const int NOT_WRITING = 0;
        private const int WRITING = 1;
        private readonly string bookmarkDirectory;
        private readonly object bookmarkFileLock = new object();
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
        private int writingBookmark;

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

        public TimeSpan LastEventLatency { get; private set; }

        public void LoadSavedBookmark()
        {
            long position = 0;
            if (File.Exists(this.bookmarkPath))
            {
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
            }
            else
            {
                // If the bookmark file has been deleted, make sure that the in-memory bookmark is removed.
                BookmarkManager.RemoveBookmark(this.bookmarkId);
                this.eventBookmark = null;
            }

            this.bookmarkId = BookmarkManager.RegisterBookmark(this.Id, position, (pos) => this.SaveBookmark(pos)).Id;
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
            this.SaveBookmark(this.prevRecordId ?? 0);
        }

        public void SaveBookmark(long position)
        {
            if (this.InitialPosition == InitialPositionEnum.EOS) return;

            try
            {
                lock (this.bookmarkFileLock)
                {
                    if (this.lastSavedBookmark >= position) return;
                    var json = string.Format(BookmarkFormatString, this.logName, position);
                    File.WriteAllText(this.bookmarkPath, json);
                    this.lastSavedBookmark = position;
                }
            }
            catch (Exception ex)
            {
                this._logger?.LogError($"Eventlog Source {this.Id} unable to save bookmark: {ex.ToMinimized()}");
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

                // Only save the bookmark if bookmarking on buffer flush is disabled.
                if (!this.bookmarkOnBufferFlush)
                    this.StartSavingBookmark();

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
                    this.StartSavingBookmark();
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
                && this.InitialPositionTimestamp.Value > eventRecord.TimeCreated.Value)
            {
                return; //Don't send if timetamp initial position is in the future
            }

            EventRecordEnvelope envelope;
            if (this.customFilters.Length > 0)
            {
                envelope = new EventRecordEnvelope(eventRecord, true, this.bookmarkId); //Need event data for filtering

                //Don't send if any filter return false
                if (this.customFilters.Any(name => !EventInfoFilters.GetFilter(name)(envelope.Data))) return;

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

        private void StartSavingBookmark()
        {
            if (Interlocked.Exchange(ref this.writingBookmark, WRITING) == NOT_WRITING)
            {
                //Kick off a different thread to save bookmark so that the original thread can continue processing
                Task.Run((Action)SaveBookmark)
                    .ContinueWith((t) =>
                    {
                        Interlocked.Exchange(ref this.writingBookmark, NOT_WRITING);
                        if (t.IsFaulted && t.Exception is AggregateException aex)
                        {
                            aex.Handle(ex =>
                            {
                                this._logger?.LogError($"EventLogSource saving bookmark Exception {ex}");
                                return true;
                            });
                        }
                    });
            }
        }
    }
}
