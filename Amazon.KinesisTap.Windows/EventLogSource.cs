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
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Eventing.Reader;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Windows
{
    public class EventLogSource : DependentEventSource<EventInfo>, IBookmarkable
    {
        private readonly string _logName;
        private readonly string _query;
        EventLogWatcher _watcher;
        private ISubject<IEnvelope<EventInfo>> _recordSubject = new Subject<IEnvelope<EventInfo>>();
        private readonly EventLogQuery _eventLogQuery;
        private EventBookmark _eventBookmark;
        private TimeSpan _latency;
        private long? _prevRecordId;
        private readonly bool _includeEventData;
        private bool _isStartFromReset = false;
        private readonly string[] _customFilters;
        private readonly object _bookmarkLock = new object(); //Ensure only one thread is accessing the memory structure at a time

        private const int WRITING = 1;
        private const int NOT_WRITING = 0;
        private int _writingBookmark = NOT_WRITING; 

        public EventLogSource(string logName, string query, IPlugInContext context) : base(new ServiceDependency("EventLog"), context)
        {
            Guard.ArgumentNotNullOrEmpty(logName, nameof(logName));
            _logName = logName;
            _query = query;

            if (string.IsNullOrEmpty(query))
            {
                _eventLogQuery = new EventLogQuery(logName, PathType.LogName);
            }
            else
            {
                _eventLogQuery = new EventLogQuery(logName, PathType.LogName, query);
            }

            string includeEventData = context?.Configuration?["IncludeEventData"];
            if (!string.IsNullOrWhiteSpace(includeEventData))
            {
                _includeEventData = Convert.ToBoolean(includeEventData);
            }

            string customFilters = context?.Configuration?["CustomFilters"];
            if (!string.IsNullOrWhiteSpace(customFilters))
            {
                _customFilters = customFilters.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
                foreach(var filter in _customFilters)
                {
                    if (EventInfoFilters.GetFilter(filter) == null)
                    {
                        throw new ConfigurationException($"Custom filter {filter} does not exist. Please check the filter name.");
                    }
                }
            }
        }

        private void CreateWatcher(bool readExistingEvents)
        {
            _watcher = new EventLogWatcher(_eventLogQuery, _eventBookmark, readExistingEvents);
            _watcher.EventRecordWritten += OnEventRecordWritten;
        }

        public override void Start()
        {
            if (!_dependency.IsDependencyAvailable())
            {
                Reset();
                return;
            }
            switch(InitialPosition)
            {
                case InitialPositionEnum.BOS:
                case InitialPositionEnum.Timestamp:
                    LoadSavedBookmark();
                    CreateWatcher(true);
                    break;
                case InitialPositionEnum.Bookmark:
                    LoadSavedBookmark();
                    if (_eventBookmark != null)
                    {
                        CreateWatcher(true);
                    }
                    else
                    {
                        CreateWatcher(false);
                    }
                    break;
                case InitialPositionEnum.EOS:
                    if (_isStartFromReset && _eventBookmark != null)
                    {
                        CreateWatcher(true);
                    }
                    else
                    {
                        CreateWatcher(false);
                    }
                    break;
                default:
                    throw new NotImplementedException($"InitialPosition {InitialPosition} is not implemented.");
            }
            //_watcher.Enabled could potentially run for a long time if position is not future and there are large number of existing records
            //so we send to another thread and wait 1 second.
            //If the configuration is incorrect, we should get error within 1 second
            try
            {
                Task.Run(() => _watcher.Enabled = true).Wait(1000);

                _metrics?.InitializeCounters(this.Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment,
                    new Dictionary<string, MetricValue>()
                    {
                        { MetricsConstants.EVENTLOG_SOURCE_EVENTS_ERROR, MetricValue.ZeroCount },
                        { MetricsConstants.EVENTLOG_SOURCE_EVENTS_READ, MetricValue.ZeroCount }
                    });
                _logger?.LogInformation($"EventLogSource id {this.Id} logging {_logName} EventLog with query {_query} started.");
            }
            catch (AggregateException ae)
            {
                ae.Handle((x) =>
                {
                    if (_required || !(x is EventLogNotFoundException))
                    {
                        _logger?.LogError($"EventLogSource id {this.Id} logging {_logName} EventLog with query {_query} error {x.ToMinimized()}.");
                    }
                    return true;
                });
            }
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
                _isStartFromReset = true;
                Start();
            }
            finally
            {
                _isStartFromReset = false;
            }
        }


        public override void Stop()
        {
            MaybeCancelPolling();
            if (_watcher != null)
            {
                try
                {
                    Task.Run(() => _watcher.Enabled = false).Wait(1000);
                    _watcher.Dispose();
                    _watcher = null;
                    StartSavingBookmark();
                    _logger?.LogInformation($"EventLogSource id {this.Id} logging {_logName} EventLog with query {_query} stopped.");
                }
                catch (AggregateException ae)
                {
                    ae.Handle((x) =>
                    {
                        _logger?.LogError($"EventLogSource id {this.Id} logging {_logName} EventLog with query {_query} error {x.ToMinimized()}.");
                        return true;
                    });
                }
            }
        }

        /// <summary>
        /// Shutdown the watcher for this source, and invoke the base method which will wait until the service this source depends on (EventLog) is back to running state.
        /// The base method will call AfterDependencyRunning once the dependent service is running again.
        /// </summary>
        public override void Reset()
        {
            if (_watcher != null)
            {
                _logger?.LogWarning($"Resetting EventLogSource id {this.Id}.");
                try
                {
                    Task.Run(() => _watcher.Enabled = false).Wait(1000);
                }
                catch (Exception e)
                {
                    _logger?.LogWarning($"Unable to disable EventLogSource watcher id {this.Id} logging {_logName} EventLog with query {_query}.  Errors: {GetExceptionContent(e)}");
                }
                try
                {
                    _watcher.Dispose();
                    _watcher = null;
                    _logger?.LogInformation($"EventLogSource {this.Id} logging {_logName} EventLog with query {_query} stopped during reset.");
                }
                catch (Exception e)
                {
                    _logger?.LogError($"Disposal of watcher failed during reset of EventLogSource {this.Id} logging {_logName} with query {_query}.  Error: {e}");
                }
            }
            base.Reset();
        }

        private static string GetExceptionContent(Exception e)
        {
            if (e is AggregateException)
            {
                var innerExceptions = ((AggregateException)e).InnerExceptions;
                if (innerExceptions != null)
                {
                    return string.Join(", ", innerExceptions.Select((x) => GetExceptionContent(x)).ToArray());
                }
                else if (e.InnerException != null)
                {
                    return (GetExceptionContent(e.InnerException));
                }
                else
                {
                    return "Empty aggregate exception";
                }
            }

            return e.ToString();    
        }

        public void LoadSavedBookmark()
        {
            if (File.Exists(GetBookmarkFilePath()))
            {
                try
                {
                    string json = File.ReadAllText(GetBookmarkFilePath());
                    _eventBookmark = JsonConvert.DeserializeObject<EventBookmark>(json);
                }
                catch(Exception ex)
                {
                    _logger?.LogError($"Eventlog Source {Id} unable to load bookmark: {ex.ToMinimized()}");
                }
            }
        }

        public void SaveBookmark()
        {
            if (_eventBookmark == null) return;
            if (InitialPosition != InitialPositionEnum.EOS)
            {
                try
                {
                    string bookmarkDir = Path.GetDirectoryName(GetBookmarkFilePath());
                    if (!Directory.Exists(bookmarkDir))
                    {
                        Directory.CreateDirectory(bookmarkDir);
                    }
                    long recordIdWritten = -1;
                    long recordIdToWrite = _prevRecordId ?? 0;
                    EventBookmark bookmark = null;
                    while (recordIdWritten != recordIdToWrite) //Keep updating if there is something to write
                    {
                        lock (_bookmarkLock)
                        {
                            bookmark = _eventBookmark;
                        }
                        string json = JsonConvert.SerializeObject(bookmark);
                        File.WriteAllText(GetBookmarkFilePath(), json);
                        recordIdWritten = recordIdToWrite;
                        Thread.Sleep(200);  //Yield to reduce CPU usage
                        recordIdToWrite = _prevRecordId ?? 0; //Read the new ID to write
                    }
                }
                catch (Exception ex)
                {
                    _logger?.LogError($"Eventlog Source {Id} unable to save bookmark: {ex.ToMinimized()}");
                }
            }
        }

        public TimeSpan LastEventLatency => _latency;

        public override IDisposable Subscribe(IObserver<IEnvelope<EventInfo>> observer)
        {
            return this._recordSubject.Subscribe(observer);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_watcher != null) _watcher.Dispose();
            }
            base.Dispose(disposing);
        }

        private void OnEventRecordWritten(object sender, EventRecordWrittenEventArgs args)
        {
            try
            {
                if (args.EventException != null)
                {
                    if (args.EventException is OperationCanceledException 
                        || args.EventException is EventLogException)
                    {
                        Reset();
                    }
                    else
                    {
                        _recordSubject.OnError(args.EventException);
                    }
                }
                else
                {
                    EventRecord eventRecord = args.EventRecord;
                    if (eventRecord != null)
                    {
                        _latency = DateTime.Now.Subtract(eventRecord.TimeCreated ?? DateTime.Now);
                        ProcessRecord(eventRecord);
                    }
                }
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

        private void ProcessRecordError(Exception recordEx)
        {
            _logger?.LogError($"EventLogSource id {this.Id} logging {_logName} EventLog with query {_query} has record error {recordEx.ToMinimized()}.");
            _metrics?.PublishCounter(this.Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment,
                MetricsConstants.EVENTLOG_SOURCE_EVENTS_ERROR, 1, MetricUnit.Count);
        }

        private void ProcessRecord(EventRecord eventRecord)
        {
            //EventLogWatcher occasionally fire multiple times. The duplicated events have the same RecordId
            //This code deduplicate the events.
            if (_prevRecordId != eventRecord.RecordId)
            {
                _prevRecordId = eventRecord.RecordId;
                SendEventRecord(eventRecord);
                lock (_bookmarkLock)
                {
                    _eventBookmark = eventRecord.Bookmark;
                }
                StartSavingBookmark();
            }
            else
            {
                _logger?.LogInformation($"EventLogSource id {this.Id} skipped duplicated log: {eventRecord.ToXml()}.");
            }
        }

        private void StartSavingBookmark()
        {
            if (Interlocked.Exchange(ref _writingBookmark, WRITING) == NOT_WRITING)
            {
                //Kick off a different thread to save bookmark so that the original thread can continue processing
                Task.Run((Action)SaveBookmark)
                    .ContinueWith((t) =>
                    {
                        Interlocked.Exchange(ref _writingBookmark, NOT_WRITING);
                        if (t.IsFaulted && t.Exception is AggregateException aex)
                        {
                            aex.Handle(ex =>
                            {
                                _logger?.LogError($"EventLogSource saving bookmark Exception {ex}");
                                return true;
                            });
                        }
                    });
            }
        }

        private void SendEventRecord(EventRecord eventRecord)
        {
            if (InitialPosition == InitialPositionEnum.Timestamp 
                && InitialPositionTimestamp.HasValue 
                && eventRecord.TimeCreated.HasValue
                && InitialPositionTimestamp.Value > eventRecord.TimeCreated.Value)
            {
                return; //Don't send if timetamp initial position is in the future
            }

            EventRecordEnvelope envelope;
            if (_customFilters?.Length > 0)
            {
                envelope = new EventRecordEnvelope(eventRecord, true, _context); //Need event data for filtering

                //Don't send if any filter return false
                if (_customFilters.Any(name => !EventInfoFilters.GetFilter(name)(envelope.Data)))
                {
                    return;
                }

                //Strip off Event Data if not configured
                if (!_includeEventData)
                {
                    envelope.Data.EventData = null;
                }
            }
            else
            {
                envelope = new EventRecordEnvelope(eventRecord, _includeEventData, _context);
            }

            _recordSubject.OnNext(envelope);
            _metrics?.PublishCounter(this.Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment, 
                MetricsConstants.EVENTLOG_SOURCE_EVENTS_READ, 1, MetricUnit.Count);
        }

    }
}
