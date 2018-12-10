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
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Windows
{
    public class EventLogSource : EventSource<EventInfo>, IBookmarkable, IDisposable
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

        public EventLogSource(string logName, string query, IPlugInContext context) : base(context)
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
        }

        private void CreateWatcher(bool readExistingEvents)
        {
            _watcher = new EventLogWatcher(_eventLogQuery, _eventBookmark, readExistingEvents);
            _watcher.EventRecordWritten += OnEventRecordWritten;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        public override void Start()
        {
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
                    CreateWatcher(false);
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
                    _logger?.LogError($"EventLogSource id {this.Id} logging {_logName} EventLog with query {_query} error {x}.");
                    return true;
                });
            }
        }

        public override void Stop()
        {
            if (_watcher != null)
            {
                try
                {
                    Task.Run(() => _watcher.Enabled = false).Wait(1000);
                    _watcher.Dispose();
                    _watcher = null;
                    SaveBookmark();
                    _logger?.LogInformation($"EventLogSource id {this.Id} logging {_logName} EventLog with query {_query} stopped.");
                }
                catch (AggregateException ae)
                {
                    ae.Handle((x) =>
                    {
                        _logger?.LogError($"EventLogSource id {this.Id} logging {_logName} EventLog with query {_query} error {x}.");
                        return true;
                    });
                }
            }
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
                    _logger.LogError($"Eventlog Source {Id} unable to load bookmark: {ex.ToString()}");
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

                    string json = JsonConvert.SerializeObject(_eventBookmark);
                    File.WriteAllText(GetBookmarkFilePath(), json);
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Eventlog Source {Id} unable to save bookmark: {ex.ToString()}");
                }
            }
        }

        public TimeSpan LastEventLatency => _latency;

        public override IDisposable Subscribe(IObserver<IEnvelope<EventInfo>> observer)
        {
            return this._recordSubject.Subscribe(observer);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_watcher != null) _watcher.Dispose();
            }
        }

        private void OnEventRecordWritten(object sender, EventRecordWrittenEventArgs args)
        {
            try
            {
                if (args.EventException != null)
                {
                    _recordSubject.OnError(args.EventException);
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
            catch (Exception recordEx)
            {
                _logger?.LogError($"EventLogSource id {this.Id} logging {_logName} EventLog with query {_query} has record error {recordEx}.");
                _metrics?.PublishCounter(this.Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment,
                    MetricsConstants.EVENTLOG_SOURCE_EVENTS_ERROR, 1, MetricUnit.Count);
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
            }
            else
            {
                _logger?.LogInformation($"EventLogSource id {this.Id} skipped duplicated log: {eventRecord.ToXml()}.");
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
            _recordSubject.OnNext(new EventRecordEnvelope(eventRecord, _includeEventData));
            _metrics?.PublishCounter(this.Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment, 
                MetricsConstants.EVENTLOG_SOURCE_EVENTS_READ, 1, MetricUnit.Count);
        }
    }
}
