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
using System.Diagnostics.Eventing.Reader;
using System.IO;
using System.Linq;
using System.Reactive.Subjects;
using System.Reflection;
using System.Runtime.Versioning;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

namespace Amazon.KinesisTap.Windows
{
    [SupportedOSPlatform("windows")]
    internal abstract class WindowsEventLogSourceBase<TRecord> : AsyncDependentSource<TRecord>, IBookmarkable, IDisposable
    {
        private const string BookmarkFormatString = "{{\"BookmarkText\":\"<BookmarkList>\r\n<Bookmark Channel='{0}' RecordId='{1}' IsCurrent='true'/>\r\n</BookmarkList>\"}}";
        private const string RecordIdRegex = "RecordId='(\\d+)'";

        private readonly FieldInfo _eventBookmarkPrivateField = GetEventBookmarkField();
        protected readonly string _logName;
        protected readonly string _query;
        protected readonly IBookmarkManager _bookmarkManager;
        protected readonly ISubject<IEnvelope<TRecord>> _recordSubject = new Subject<IEnvelope<TRecord>>();
        protected long _bookmarkRecordPosition = -1;
        protected EventBookmark _eventBookmark;

        protected WindowsEventLogSourceBase(string id, string logName, string query,
            IBookmarkManager bookmarkManager, IPlugInContext context) : base(new EventLogDependency(logName), context)
        {
            Guard.ArgumentNotNull(id, nameof(id));
            Guard.ArgumentNotNull(logName, nameof(logName));
            Id = id;
            _logName = logName;
            _query = query;
            _bookmarkManager = bookmarkManager;
        }

        /// <inheritdoc />
        public override IDisposable Subscribe(IObserver<IEnvelope<TRecord>> observer) => _recordSubject.Subscribe(observer);

        /// <inheritdoc />
        public override async ValueTask StartAsync(CancellationToken stopToken)
        {
            if (InitialPosition != InitialPositionEnum.EOS)
            {
                await _bookmarkManager.RegisterSourceAsync(this, stopToken);
            }
        }

        /// <inheritdoc />
        public override sealed void Start() => throw new InvalidOperationException("Synchronous start/stop not supported");

        /// <inheritdoc />
        public override sealed void Stop() => throw new InvalidOperationException("Synchronous start/stop not supported");

        /// <inheritdoc />
        public string BookmarkKey => Id;

        /// <inheritdoc />
        public ValueTask OnBookmarkCallback(IEnumerable<RecordBookmark> recordBookmarkData)
        {
            var latest = recordBookmarkData.Max(r => r is IntegerPositionRecordBookmark posBookmark ? posBookmark.Position : long.MinValue);
            Utility.InterlockedExchangeIfGreaterThan(ref _bookmarkRecordPosition, latest, latest);
            return ValueTask.CompletedTask;
        }

        /// <inheritdoc />
        public void OnBookmarkLoaded(byte[] bookmarkData)
        {
            if (bookmarkData is null)
            {
                _eventBookmark = null;
                return;
            }

            var json = Encoding.UTF8.GetString(bookmarkData);
            var jsonObject = JObject.Parse(json);
            var bookmarkText = (string)jsonObject["BookmarkText"];

            _eventBookmark = GetBookmarkFromText(bookmarkText, _logName, _query);
            if (_eventBookmark is null)
            {
                _bookmarkRecordPosition = 0;
                return;
            }
            _bookmarkRecordPosition = GetEventIdFromBookmark(_eventBookmark);
        }

        /// <inheritdoc />
        public byte[] SerializeBookmarks()
        {
            var pos = Interlocked.Read(ref _bookmarkRecordPosition);
            if (pos < 0)
            {
                return null;
            }
            var json = string.Format(BookmarkFormatString, _logName, pos);
            return Encoding.UTF8.GetBytes(json);
        }

        protected async ValueTask InitializeBookmarkLocation(CancellationToken cancellationToken)
        {
            try
            {
                switch (InitialPosition)
                {
                    default:
                    case InitialPositionEnum.BOS:
                    case InitialPositionEnum.Timestamp:
                        break;
                    case InitialPositionEnum.Bookmark:
                    case InitialPositionEnum.EOS:
                        if (InitialPosition == InitialPositionEnum.EOS || _eventBookmark == null)
                        {
                            var latestBookmark = await GetLatestBookmark(cancellationToken);
                            _eventBookmark = latestBookmark.Item2;
                            _bookmarkRecordPosition = latestBookmark.Item1;
                        }
                        break;
                }
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested || ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Error initializing bookmark location.");
            }
        }

        /// <summary>
        /// With .NET 5 we can't simply create <see cref="EventBookmark"/> by deserializing the json anymore.
        /// So we need to do some trick to get it.
        /// </summary>
        private EventBookmark GetBookmarkFromText(string bookmarkText, string logName, string query)
        {
            using (var reader = new EventLogReader(new EventLogQuery(logName, PathType.LogName, query)))
            {
                // read a random record
                var record = reader.ReadEvent();
                if (record is null)
                {
                    // this means there's no bookmark possible
                    return null;
                }
                var bookmark = record.Bookmark;
                _eventBookmarkPrivateField.SetValue(bookmark, bookmarkText);
                return bookmark;
            }
        }

        private static FieldInfo GetEventBookmarkField()
        {
            // get the 'bookmark' private field, see https://referencesource.microsoft.com/#system.core/System/Diagnostics/Eventing/Reader/EventBookmark.cs,a86c8942d3fe4a5b,references
            var privateField = typeof(EventBookmark).GetFields(BindingFlags.NonPublic | BindingFlags.Instance).SingleOrDefault();
            if (privateField is null)
            {
                throw new InvalidOperationException("Cannot initiate event bookmark, this might be a runtime error");
            }
            return privateField;
        }

        private long GetEventIdFromBookmark(EventBookmark eventBookmark)
        {
            var bookmarkText = (string)_eventBookmarkPrivateField.GetValue(eventBookmark);

            var match = Regex.Match(bookmarkText, RecordIdRegex);
            if (match.Success && match.Groups.Count == 2 && long.TryParse(match.Groups[1].Value, out var recordId))
            {
                return recordId;
            }
            return 0;
        }

        protected async ValueTask<(long, EventBookmark)> GetLatestBookmark(CancellationToken stopToken)
        {
            using var eventLogReader = await CreateEventLogReader(null, stopToken);
            eventLogReader.Seek(SeekOrigin.End, 0);
            // now, for some odd reason, Seek to End will set the reader BEFORE the last record, so we do a random read here 
            // if DoRead() returns a record, the bookmark is set after that record (which should be the ACTUAL last record)
            // else, the event log is empty, and bookmark can be set to NULL
            using (var last = DoRead(eventLogReader))
            {
                return (last?.RecordId ?? -1, last?.Bookmark);
            }
        }

        /// <summary>
        /// The implementation of EventLogReader throws 'InvalidOperationException' or 'EventLogException' when the end of stream is reached
        /// (https://referencesource.microsoft.com/#system.core/System/Diagnostics/Eventing/Reader/EventLogReader.cs,177)
        /// so we capture that and return null instead.
        /// </summary>
        protected static EventRecord DoRead(EventLogReader reader)
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

        /// <summary>
        /// This method will try to create the <see cref="EventLogReader"/>, however, if the reader can't be created for some reason,
        /// e.g. Service not available or LogName none-existing, it will delay the creation and poll the dependency
        /// </summary>
        protected async ValueTask<EventLogReader> CreateEventLogReader(EventBookmark bookmark, CancellationToken stopToken)
        {
            while (true)
            {
                try
                {
                    var reader = new EventLogReader(new EventLogQuery(_logName, PathType.LogName, _query), _eventBookmark)
                    {
                        BatchSize = 128
                    };

                    return reader;
                }
                catch (EventLogException ele)
                when (ele.Message.Contains("The handle is invalid", StringComparison.OrdinalIgnoreCase) || ele is EventLogNotFoundException)
                {
                    _logger.LogWarning(ele, "Event log '{0}' cannot be read", _logName);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Error reading event log '{_logName}' with query {_query}");
                }

                await EnsureDependencyAvailable(stopToken);
            }
        }

        private bool _disposed;

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _dependency.Dispose();
                }

                _disposed = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
