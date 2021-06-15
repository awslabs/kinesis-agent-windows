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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Test;
using Amazon.KinesisTap.Test.Common;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Amazon.KinesisTap.Windows.Test
{
    /// <summary>
    /// Tests that verify the behavior of the eventlog watcher and bookmarks when BookmarkOnBufferFlush is enabled.
    /// 
    /// Tests in this class that are invoked with the "acknowledge" parameter set to "true" will reproduce the situation
    /// where the sink calls back to the BookmarkManager telling it that the events up to a certain position have been processed.
    /// When it is "false", we are simulating the situation when the sink has NOT acknowledged the sending of events.
    /// This allows us to verify that the callbacks in the sinks are updating the bookmarks as expected.
    /// </summary>
    [Collection(nameof(EventLogBookmarkBufferedSinkTest))]
    public class EventLogBookmarkBufferedSinkTest : IDisposable
    {
        private const string LogName = "Application";
        private readonly string _bookmarkDir = Path.Combine(TestUtility.GetTestHome(), Guid.NewGuid().ToString());
        private readonly string _logSource = $"{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-{Guid.NewGuid()}";

        public EventLogBookmarkBufferedSinkTest()
        {
            if (!EventLog.SourceExists(_logSource))
            {
                EventLog.CreateEventSource(_logSource, LogName);
            }
        }

        public void Dispose()
        {
            if (EventLog.SourceExists(_logSource))
            {
                EventLog.DeleteEventSource(_logSource);
            }
            if (Directory.Exists(_bookmarkDir))
            {
                Directory.Delete(_bookmarkDir, true);
            }
        }

        [WindowsOnlyTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task InitialPositionBOS(bool polling)
        {
            var sink = new ListEventSink();
            var msg = "A fresh message";

            // Write some events before the source is created
            for (var i = 0; i < 3; i++)
            {
                EventLog.WriteEntry(_logSource, msg, EventLogEntryType.Information);
            }

            await Task.Delay(100);
            var bm = new FileBookmarkManager(_bookmarkDir, NullLogger.Instance);

            var cts = new CancellationTokenSource();
            var source = CreateSource(polling, LogName, bm, InitialPositionEnum.BOS);

            var subscription = source.Subscribe(sink);

            await bm.StartAsync(cts.Token);
            await source.StartAsync(cts.Token);

            await Task.Delay(1000);
            Assert.Equal(3, sink.Count);

            cts.Cancel();
            await source.StopAsync(default);
            await bm.StopAsync(default);
            subscription.Dispose();

            for (var i = 0; i < 5; i++)
            {
                EventLog.WriteEntry(_logSource, msg, EventLogEntryType.Information);
            }

            sink.Clear();
            bm = new FileBookmarkManager(_bookmarkDir, NullLogger.Instance);
            cts = new CancellationTokenSource();
            source = CreateSource(polling, LogName, bm, InitialPositionEnum.BOS);
            source.Subscribe(sink);

            await bm.StartAsync(cts.Token);
            await source.StartAsync(cts.Token);

            await Task.Delay(1000);
            Assert.Equal(5, sink.Count);

            cts.Cancel();
            await source.StopAsync(default);
            await bm.StopAsync(default);
        }

        [WindowsOnlyTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task InitialPositionBookmark(bool polling)
        {
            var sink = new ListEventSink();
            var msg = "A fresh message";

            // Write some events before the source is created
            for (var i = 0; i < 3; i++)
            {
                EventLog.WriteEntry(_logSource, msg, EventLogEntryType.Information);
            }
            await Task.Delay(100);

            var bm = new FileBookmarkManager(_bookmarkDir, NullLogger.Instance);

            var cts = new CancellationTokenSource();
            var source = CreateSource(polling, LogName, bm, InitialPositionEnum.Bookmark);

            var subscription = source.Subscribe(sink);

            await bm.StartAsync(cts.Token);
            await source.StartAsync(cts.Token);

            await Task.Delay(1000);
            Assert.Empty(sink);

            for (var i = 0; i < 5; i++)
            {
                EventLog.WriteEntry(_logSource, msg, EventLogEntryType.Information);
            }

            await Task.Delay(1000);
            Assert.Equal(5, sink.Count);

            cts.Cancel();
            await source.StopAsync(default);
            await bm.StopAsync(default);
            subscription.Dispose();

            for (var i = 0; i < 7; i++)
            {
                EventLog.WriteEntry(_logSource, msg, EventLogEntryType.Information);
            }

            bm = new FileBookmarkManager(_bookmarkDir, NullLogger.Instance);
            cts = new CancellationTokenSource();
            source = CreateSource(polling, LogName, bm, InitialPositionEnum.Bookmark);

            source.Subscribe(sink);

            await bm.StartAsync(cts.Token);
            await source.StartAsync(cts.Token);

            await Task.Delay(1000);
            Assert.Equal(12, sink.Count);

            cts.Cancel();
            await source.StopAsync(default);
            await bm.StopAsync(default);
        }

        [WindowsOnlyTheory]
        [InlineData(true, InitialPositionEnum.Bookmark)]
        [InlineData(false, InitialPositionEnum.Bookmark)]
        [InlineData(true, InitialPositionEnum.BOS)]
        [InlineData(false, InitialPositionEnum.BOS)]
        public async Task BookmarkOnBufferFlush(bool polling, InitialPositionEnum initialPosition)
        {
            const string msg = "BookmarkOnBufferFlush";
            var bm = new FileBookmarkManager(_bookmarkDir, NullLogger.Instance);
            var sink = new ThrottledListEventSink(bm);

            // Write 3 events before the source is created
            for (var i = 0; i < 3; i++)
            {
                EventLog.WriteEntry(_logSource, msg, EventLogEntryType.Information);
            }
            await Task.Delay(100);

            var cts = new CancellationTokenSource();
            var source = CreateSource(polling, LogName, bm, initialPosition, true);

            var subscription = source.Subscribe(sink);
            await bm.StartAsync(cts.Token);
            await source.StartAsync(cts.Token);

            // write 5 events
            for (var i = 0; i < 5; i++)
            {
                EventLog.WriteEntry(_logSource, msg, EventLogEntryType.Information);
            }

            await Task.Delay(1000);
            Assert.Empty(sink);

            // stop the pipeline
            cts.Cancel();
            await source.StopAsync(default);
            await bm.StopAsync(default);
            subscription.Dispose();

            // re-start the pipeline
            cts = new CancellationTokenSource();
            bm = new FileBookmarkManager(_bookmarkDir, NullLogger.Instance);
            sink = new ThrottledListEventSink(bm);
            source = CreateSource(polling, LogName, bm, initialPosition, true);
            subscription = source.Subscribe(sink);
            await bm.StartAsync(cts.Token);
            await source.StartAsync(cts.Token);

            await Task.Delay(1000);
            await sink.AllowEvents();
            // the sink now should collect all the existing items since position should not have been bookmarked before
            Assert.Equal(initialPosition == InitialPositionEnum.Bookmark ? 5 : 8, sink.Count);

            // stop the pipeline
            cts.Cancel();
            await source.StopAsync(default);
            await bm.StopAsync(default);
            subscription.Dispose();

            // re-start the pipeline one more time
            cts = new CancellationTokenSource();
            bm = new FileBookmarkManager(_bookmarkDir, NullLogger.Instance);
            sink = new ThrottledListEventSink(bm);
            source = CreateSource(polling, LogName, bm, initialPosition, true);
            subscription = source.Subscribe(sink);
            await bm.StartAsync(cts.Token);
            await source.StartAsync(cts.Token);

            // write 7 events
            for (var i = 0; i < 7; i++)
            {
                EventLog.WriteEntry(_logSource, msg, EventLogEntryType.Information);
            }
            await Task.Delay(1000);
            await sink.AllowEvents();

            // last position should be bookmarked, so only records from this session is collected
            Assert.Equal(7, sink.Count);

            // stop the pipeline
            cts.Cancel();
            await source.StopAsync(default);
            await bm.StopAsync(default);
            subscription.Dispose();
        }

        private IEventSource CreateSource(bool polling, string logName,
            IBookmarkManager bookmarkManager,
            InitialPositionEnum initialPosition = InitialPositionEnum.Bookmark,
            bool bookmarkOnFlush = false,
            DateTime initialPositionTimestamp = default)
        {
            IEventSource source;
            if (polling)
            {
                source = CreatePollingSource(logName, bookmarkManager, initialPosition, bookmarkOnFlush, initialPositionTimestamp);
            }
            else
            {
                source = CreateNotificationSource(logName, bookmarkManager, initialPosition, bookmarkOnFlush, initialPositionTimestamp);
            }

            return source;
        }

        private WindowsEventPollingSource CreatePollingSource(string logName,
            IBookmarkManager bookmarkManager,
            InitialPositionEnum initialPosition,
            bool bookmarkOnFlush,
            DateTime initialPositionTimestamp)
        {
            var query = $"*[System/Provider/@Name='{_logSource}']";
            if (initialPositionTimestamp.Kind == DateTimeKind.Unspecified)
            {
                initialPositionTimestamp = DateTime.SpecifyKind(initialPositionTimestamp, DateTimeKind.Utc);
            }
            return new WindowsEventPollingSource(nameof(EventLogBookmarkBufferedSinkTest), logName, query, bookmarkManager, new WindowsEventLogPollingSourceOptions
            {
                MaxReaderDelayMs = 500,
                InitialPosition = initialPosition,
                InitialPositionTimestamp = initialPositionTimestamp.ToUniversalTime(),
                BookmarkOnBufferFlush = bookmarkOnFlush
            }, new PluginContext(null, NullLogger.Instance, null));
        }

        private EventLogSource CreateNotificationSource(string logName,
            IBookmarkManager bookmarkManager,
            InitialPositionEnum initialPosition,
            bool bookmarkOnFlush,
            DateTime initialPositionTimestamp)
        {
            var query = $"*[System/Provider/@Name='{_logSource}']";
            if (initialPositionTimestamp.Kind == DateTimeKind.Unspecified)
            {
                initialPositionTimestamp = DateTime.SpecifyKind(initialPositionTimestamp, DateTimeKind.Utc);
            }

            var source = new EventLogSource(nameof(EventLogBookmarkBufferedSinkTest), LogName, query, bookmarkManager,
                new WindowsEventLogSourceOptions
                {
                    InitialPosition = initialPosition,
                    BookmarkOnBufferFlush = bookmarkOnFlush,
                    InitialPositionTimestamp = initialPositionTimestamp.ToUniversalTime()
                },
                new PluginContext(null, NullLogger.Instance, null));
            return source;
        }
    }
}
