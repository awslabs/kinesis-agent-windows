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
namespace Amazon.KinesisTap.Windows.Test
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using Amazon.KinesisTap.AWS;
    using Amazon.KinesisTap.Core;
    using Amazon.KinesisTap.Core.Test;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Logging.Abstractions;
    using Xunit;

    /// <summary>
    /// Tests that verify the behavior of the eventlog watcher and bookmarks when BookmarkOnBufferFlush is enabled.
    /// 
    /// Tests in this class that are invoked with the "acknowledge" parameter set to "true" will reproduce the situation
    /// where the sink calls back to the BookmarkManager telling it that the events up to a certain position have been processed.
    /// When it is "false", we are simulating the situation when the sink has NOT acknowledged the sending of events.
    /// This allows us to verify that the callbacks in the sinks are updating the bookmarks as expected.
    /// </summary>
    public class EventLogBookmarkBufferedSinkTest : IDisposable
    {
        private const string LogName = "Application";
        private const string LogSource = nameof(EventLogBookmarkBufferedSinkTest);
        private readonly BookmarkManager _bookmarkManager = new BookmarkManager();

        public EventLogBookmarkBufferedSinkTest()
        {
            if (!EventLog.SourceExists(LogSource))
            {
                EventLog.CreateEventSource(LogSource, LogName);
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void TestInitialPositionBOS(bool acknowledge)
        {
            var records = new ListEventSink();
            var sourceId = "TestEvtInitialPositionTimeStamp";
            var eventId = (int)(DateTime.Now.Ticks % ushort.MaxValue);
            var msg = "A fresh message";

            // Write some events before the source is created
            for (int i = 0; i < 3; i++)
                EventLog.WriteEntry(LogSource, msg, EventLogEntryType.Information, eventId);

            Thread.Sleep(1000);
            var now = DateTime.UtcNow;
            using (var source = CreateSource(sourceId, eventId))
            {
                source.Subscribe(records);
                source.Id = sourceId;
                source.InitialPosition = InitialPositionEnum.BOS;
                source.Start();

                for (int i = 0; i < 5; i++)
                    EventLog.WriteEntry(LogSource, msg, EventLogEntryType.Information, eventId);

                Thread.Sleep(5000);

                if (acknowledge)
                {
                    // Send the acknowledgements as if they had come from sources.
                    var bookmark = Assert.IsType<BookmarkInfo>(_bookmarkManager.GetBookmark(sourceId));
                    _bookmarkManager.SaveBookmark(bookmark.Id, records.Last().Position, null);
                }

                source.Stop();
                Thread.Sleep(1000);

                Assert.True(records[0].Timestamp < now, $"First record should have been written before {now}, but was written at {records[0].Timestamp}");

                var lastRecordTimestamp = records.Last().Timestamp;
                Assert.True(lastRecordTimestamp > now, $"Last record should have been written after {now}, but was written at {records.Last().Timestamp}");

                records.Clear();

                //Write some new logs after the source stop
                var newmsg = "A fresh message after source stop";
                EventLog.WriteEntry(LogSource, newmsg, EventLogEntryType.Information, eventId);
                Thread.Sleep(1000);
                source.Start();
                Thread.Sleep(3000);

                IEnvelope lastRecord;
                if (acknowledge)
                {
                    lastRecord = Assert.Single(records);
                }
                else
                {
                    Assert.Equal(9, records.Count);
                    lastRecord = records.Last();
                }

                Assert.True(lastRecord.Timestamp > lastRecordTimestamp);
                Assert.Matches("after source stop", lastRecord.GetMessage("string"));
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void TestInitialPositionBookMark(bool acknowledge)
        {
            var records = new ListEventSink();
            var sourceId = "TestEvtInitialPositionBookMark";
            var eventId = (int)(DateTime.Now.Ticks % ushort.MaxValue);
            var msg = "A fresh message";

            // Write some events before the source is created
            for (int i = 0; i < 3; i++)
                EventLog.WriteEntry(LogSource, msg, EventLogEntryType.Information, eventId);

            var now = DateTime.UtcNow;
            using (var source = CreateSource(sourceId, eventId))
            {
                source.Subscribe(records);
                source.Id = sourceId;
                source.InitialPosition = InitialPositionEnum.Bookmark;
                source.Start();

                Thread.Sleep(2000);

                // When using Bookmark as Initial position, and there is no bookmark, it should not process old events.
                Assert.Empty(records);

                EventLog.WriteEntry(LogSource, msg, EventLogEntryType.Information, eventId);

                Thread.Sleep(1000);

                if (acknowledge)
                {
                    // Send the acknowledgements as if they had come from sources.
                    var bookmark = Assert.IsType<BookmarkInfo>(_bookmarkManager.GetBookmark(sourceId));
                    _bookmarkManager.SaveBookmark(bookmark.Id, records.Last().Position, null);
                }

                source.Stop();
                Thread.Sleep(1000);

                var lastRecordTimestamp = Assert.Single(records).Timestamp;
                Assert.True(lastRecordTimestamp > now);

                records.Clear();

                //Write some new logs after the source stop
                var newmsg = "A fresh message after source stop";
                EventLog.WriteEntry(LogSource, newmsg, EventLogEntryType.Information, eventId);
                Thread.Sleep(1000);
                source.Start();
                Thread.Sleep(1000);

                // If it's a clean shutdown (i.e. config reload), the bookmark isn't removed from BookmarkManager,
                // so the source should pick up where it left off according to the previous bookmark value.
                if (acknowledge)
                {
                    var lastRecord = Assert.Single(records);
                    Assert.True(lastRecord.Timestamp > lastRecordTimestamp);
                    Assert.Matches("after source stop", lastRecord.GetMessage("string"));
                }
                else
                {
                    // When using Bookmark as Initial position, and there is no bookmark, it should not process old events.
                    // Since we didn't commit the bookmark in this theory, no records should be returned.
                    Assert.Empty(records);
                }
            }
        }

        /// <summary>
        /// Tests that when EventLogSource stops, the connected sink is able to flush the bookmark immediately,
        /// so that buffered events in the sink are not duplicated next time the source starts.
        /// </summary>
        [Fact]
        public void TestBookmarkFlushedAfterSourceStops()
        {
            var localSinkPath = Path.Combine(TestUtility.GetTestHome(), $"{nameof(TestBookmarkFlushedAfterSourceStops)}-{Guid.NewGuid()}");
            var localSinkconfig = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string> { ["FilePath"] = localSinkPath, ["Id"] = "LocalSink" })
                .Build();
            var localSink = new FileSystemEventSink(new PluginContext(localSinkconfig, NullLogger.Instance, null, _bookmarkManager, null, null), 100, 1, 5, 5);

            var sourceId = nameof(TestBookmarkFlushedAfterSourceStops);
            var eventId = (int)(DateTime.Now.Ticks % ushort.MaxValue);
            var msg = $"{nameof(TestBookmarkFlushedAfterSourceStops)} message";
            var bookmarkFilePath = Path.Combine(Utility.GetKinesisTapProgramDataPath(), ConfigConstants.BOOKMARKS, $"{sourceId}.bm");

            using (var source = CreateSource(sourceId, eventId))
            {
                source.Subscribe(localSink);
                source.Id = sourceId;
                source.InitialPosition = InitialPositionEnum.Bookmark;
                source.Start();
                localSink.Start();

                // generate an event
                EventLog.WriteEntry(LogSource, msg, EventLogEntryType.Information, eventId);
                Thread.Sleep(1000);

                // assert sure that the event exists in the sink and the first bookmark is always flushed
                Assert.True(File.Exists(localSinkPath));
                Assert.True(File.Exists(bookmarkFilePath));

                var bookmarkFileWriteTime = File.GetLastWriteTime(bookmarkFilePath);

                // we now create a situation where the sink has records in the buffer while the source stops
                // we do so by acquiring a lock on the local sink file
                var sinkFileStream = File.OpenWrite(localSinkPath);
                // generate an event
                EventLog.WriteEntry(LogSource, msg, EventLogEntryType.Information, eventId);
                Thread.Sleep(1000);

                // stop the source
                source.Stop();
                // now release the file lock
                sinkFileStream.Close();
                Thread.Sleep(3000);

                // assert that the bookmark file is updated.
                Assert.True(File.GetLastWriteTime(bookmarkFilePath) > bookmarkFileWriteTime);
                source.Start();
                Thread.Sleep(1000);

                // assert that the sink contains exactly 2 records, meaning no duplicate event is streamed
                Assert.Equal(2, File.ReadAllLines(localSinkPath).Length);

                File.Delete(localSinkPath);
            }
        }

        private EventLogSource CreateSource(string sourceId, int eventId)
        {
            DeleteExistingBookmarkFile(sourceId);
            _bookmarkManager.RemoveBookmark(sourceId);

            var config = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string> { ["BookmarkOnBufferFlush"] = "true", ["Id"] = sourceId })
                .Build();

            return new EventLogSource(LogName, $"*[System[EventID={eventId}]]", new PluginContext(config, null, null, _bookmarkManager));
        }

        private static void DeleteExistingBookmarkFile(string sourceId)
        {
            var bookmarkFile = Path.Combine(Utility.GetKinesisTapProgramDataPath(), ConfigConstants.BOOKMARKS, $"{sourceId}.bm");
            if (File.Exists(bookmarkFile))
                File.Delete(bookmarkFile);
        }

        public void Dispose()
        {
            if (EventLog.SourceExists(LogSource))
            {
                EventLog.DeleteEventSource(LogSource);
            }
        }
    }
}
