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
 using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    public class DirectoryWatcherBookmarkTest
    {
        private static readonly string BookmarkDirectory = Path.Combine(TestUtility.GetTestHome(), "bookmark");
        private const string RECORD_TIME_STAMP_FORMAT = "yyyy/MM/dd HH:mm:ss.fff";

        [Fact]
        public void TestEOS()
        {
            string sourceId = "TestDirectorySourceEOS";
            Setup(sourceId);

            WriteLogs("A", 2);
            WriteLogs("B", 3);

            ListEventSink logRecords = new ListEventSink();
            DirectorySource<IDictionary<string, string>, LogContext> watcher = CreateDirectorySource(sourceId, logRecords);
            watcher.InitialPosition = InitialPositionEnum.EOS;
            watcher.Start();
            Thread.Sleep(2000);
            watcher.Stop();
            Assert.Empty(logRecords);

            watcher.Start();
            WriteLogs("B", 1);
            WriteLogs("C", 5);
            Thread.Sleep(2000);
            watcher.Stop();

            Assert.Equal(6, logRecords.Count);
        }

        [Fact]
        public void TestBOS()
        {
            string sourceId = "TestDirectorySourceBOSBookmark";
            Setup(sourceId);

            WriteLogs("A", 2);
            WriteLogs("B", 3);

            ListEventSink logRecords = new ListEventSink();
            DirectorySource<IDictionary<string, string>, LogContext> watcher = CreateDirectorySource(sourceId, logRecords);
            watcher.InitialPosition = InitialPositionEnum.BOS;
            watcher.Start();
            Thread.Sleep(2000);
            watcher.Stop();
            Assert.Equal(5, logRecords.Count);
            logRecords.Clear();

            WriteLogs("B", 1);
            WriteLogs("C", 5);

            watcher.Start();
            WriteLogs("D", 7);
            Thread.Sleep(2000);
            watcher.Stop();

            Assert.Equal(13, logRecords.Count);
        }

        [Fact]
        public void TestBookmark()
        {
            string sourceId = "TestDirectorySourceBookmark";
            Setup(sourceId);

            WriteLogs("A", 2);
            WriteLogs("B", 3);

            ListEventSink logRecords = new ListEventSink();
            DirectorySource<IDictionary<string, string>, LogContext> watcher = CreateDirectorySource(sourceId, logRecords);
            watcher.InitialPosition = InitialPositionEnum.Bookmark;
            watcher.Start();
            Thread.Sleep(2000);
            watcher.Stop();
            Assert.Empty(logRecords);

            WriteLogs("B", 1);
            WriteLogs("C", 5);

            watcher.Start();
            WriteLogs("D", 7);
            Thread.Sleep(2000);
            watcher.Stop();

            Assert.Equal(13, logRecords.Count);
        }

        [Fact]
        public void TestTimestamp()
        {
            string sourceId = "TestDirectorySourceTimestamp";
            Setup(sourceId);

            WriteLogs("A", 1);
            Thread.Sleep(200);

            DateTime timestamp = DateTime.Now;
            Thread.Sleep(1000);

            WriteLogs("A", 2);
            WriteLogs("B", 3);

            ListEventSink logRecords = new ListEventSink();
            DirectorySource<IDictionary<string, string>, LogContext> watcher = CreateDirectorySource(sourceId, logRecords);
            watcher.InitialPosition = InitialPositionEnum.Timestamp;
            watcher.InitialPositionTimestamp = timestamp;
            watcher.Start();
            Thread.Sleep(2000);
            watcher.Stop();
            Assert.Equal(5, logRecords.Count);
            logRecords.Clear();

            WriteLogs("B", 1);
            WriteLogs("C", 5);

            watcher.Start();
            WriteLogs("D", 7);
            Thread.Sleep(2000);
            watcher.Stop();

            Assert.Equal(13, logRecords.Count);
        }

        private DirectorySource<IDictionary<string, string>, LogContext> CreateDirectorySource(string sourceId, ListEventSink logRecords)
        {
            DirectorySource<IDictionary<string, string>, LogContext> watcher = new DirectorySource<IDictionary<string, string>, LogContext>
                (BookmarkDirectory,
                "log_?.log",
                1000,
                new PluginContext(null, NullLogger.Instance, null),
                new TimeStampRecordParser(RECORD_TIME_STAMP_FORMAT, null, DateTimeKind.Utc),
                DirectorySourceFactory.CreateLogSourceInfo);
            watcher.Id = sourceId;
            watcher.Subscribe(logRecords);
            return watcher;
        }

        private void Setup(string sourceId)
        {
            CreateDirectory();
            DeleteBookmark(sourceId);
        }

        private static void DeleteBookmark(string sourceId)
        {
            string bookmarkPath = Path.Combine(Utility.GetKinesisTapProgramDataPath(), ConfigConstants.BOOKMARKS, $"{sourceId}.bm");
            if (File.Exists(bookmarkPath))
            {
                File.Delete(bookmarkPath);
            }
        }

        private void CreateDirectory()
        {
            if (Directory.Exists(BookmarkDirectory))
            {
                Directory.Delete(BookmarkDirectory, true);
            }
            Directory.CreateDirectory(BookmarkDirectory);
        }

        private void WriteLogs(string suffix, int records)
        {
            string filepath = Path.Combine(BookmarkDirectory, $"log_{suffix}.log");
            using (var sw = File.AppendText(filepath))
            {
                DateTime timestamp = DateTime.Now;
                   
                for (int i = 0; i < records; i++)
                {
                    sw.WriteLine($"{timestamp.AddMilliseconds(i).ToString(RECORD_TIME_STAMP_FORMAT)} {suffix} {i}");
                }
            }
        }
    }
}
