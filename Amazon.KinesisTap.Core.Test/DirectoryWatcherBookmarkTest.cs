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
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    public class DirectoryWatcherBookmarkTest
    {
        private readonly BookmarkManager _bookmarkManager = new BookmarkManager();
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
        public void TestEOSWithIncludeSubdirectories()
        {
            string sourceId = "TestEOSWithIncludeSubdirectories";
            var subDir1 = "CPU";
            var subDir2 = "Memory";

            var subdirectories = new string[] { subDir1, subDir2 };

            Setup(sourceId, subdirectories);

            WriteLogs("A", 2, subDir1);
            WriteLogs("B", 3, subDir2);

            ListEventSink logRecords = new ListEventSink();
            var config = TestUtility.GetConfig("Sources", "IncludeSubdirectories");
            DirectorySource<IDictionary<string, string>, LogContext> watcher = CreateDirectorySource(sourceId, "log_?.log", logRecords, config);
            watcher.InitialPosition = InitialPositionEnum.EOS;
            watcher.Start();
            Thread.Sleep(2000);
            watcher.Stop();
            Assert.Empty(logRecords);

            watcher.Start();
            WriteLogs("B", 1, subDir1);
            WriteLogs("C", 5, subDir2);
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
        public void TestBOSWithIncludeSubdirectories()
        {
            string sourceId = "TestBOSWithIncludeSubdirectories";
            var subDir1 = "CPU";
            var subDir2 = "Memory";

            var subdirectories = new string[] { subDir1, subDir2 };

            Setup(sourceId, subdirectories);

            WriteLogs("A", 2, subDir1);
            WriteLogs("B", 3, subDir2);

            ListEventSink logRecords = new ListEventSink();
            var config = TestUtility.GetConfig("Sources", "IncludeSubdirectories");
            DirectorySource<IDictionary<string, string>, LogContext> watcher = CreateDirectorySource(sourceId, "log_?.log", logRecords, config);
            watcher.InitialPosition = InitialPositionEnum.BOS;
            watcher.Start();
            Thread.Sleep(2000);
            watcher.Stop();
            Assert.Equal(5, logRecords.Count);
            logRecords.Clear();

            WriteLogs("B", 1, subDir1);
            WriteLogs("C", 5, subDir2);

            watcher.Start();
            WriteLogs("D", 7, subDir1);
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
        public void TestBookmarkWithIncludeSubdirectories()
        {
            string sourceId = "TestBookmarkWithIncludeSubdirectories";
            var subDir1 = "CPU";
            var subDir2 = "Memory";

            var subdirectories = new string[] { subDir1, subDir2 };

            Setup(sourceId, subdirectories);

            WriteLogs("A", 2, subDir1);
            WriteLogs("B", 3, subDir2);

            ListEventSink logRecords = new ListEventSink();
            var config = TestUtility.GetConfig("Sources", "IncludeSubdirectories");
            DirectorySource<IDictionary<string, string>, LogContext> watcher = CreateDirectorySource(sourceId, "log_?.log", logRecords, config);
            watcher.InitialPosition = InitialPositionEnum.Bookmark;
            watcher.Start();
            Thread.Sleep(2000);
            watcher.Stop();
            Assert.Empty(logRecords);

            WriteLogs("A", 1, subDir1);
            WriteLogs("B", 5, subDir2);

            watcher.Start();
            WriteLogs("A", 7, subDir1);
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

        [Fact]
        public void TestTimestampWithIncludeSubdirectories()
        {
            string sourceId = "TestTimestampWithIncludeSubdirectories";
            var subDir1 = "CPU";
            var subDir2 = "Memory";

            var subdirectories = new string[] { subDir1, subDir2 };

            Setup(sourceId, subdirectories);

            WriteLogs("A", 1, subDir1);
            Thread.Sleep(200);

            DateTime timestamp = DateTime.Now;
            Thread.Sleep(1000);

            WriteLogs("A", 2, subDir1);
            WriteLogs("B", 3, subDir2);

            ListEventSink logRecords = new ListEventSink();
            var config = TestUtility.GetConfig("Sources", "IncludeSubdirectories");
            DirectorySource<IDictionary<string, string>, LogContext> watcher = CreateDirectorySource(sourceId, "log_?.log", logRecords, config);
            watcher.InitialPosition = InitialPositionEnum.Timestamp;
            watcher.InitialPositionTimestamp = timestamp;
            watcher.Start();
            Thread.Sleep(2000);
            watcher.Stop();
            Assert.Equal(5, logRecords.Count);
            logRecords.Clear();

            WriteLogs("B", 1, subDir1);
            WriteLogs("C", 5, subDir2);

            watcher.Start();
            WriteLogs("D", 7, subDir1);
            Thread.Sleep(2000);
            watcher.Stop();

            Assert.Equal(13, logRecords.Count);
        }

        [Fact]
        public void TestBookmarkWithMultipleFilter()
        {
            string sourceId = "TestDirectorySourceBookmarkWithMultipleFilter";
            Setup(sourceId);

            WriteLogs("A", 2);
            WriteLogs("B", 3);

            ListEventSink logRecords = new ListEventSink();
            DirectorySource<IDictionary<string, string>, LogContext> watcher = CreateDirectorySource(sourceId, "log_A.log|log_B.log", logRecords);
            watcher.InitialPosition = InitialPositionEnum.Bookmark;
            watcher.Start();
            Thread.Sleep(2000);
            watcher.Stop();
            Assert.Empty(logRecords);

            WriteLogs("A", 1);
            WriteLogs("B", 5);

            watcher.Start();
            WriteLogs("A", 7);
            Thread.Sleep(2000);
            watcher.Stop();

            Assert.Equal(13, logRecords.Count);
        }

        [Fact]
        public void TestBookmarkWithMultipleFilterWithIncludeSubdirectories()
        {
            string sourceId = "TestBookmarkWithMultipleFilterWithIncludeSubdirectories";
            var subDir1 = "CPU";
            var subDir2 = "Memory";

            var subdirectories = new string[] { subDir1, subDir2 };

            Setup(sourceId, subdirectories);

            WriteLogs("A", 2, subDir1);
            WriteLogs("B", 3, subDir2);

            ListEventSink logRecords = new ListEventSink();
            var config = TestUtility.GetConfig("Sources", "IncludeSubdirectories");
            DirectorySource<IDictionary<string, string>, LogContext> watcher = CreateDirectorySource(sourceId, "log_A.log|log_B.log", logRecords, config);
            watcher.InitialPosition = InitialPositionEnum.Bookmark;
            watcher.Start();
            Thread.Sleep(2000);
            watcher.Stop();
            Assert.Empty(logRecords);

            WriteLogs("A", 1, subDir1);
            WriteLogs("B", 5, subDir2);

            watcher.Start();
            WriteLogs("A", 7, subDir1);
            Thread.Sleep(2000);
            watcher.Stop();

            Assert.Equal(13, logRecords.Count);
        }

        [Fact]
        public void TestBookmarkWithExcludedExtension()
        {
            string sourceId = "TestDirectorySourceBookmarkWithExcludedExtension";
            Setup(sourceId);

            WriteLogs("A", 2);
            WriteLogs("B", "zip", 3);

            ListEventSink logRecords = new ListEventSink();
            DirectorySource<IDictionary<string, string>, LogContext> watcher = CreateDirectorySource(sourceId, "*.*", logRecords);
            watcher.InitialPosition = InitialPositionEnum.Bookmark;
            watcher.Start();
            Thread.Sleep(2000);
            watcher.Stop();
            Assert.Empty(logRecords);

            WriteLogs("A", 1);
            WriteLogs("B", "zip", 5);

            watcher.Start();
            WriteLogs("A", 7);
            Thread.Sleep(2000);
            watcher.Stop();

            Assert.Equal(8, logRecords.Count);
        }

        [Fact]
        public void TestBookmarkWithExcludedExtensionWithIncludeSubdirectories()
        {
            string sourceId = "TestDirectorySourceBookmarkWithExcludedExtension";
            var subDir1 = "CPU";
            var subDir2 = "Memory";

            var subdirectories = new string[] { subDir1, subDir2 };

            Setup(sourceId, subdirectories);

            WriteLogs("A", 2, subDir1);
            WriteLogs("B", 3, subDir2);

            ListEventSink logRecords = new ListEventSink();
            var config = TestUtility.GetConfig("Sources", "IncludeSubdirectories");
            DirectorySource<IDictionary<string, string>, LogContext> watcher = CreateDirectorySource(sourceId, "*.*", logRecords, config);
            watcher.InitialPosition = InitialPositionEnum.Bookmark;
            watcher.Start();
            Thread.Sleep(2000);
            watcher.Stop();
            Assert.Empty(logRecords);

            WriteLogs("A", 1, subDir1);
            WriteLogs("B", "zip", 5, subDir2);

            watcher.Start();
            WriteLogs("A", 7, subDir1);
            Thread.Sleep(2000);
            watcher.Stop();

            Assert.Equal(8, logRecords.Count);
        }

        private DirectorySource<IDictionary<string, string>, LogContext> CreateDirectorySource(string sourceId, ListEventSink logRecords, IConfiguration config = null)
        {
            return CreateDirectorySource(sourceId, "log_?.log", logRecords);
        }

        private DirectorySource<IDictionary<string, string>, LogContext> CreateDirectorySource(string sourceId, string filter, ListEventSink logRecords, IConfiguration config = null)
        {
            DirectorySource<IDictionary<string, string>, LogContext> watcher = new DirectorySource<IDictionary<string, string>, LogContext>
                (BookmarkDirectory,
                filter,
                1000,
                new PluginContext(config, NullLogger.Instance, null, _bookmarkManager),
                new TimeStampRecordParser(RECORD_TIME_STAMP_FORMAT, null, DateTimeKind.Utc));
            watcher.Id = sourceId;
            watcher.Subscribe(logRecords);
            return watcher;
        }

        private void Setup(string sourceId, string[] directories = null)
        {
            CreateDirectory(directories);
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

        private void CreateDirectory(string[] directories = null)
        {
            if (Directory.Exists(BookmarkDirectory))
            {
                Directory.Delete(BookmarkDirectory, true);
            }
            if (directories == null)
            {
                Directory.CreateDirectory(BookmarkDirectory);
            }
            else
            {
                foreach (var dir in directories)
                {
                    Directory.CreateDirectory(Path.Combine(BookmarkDirectory, dir));
                }
            }
        }

        private void WriteLogs(string suffix, int records, string subdirectory = null)
        {
            WriteLogs(suffix, "log", records, subdirectory);
        }

        private void WriteLogs(string suffix, string extension, int records, string subdirectory = null)
        {
            string filepath = Path.Combine(BookmarkDirectory, subdirectory ?? string.Empty, $"log_{suffix}.{extension}");
            using (var sw = File.AppendText(filepath))
            {
                DateTime timestamp = DateTime.UtcNow;

                for (int i = 0; i < records; i++)
                {
                    sw.WriteLine($"{timestamp.AddMilliseconds(i).ToString(RECORD_TIME_STAMP_FORMAT, CultureInfo.InvariantCulture)} {suffix} {i}");
                }

                sw.Flush();
            }
        }
    }
}
