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
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Test;
using Amazon.KinesisTap.Test.Common;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Xunit.Abstractions;

namespace Amazon.KinesisTap.Filesystem.Test
{
    [Collection(nameof(AsyncDirectorySourceBookmarkTest))]
    public class AsyncDirectorySourceBookmarkTest : AsyncDirectorySourceTestBase
    {
        private readonly string _bookmarkDir = Path.Combine(TestUtility.GetTestHome(), Guid.NewGuid().ToString());
        private readonly CancellationTokenSource _cts = new();

        public AsyncDirectorySourceBookmarkTest(ITestOutputHelper output) : base(output)
        {
        }

        private bool _disposed;
        protected override void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                if (Directory.Exists(_bookmarkDir))
                {
                    Directory.Delete(_bookmarkDir, true);
                }
                _cts.Dispose();
            }

            _disposed = true;
            base.Dispose(disposing);
        }

        [Theory]
        [InlineData(100, 0)]
        [InlineData(100, 200)]
        [InlineData(0, 100)]
        public async Task InitialPositionBOS(int initialRecords, int runningRecords)
        {
            var rnd = new Random();
            var sink = new ListEventSink();
            for (var i = 0; i < initialRecords; i++)
            {
                WriteLine($"file_{rnd.Next(0, 10)}.log", "test");
            }

            var bookmarkManager = await StartBookmarkManager();

            using (var source = CreateSource("*.log", sink, InitialPositionEnum.BOS, bookmarkManager))
            {
                await source.StartAsync(_cts.Token);
                await Task.Delay(500);
                Assert.Equal(initialRecords, sink.Count);

                for (var i = 0; i < runningRecords; i++)
                {
                    WriteLine($"file_{rnd.Next(0, 10)}.log", "test");
                }

                await Task.Delay(500);
                Assert.Equal(initialRecords + runningRecords, sink.Count);

                _cts.Cancel();
                await bookmarkManager.StopAsync(default);
                await source.StopAsync(default);
            }
        }

        [Theory]
        [InlineData(100, 0)]
        [InlineData(100, 200)]
        [InlineData(50, 100)]
        public async Task InitialPositionBOS_SourceRestart(int records1, int records2)
        {
            var sink = new ListEventSink();

            var bookmarkManager = await StartBookmarkManager();

            using (var source = CreateSource("*.log", sink, InitialPositionEnum.BOS, bookmarkManager))
            {
                await source.StartAsync(_cts.Token);

                // write some logs to file_A
                WriteLine($"file_A.log", "test", records1);
                await Task.Delay(500);
                // assert the records are sent
                Assert.Equal(records1, sink.Count);

                _cts.Cancel();
                await source.StopAsync(default);
                await bookmarkManager.StopAsync(default);
            }

            // write some logs to file_A
            WriteLine($"file_A.log", "test", records2);

            var cts2 = new CancellationTokenSource();
            bookmarkManager = await StartBookmarkManager(cts2.Token);

            using (var source = CreateSource("*.log", sink, InitialPositionEnum.BOS, bookmarkManager))
            {
                await source.StartAsync(cts2.Token);
                await Task.Delay(500);

                // assert the records are all received
                Assert.Equal(records1 + records2, sink.Count);

                cts2.Cancel();
                await bookmarkManager.StopAsync(default);
                await source.StopAsync(default);
            }
        }

        [Theory]
        [InlineData("zip")]
        [InlineData("gz")]
        [InlineData("tar")]
        [InlineData("bz2")]
        public async Task InitialPositionBOSExcludedFiles(string extension)
        {
            var sink = new ListEventSink();
            WriteLine($"file{extension}", "test");

            var bookmarkManager = await StartBookmarkManager();
            using (var source = CreateSource("*.log", sink, InitialPositionEnum.BOS, bookmarkManager))
            {
                await source.StartAsync(_cts.Token);
                await Task.Delay(500);
                Assert.Empty(sink);

                _cts.Cancel();
                await bookmarkManager.StopAsync(default);
                await source.StopAsync(default);
            }
        }

        [Theory]
        [InlineData(100, 0)]
        [InlineData(100, 200)]
        [InlineData(0, 100)]
        public async Task InitialPositionEOS(int initialRecords, int runningRecords)
        {
            var sink = new ListEventSink();
            for (var i = 0; i < initialRecords; i++)
            {
                WriteLine($"file.log", "test");
            }

            var bookmarkManager = await StartBookmarkManager();
            using (var source = CreateSource("*.log", sink, InitialPositionEnum.EOS, bookmarkManager))
            {
                await source.StartAsync(_cts.Token);
                await Task.Delay(500);
                Assert.Empty(sink);

                for (var i = 0; i < runningRecords; i++)
                {
                    WriteLine($"file.log", "test");
                }

                await Task.Delay(500);
                Assert.Equal(runningRecords, sink.Count);
                for (var i = 0; i < runningRecords; i++)
                {
                    var record = sink[i] as LogEnvelope<string>;
                    Assert.Equal(i + initialRecords + 1, record.LineNumber);
                }
                _cts.Cancel();
                await bookmarkManager.StopAsync(default);
                await source.StopAsync(default);
            }
        }

        [Theory]
        [InlineData(100, 0)]
        [InlineData(100, 200)]
        [InlineData(100, 50)]
        public async Task InitialPositionBookmark(int records1, int records2)
        {
            var sink = new ListEventSink();
            var bookmarkManager = await StartBookmarkManager();
            using (var source = CreateSource("*.log", sink, InitialPositionEnum.Bookmark, bookmarkManager))
            {
                await source.StartAsync(_cts.Token);

                // write some logs to file_A
                WriteLine($"file_A.log", "test", records1);
                await Task.Delay(500);
                // assert the records are sent
                Assert.Equal(records1, sink.Count);
                _cts.Cancel();
                await bookmarkManager.StopAsync(default);
                await source.StopAsync(default);
            }

            // write some logs to file_A
            WriteLine($"file_A.log", "test", records2);

            var restartCts = new CancellationTokenSource();
            bookmarkManager = await StartBookmarkManager(restartCts.Token);
            using (var source = CreateSource("*.log", sink, InitialPositionEnum.Bookmark, bookmarkManager))
            {
                await source.StartAsync(restartCts.Token);
                await Task.Delay(500);

                // assert the records are all received
                Assert.Equal(records1 + records2, sink.Count);
                restartCts.Cancel();
                await bookmarkManager.StopAsync(default);
                await source.StopAsync(default);
            }
        }

        [Theory]
        [InlineData(50)]
        public async Task InitialPositionBookmark_RecordsAddedWhileNotRunning(int records)
        {
            // write some logs to file_A
            WriteLine($"file_A.log", "test");

            var sink = new ListEventSink();
            var bookmarkManager = await StartBookmarkManager();
            using (var source = CreateSource("*.log", sink, InitialPositionEnum.Bookmark, bookmarkManager))
            {
                await source.StartAsync(_cts.Token);
                // delay
                await Task.Delay(1000);
                // assert the records are sent
                Assert.Empty(sink);
                _cts.Cancel();
                await bookmarkManager.StopAsync(default);
                await source.StopAsync(default);
            }

            // write some more logs to file_A while source is not running
            WriteLine($"file_A.log", "test", records);

            // restart source and make sure that the previous records are captured
            var restartCts = new CancellationTokenSource();
            bookmarkManager = await StartBookmarkManager(restartCts.Token);
            using (var source = CreateSource("*.log", sink, InitialPositionEnum.Bookmark, bookmarkManager))
            {
                await source.StartAsync(restartCts.Token);
                await Task.Delay(500);

                // assert the records are all received
                Assert.Equal(records, sink.Count);
                restartCts.Cancel();
                await bookmarkManager.StopAsync(default);
                await source.StopAsync(default);
            }
        }

        [Theory]
        [InlineData(100, 0)]
        [InlineData(100, 200)]
        [InlineData(100, 50)]
        public async Task InitialPositionBookmark_DeletedLogFile(int records1, int records2)
        {
            var sink = new ListEventSink();
            const string fileName = "file_A.log";

            var bookmarkManager = await StartBookmarkManager();
            using (var source = CreateSource("*.log", sink, InitialPositionEnum.Bookmark, bookmarkManager))
            {
                await source.StartAsync(_cts.Token);

                // write some logs
                WriteLine(fileName, "test", records1);
                await Task.Delay(500);
                // assert the records are sent
                Assert.Equal(records1, sink.Count);
                File.Delete(Path.Combine(_testDir, fileName));
                await Task.Delay(100);

                _cts.Cancel();
                await bookmarkManager.StopAsync(default);
                await source.StopAsync(default);
            }

            sink.Clear();

            var restartCts = new CancellationTokenSource();
            bookmarkManager = await StartBookmarkManager(restartCts.Token);
            using (var source = CreateSource("*.log", sink, InitialPositionEnum.Bookmark, bookmarkManager))
            {
                await source.StartAsync(restartCts.Token);
                // write some logs to file_A
                WriteLine(fileName, "test", records2);

                await Task.Delay(500);

                // assert the records are all received
                Assert.Equal(records2, sink.Count);

                restartCts.Cancel();
                await source.StopAsync(default);
                await source.StopAsync(default);
            }
        }

        [Theory]
        [InlineData(10, 0)]
        [InlineData(10, 20)]
        [InlineData(10, 5)]
        public async Task InitialPositionTimestamp(int records1, int records2)
        {
            var sink = new ListEventSink();
            const string fileName = "file_A.log";

            WriteLine(fileName, "datetime,message");

            var messages = new List<string>();

            for (var i = 0; i < records1; i++)
            {
                messages.Add($"{DateTime.Now:MM/dd/yyyy HH:mm:ss.fff},before");
                await Task.Delay(10);
            }

            var checkPoint = DateTime.Now;
            await Task.Delay(10);

            for (var i = 0; i < records2; i++)
            {
                messages.Add($"{DateTime.Now:MM/dd/yyyy HH:mm:ss.fff},after");
                await Task.Delay(10);
            }

            foreach (var m in messages)
            {
                WriteLine(fileName, m);
            }

            var bookmarkManager = await StartBookmarkManager();

            var ctx = new PluginContext(null, NullLogger.Instance, null, bookmarkManager, null, null);
            using var source = new AsyncDirectorySource<KeyValueLogRecord, DelimitedTextLogContext>(_sourceId, _testDir,
                   new GenericDelimitedLogParser(NullLogger.Instance, ",", new GenericDelimitedLogParserOptions
                   {
                       HeadersPattern = "^datetime",
                       TimestampField = "datetime"
                   }), bookmarkManager,
                   new DirectorySourceOptions
                   {
                       NameFilters = new string[] { "*.log" },
                       QueryPeriodMs = 100,
                       InitialPosition = InitialPositionEnum.Timestamp,
                       InitialPositionTimestamp = checkPoint,
                   }, ctx);
            source.Subscribe(sink);

            await source.StartAsync(_cts.Token);

            await Task.Delay(500);

            Assert.Equal(records2, sink.Count);

            _cts.Cancel();
            await source.StopAsync(default);
            await bookmarkManager.StopAsync(default);
        }

        [Theory]
        [InlineData(10, 0)]
        [InlineData(10, 20)]
        [InlineData(10, 5)]
        public async Task InitialPositionBookmark_BookmarkOnBufferFlush(int records1, int records2)
        {
            // test with 2 files. Why 2? because our BookmarkManager callback captures the file path,
            // so we need to make sure it captures the path correctly
            const string fileA = "file_A.log";
            const string fileB = "file_B.log";

            var bookmarkPath = Path.Combine(_bookmarkDir, $"{_sourceId}.bm");
            var bm = await StartBookmarkManager();
            var sink = new ThrottledListEventSink(bm);
            using (var source = CreateSource("*.log", sink, InitialPositionEnum.Bookmark, bm, true))
            {
                await source.StartAsync(_cts.Token);
                WriteLine(fileA, "test1", records1);
                WriteLine(fileB, "test1", records1);
                await Task.Delay(500);

                Assert.Empty(sink);

                _cts.Cancel();
                await bm.StopAsync(default);
                await source.StopAsync(default);
            }

            // the bookmark file should exist
            Assert.True(File.Exists(bookmarkPath));

            var restartCts = new CancellationTokenSource();
            bm = await StartBookmarkManager(restartCts.Token);
            sink = new ThrottledListEventSink(bm);
            using (var source = CreateSource("*.log", sink, InitialPositionEnum.Bookmark, bm, true))
            {
                await source.StartAsync(restartCts.Token);
                // wait for source to collect the logs
                await Task.Delay(500);
                await sink.AllowEvents();

                // now the sink should contain all the records written from previous time
                // if 'bookmarkOnFlush' doesn't work, the bookmark is already saved when the source sends the data to the sink
                // the last time, so no record is written this time.
                Assert.Equal(records1 * 2, sink.Count);
                Assert.True(sink.All(e => (e as IEnvelope<string>).Data == "test1"));

                WriteLine(fileA, "test2", records2);
                WriteLine(fileB, "test2", records2);
                await Task.Delay(500);

                // we have not allowed events to go through the second time, so they shouldn't
                Assert.Equal(records1 * 2, sink.Count);
                restartCts.Cancel();
                await source.StopAsync(default);
                await bm.StopAsync(default);
            }

            restartCts = new CancellationTokenSource();
            bm = await StartBookmarkManager(restartCts.Token);
            sink = new ThrottledListEventSink(bm);
            using (var source = CreateSource("*.log", sink, InitialPositionEnum.Bookmark, bm, true))
            {
                await source.StartAsync(restartCts.Token);

                // wait for source to collect the logs
                await Task.Delay(500);
                await sink.AllowEvents();

                // now the sink should contain all the records from previous time
                // if 'bookmarkOnFlush' doesn't work, the bookmark is already saved when the source sends the data to the sink
                // the last time, so no record is written this time.
                Assert.Equal(records2 * 2, sink.Count);
                Assert.True(sink.All(e => (e as IEnvelope<string>).Data == "test2"));

                restartCts.Cancel();
                await source.StopAsync(default);
                await bm.StopAsync(default);
            }
        }

        private ValueTask<IBookmarkManager> StartBookmarkManager() => StartBookmarkManager(_cts.Token);

        private async ValueTask<IBookmarkManager> StartBookmarkManager(CancellationToken cancellationToken)
        {
            var manager = new FileBookmarkManager(_bookmarkDir, NullLogger.Instance);
            await manager.StartAsync(cancellationToken);
            return manager;
        }

        private AsyncDirectorySource<string, LogContext> CreateSource(string filters, IEventSink sink,
            InitialPositionEnum initialPosition, IBookmarkManager bookmarkManager, bool bookmarkOnSinkFlush = false)
        {
            var logger = NullLogger.Instance;
            var ctx = new PluginContext(null, logger, null, bookmarkManager, null, null);
            var source = new AsyncDirectorySource<string, LogContext>(_sourceId, _testDir, new SingleLineLogParser(logger, 0, null, 1024),
                bookmarkManager, new DirectorySourceOptions
                {
                    NameFilters = filters.Split('|', StringSplitOptions.RemoveEmptyEntries),
                    QueryPeriodMs = 100,
                    InitialPosition = initialPosition,
                    BookmarkOnBufferFlush = bookmarkOnSinkFlush
                }, ctx);
            source.Subscribe(sink);

            return source;
        }

        private void WriteLine(string file, string text, int count = 1, bool truncate = false)
        {
            var lines = Enumerable.Repeat(text, count).ToArray();
            var filePath = Path.Combine(_testDir, file);
            if (truncate)
            {
                File.WriteAllLines(filePath, lines);
            }
            else
            {
                File.AppendAllLines(filePath, lines);
            }
        }
    }
}
