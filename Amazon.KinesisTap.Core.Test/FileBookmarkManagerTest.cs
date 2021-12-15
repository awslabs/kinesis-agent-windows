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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    [Collection(nameof(FileBookmarkManagerTest))]
    public class FileBookmarkManagerTest : IDisposable
    {
        private static readonly string _bookmarkDirName = "Bookmark";
        private readonly string _testDir = Path.Combine(AppContext.BaseDirectory, Guid.NewGuid().ToString());
        private readonly IAppDataFileProvider _appDataFileProvider;
        private long _position = -1;

        public FileBookmarkManagerTest()
        {
            Directory.CreateDirectory(Path.Combine(_testDir, _bookmarkDirName));
            _appDataFileProvider = new ProtectedAppDataFileProvider(_testDir);
        }

        public void Dispose()
        {
            if (Directory.Exists(_testDir))
            {
                Directory.Delete(_testDir, true);
            }
        }

        [Fact]
        public async Task SystemStart_DirectoryIsCreated()
        {
            if (Directory.Exists(_testDir))
            {
                Directory.Delete(_testDir, true);
                await Task.Delay(500);
            }

            // start the system
            var cts = new CancellationTokenSource();
            var system = new FileBookmarkManager(_bookmarkDirName, NullLogger.Instance, _appDataFileProvider);
            await system.StartAsync(cts.Token);
            cts.Cancel();
            await system.StopAsync(default);

            Assert.True(Directory.Exists(Path.Combine(_testDir, _bookmarkDirName)));
        }

        [Theory]
        [InlineData(null, 0)]
        [InlineData("1", 1)]
        [InlineData("123456789", 123456789)]
        public async Task SourceRegistration_DataIsLoaded(string existingBookmark, long expectedPosition)
        {
            var guid = Guid.NewGuid().ToString();
            var id = $"{nameof(SourceRegistration_DataIsLoaded)}-{guid}";
            if (existingBookmark is not null)
            {
                await File.WriteAllTextAsync(Path.Combine(_testDir, _bookmarkDirName, $"{id}.bm"), existingBookmark);
            }

            var source = MockSource_WithPosition(id);

            // start the system and register the source
            var cts = new CancellationTokenSource();
            var system = new FileBookmarkManager(_bookmarkDirName, NullLogger.Instance, _appDataFileProvider);
            await system.StartAsync(cts.Token);
            await system.RegisterSourceAsync(source, cts.Token);

            // make sure _position is loaded
            Assert.Equal(expectedPosition, _position);
            cts.Cancel();
            await system.StopAsync(default);
        }

        [Theory]
        [InlineData(0, "0")]
        [InlineData(123456789, "123456789")]
        public async Task SystemStop_DataIsSaved(long position, string expectedData)
        {
            var source = MockSource_WithPosition(nameof(SourceRegistration_DataIsLoaded));

            var cts = new CancellationTokenSource();
            var system = new FileBookmarkManager(_bookmarkDirName, NullLogger.Instance, _appDataFileProvider);
            await system.StartAsync(cts.Token);
            await system.RegisterSourceAsync(source, cts.Token);
            _position = position;

            // stop the system
            cts.Cancel();
            await system.StopAsync(default);

            var content = await File.ReadAllTextAsync(Path.Combine(_testDir, _bookmarkDirName, $"{nameof(SourceRegistration_DataIsLoaded)}.bm"));
            Assert.Equal(expectedData, content);
        }

        [Theory]
        [InlineData(123456789, "123456789")]
        public async Task SystemRunning_DataIsSynced(long position, string expectedData)
        {
            const int syncPeriodMs = 100;
            var source = MockSource_WithPosition(nameof(SourceRegistration_DataIsLoaded));
            var cts = new CancellationTokenSource();
            var system = new FileBookmarkManager(_bookmarkDirName, syncPeriodMs, NullLogger.Instance, _appDataFileProvider);
            await system.StartAsync(cts.Token);
            await system.RegisterSourceAsync(source, cts.Token);
            _position = position;

            // wait for some time
            await Task.Delay(syncPeriodMs * 4);

            string bmFileContent = null;
            for (var i = 0; i < 2; i++)
            {
                try
                {
                    bmFileContent = await File.ReadAllTextAsync(Path.Combine(_testDir, _bookmarkDirName, $"{nameof(SourceRegistration_DataIsLoaded)}.bm"));
                    break;
                }
                catch (IOException)
                {
                    // this might happen due to the file being written, so we backoff a bit
                    await Task.Delay(200);
                }
            }

            // check the content of the bookmark file
            Assert.Equal(expectedData, bmFileContent);

            // stop the system
            cts.Cancel();
            await system.StopAsync(default);
        }

        [Fact]
        public async Task SourceLoadingError_BookmarkIsStillSaved()
        {
            var mockSource = new Mock<IBookmarkable>();
            mockSource.Setup(s => s.BookmarkKey).Returns(nameof(SourceLoadingError_BookmarkIsStillSaved));
            mockSource
                .Setup(s => s.SerializeBookmarks())
                .Returns(() => Encoding.ASCII.GetBytes(Interlocked.Read(ref _position).ToString()));
            mockSource
                .Setup(s => s.OnBookmarkLoaded(It.IsAny<byte[]>()))
                .Throws(new Exception());
            var source = mockSource.Object;

            var cts = new CancellationTokenSource();
            var system = new FileBookmarkManager(_bookmarkDirName, NullLogger.Instance, _appDataFileProvider);
            await system.StartAsync(cts.Token);
            await system.RegisterSourceAsync(source, cts.Token);
            _position = 123456;

            // stop the system
            cts.Cancel();
            await system.StopAsync(default);
            // check the content of the bookmark file
            var content = await File.ReadAllTextAsync(Path.Combine(_testDir, _bookmarkDirName, $"{nameof(SourceLoadingError_BookmarkIsStillSaved)}.bm"));
            Assert.Equal(_position.ToString(), content);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(100)]
        public async Task BookmarkCallback_SourceReceivesData(int length)
        {
            const string sourceKey = nameof(BookmarkCallback_SourceReceivesData);

            var position = -1;
            var mockSource = new Mock<IBookmarkable>();
            mockSource.Setup(s => s.BookmarkKey).Returns(sourceKey);
            mockSource
                .Setup(s => s.OnBookmarkCallback(It.IsAny<IEnumerable<RecordBookmark>>()))
                .Returns((IEnumerable<RecordBookmark> data) =>
                {
                    foreach (var item in data)
                    {
                        if (item is not IntegerPositionRecordBookmark intBookmark)
                        {
                            continue;
                        }
                        var newPos = (int)Math.Max(position, intBookmark.Position);
                        Interlocked.Exchange(ref position, newPos);
                    }
                    return ValueTask.CompletedTask;
                });
            var source = mockSource.Object;

            var cts = new CancellationTokenSource();
            var system = new FileBookmarkManager(_bookmarkDirName, NullLogger.Instance, _appDataFileProvider);
            await system.StartAsync(cts.Token);
            await system.RegisterSourceAsync(source, cts.Token);

            var positions = Enumerable.Range(0, length).ToArray();
            await system.BookmarkCallback(sourceKey, positions.Select(p => new IntegerPositionRecordBookmark(sourceKey, sourceKey, p)));
            await Task.Delay(100);

            Assert.Equal(positions.Max(), position);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(500)]
        public async Task BookmarkCallback_NoRaceCondition(int concurrency)
        {
            const string sourceKey = nameof(BookmarkCallback_NoRaceCondition);
            using var semaphore = new SemaphoreSlim(0, concurrency);
            var counter = 0;

            var mockSource = new Mock<IBookmarkable>();
            mockSource.Setup(s => s.BookmarkKey).Returns(sourceKey);
            mockSource
                .Setup(s => s.OnBookmarkCallback(It.IsAny<IEnumerable<RecordBookmark>>()))
                .Returns((IEnumerable<RecordBookmark> data) =>
                {
                    // increase the counter, if there is race condition, the counter value will be overwritten by a concurrent task
                    var val = counter + 1;
                    Interlocked.Exchange(ref counter, val);
                    return ValueTask.CompletedTask;
                });
            var source = mockSource.Object;

            var cts = new CancellationTokenSource();
            var system = new FileBookmarkManager(_bookmarkDirName, NullLogger.Instance, _appDataFileProvider);
            await system.StartAsync(cts.Token);
            await system.RegisterSourceAsync(source, cts.Token);

            // prepare a bunch of concurrent callback tasks
            var callbackTasks = new Task[concurrency];
            for (var i = 0; i < concurrency; i++)
            {
                callbackTasks[i] = CallbackTask(sourceKey, system, semaphore);
            }

            // let the callbacks happen
            semaphore.Release(concurrency);
            await Task.WhenAll(callbackTasks);
            await Task.Delay(Math.Max(1000, concurrency * 5));

            // just confirm that the counter is correct -> no race condition
            Assert.Equal(concurrency, counter);
        }

        private static async Task CallbackTask(string sourceKey, FileBookmarkManager system, SemaphoreSlim semaphore)
        {
            await Task.Yield();
            await semaphore.WaitAsync();
            await system.BookmarkCallback(sourceKey, Array.Empty<RecordBookmark>());
        }

        private IBookmarkable MockSource_WithPosition(string key)
        {
            var mockSource = new Mock<IBookmarkable>();
            mockSource
                .Setup(s => s.OnBookmarkLoaded(It.IsAny<byte[]>()))
                .Callback((byte[] data) =>
                {
                    var pos = data is null
                        ? 0
                        : long.Parse(Encoding.ASCII.GetString(data));
                    Interlocked.Exchange(ref _position, pos);
                });

            mockSource.Setup(s => s.BookmarkKey).Returns(key);
            mockSource
                .Setup(s => s.SerializeBookmarks())
                .Returns(() => Encoding.ASCII.GetBytes(Interlocked.Read(ref _position).ToString()));
            return mockSource.Object;
        }
    }
}
