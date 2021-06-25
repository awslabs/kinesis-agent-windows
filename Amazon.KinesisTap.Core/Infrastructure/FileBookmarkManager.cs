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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// A bookmark system that synchronizes data to files in a directory
    /// </summary>
    public class FileBookmarkManager : IBookmarkManager
    {
        private class SourceInfo
        {
            public IBookmarkable Source { get; set; }

            public byte[] BookmarkData { get; set; }

            public string BookmarkFilePath { get; set; }
        }

        private const int DefaultFlushPeriodMs = 20 * 1000;

        private readonly string _directory;
        private readonly int _flushPeriodMs;
        private readonly ILogger _logger;
        private readonly ConcurrentDictionary<string, SourceInfo> _sourceDataMap = new();

        /// <summary>
        /// Channel used to serialize bookmark event callbacks to avoid race conditions.
        /// </summary>
        private readonly Channel<(SourceInfo, IEnumerable<RecordBookmark>)> _callbackChannel
            = Channel.CreateBounded<(SourceInfo, IEnumerable<RecordBookmark>)>(1000);

        private Task _bookmarkDataSyncTask;
        private Task _callbackTask;

        public FileBookmarkManager(string directory, ILogger logger)
            : this(directory, DefaultFlushPeriodMs, logger)
        {
        }

        public FileBookmarkManager(string directory, int flushPeriodMs, ILogger logger)
        {
            Guard.ArgumentNotNullOrEmpty(directory, nameof(directory));
            _directory = directory;
            _flushPeriodMs = flushPeriodMs;
            _logger = logger;
        }

        public ValueTask BookmarkCallback(string sourceKey, IEnumerable<RecordBookmark> bookmarkData)
        {
            Guard.ArgumentNotNull(sourceKey, nameof(sourceKey));
            if (!_sourceDataMap.TryGetValue(sourceKey, out var sourceInfo))
            {
                _logger.LogWarning("Cannot find source {0}", sourceKey);
                return ValueTask.CompletedTask;
            }

            // push this data to the callback queue for asynchronous processing
            return _callbackChannel.Writer.WriteAsync((sourceInfo, bookmarkData));
        }

        public async Task RegisterSourceAsync(IBookmarkable source, CancellationToken stopToken)
        {
            Guard.ArgumentNotNull(source?.BookmarkKey, nameof(source.BookmarkKey));
            if (_bookmarkDataSyncTask is not null && _bookmarkDataSyncTask.IsCompleted)
            {
                // do not register once the system has stopped
                return;
            }

            var bookmarkFile = Path.Combine(_directory, $"{source.BookmarkKey}.bm");
            stopToken.ThrowIfCancellationRequested();

            try
            {
                // load the source 
                var data = File.Exists(bookmarkFile)
                    ? await File.ReadAllBytesAsync(bookmarkFile, stopToken)
                    : null;
                source.OnBookmarkLoaded(data);

                _sourceDataMap[source.BookmarkKey] = new SourceInfo
                {
                    Source = source,
                    BookmarkData = data,
                    BookmarkFilePath = bookmarkFile
                };
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // OnBookmarkLoaded might throw an exception when processing the bookmark data,
                // which might be due to corrupted file, in that case we just assume
                // that the source will continue to produce bookmark
                _logger.LogError(ex, "Error processing bookmark file {0}", bookmarkFile);

                _sourceDataMap[source.BookmarkKey] = new SourceInfo
                {
                    Source = source,
                    BookmarkData = null,
                    BookmarkFilePath = bookmarkFile
                };
            }
        }

        public ValueTask StartAsync(CancellationToken stopToken)
        {
            Directory.CreateDirectory(_directory);
            _bookmarkDataSyncTask = BookmarkDataSyncTask(stopToken);
            _callbackTask = CallbackTask(stopToken);
            return ValueTask.CompletedTask;
        }

        public async ValueTask StopAsync(CancellationToken gracefulCancelToken)
        {
            if (_callbackTask is not null)
            {
                await _callbackTask;
            }

            if (_bookmarkDataSyncTask is not null)
            {
                await _bookmarkDataSyncTask;
            }

            _logger.LogInformation("Flusing bookmark data before stopping");
            // before exitting, try to flush all the bookmarks one last time
            await FlushDataAsync(gracefulCancelToken);
            _logger.LogInformation("Stopped");
        }

        /// <summary>
        /// Process the callback queue.
        /// </summary>
        private async Task CallbackTask(CancellationToken stopToken)
        {
            while (!stopToken.IsCancellationRequested)
            {
                try
                {
                    // wait for callback job item
                    var hasData = await _callbackChannel.Reader.WaitToReadAsync(stopToken);
                    if (!hasData)
                    {
                        return;
                    }

                    // pull the item and execute it for the source
                    while (_callbackChannel.Reader.TryRead(out var callbackData))
                    {
                        await callbackData.Item1.Source.OnBookmarkCallback(callbackData.Item2);
                    }
                }
                catch (OperationCanceledException) when (stopToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error executing bookmark callback");
                }
            }
        }

        private async Task BookmarkDataSyncTask(CancellationToken stopToken)
        {
            while (!stopToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(_flushPeriodMs, stopToken);

                    await FlushDataAsync(stopToken);
                }
                catch (OperationCanceledException) when (stopToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error synchronizing bookmark data");
                }
            }
        }

        private async Task FlushDataAsync(CancellationToken cancellationToken)
        {
            var sourceData = _sourceDataMap.ToArray();
            foreach (var item in sourceData)
            {
                var newData = await FlushSourceDataAsync(item.Value, cancellationToken);
                item.Value.BookmarkData = newData;
            }
        }

        private async Task<byte[]> FlushSourceDataAsync(SourceInfo sourceInfo, CancellationToken stopToken)
        {
            // we don't want to disrupt the 'write' since that could result in some corrupt data
            // even when shutting down, it's better to not have the most up-to-date bookmark (which might results in duplicate data)
            // than to have corrupt bookmark (which might result in data duplication/losses)
            // so we simply cancel here so that the 'flush' stops
            stopToken.ThrowIfCancellationRequested();

            try
            {
                var data = sourceInfo.Source.SerializeBookmarks();
                if (data is not null)
                {
                    await File.WriteAllBytesAsync(sourceInfo.BookmarkFilePath, data);
                }
                else if (File.Exists(sourceInfo.BookmarkFilePath))
                {
                    File.Delete(sourceInfo.BookmarkFilePath);
                }
                return data;
            }
            catch (NotInitializedException)
            {
                _logger.LogWarning("Bookmark of {0} has not been initialized, bookmark will not be flushed", sourceInfo.Source.BookmarkKey);
                return sourceInfo.BookmarkData;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error flushing bookmark data for source {0}", sourceInfo.Source.BookmarkKey);
                return null;
            }
        }
    }
}
