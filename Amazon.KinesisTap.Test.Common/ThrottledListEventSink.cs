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
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Amazon.KinesisTap.Test.Common
{
    /// <summary>
    /// A sink that only add events when allowed. Also support BookmarkManager
    /// </summary>
    public class ThrottledListEventSink : List<IEnvelope>, IEventSink
    {
        protected readonly ILogger _logger;
        private readonly IBookmarkManager _bookmarkManager;
        private readonly Channel<IEnvelope> _channel = Channel.CreateUnbounded<IEnvelope>();
        protected int _hasBookmarkableSource = -1;

        public ThrottledListEventSink(IBookmarkManager bookmarkManager) : this(NullLogger.Instance, bookmarkManager) { }

        public ThrottledListEventSink(ILogger logger, IBookmarkManager bookmarkManager)
        {
            _logger = logger;
            _bookmarkManager = bookmarkManager;
        }

        public string Id { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public void OnCompleted()
        {
            _logger.LogInformation($"Sink {Id} completed");
        }

        public void OnError(Exception error)
        {
            _logger.LogError(error, $"Sink {Id} error");
        }

        public void OnNext(IEnvelope value)
        {
            _channel.Writer.TryWrite(value);
        }

        public ValueTask AllowEvents(int count = int.MaxValue)
        {
            var batch = new List<IEnvelope>();
            while (batch.Count < count)
            {
                if (!_channel.Reader.TryRead(out var envelope))
                {
                    break;
                }
                batch.Add(envelope);
            }

            AddRange(batch);
            return SaveBookmarks(batch);
        }

        protected async ValueTask SaveBookmarks(List<IEnvelope> envelopes)
        {
            // Ordering records is computationally expensive, so we only want to do it if bookmarking is enabled.
            // It's much cheaper to check a boolean property than to order the records and check if they have a bookmarkId.
            // Unfortunately, we don't know if the source is bookmarkable until we get some records, so we have to set this up
            // as a nullable property and set it's value on the first incoming batch of records.
            if (_hasBookmarkableSource < 0)
            {
                var hasBookmarkableSource = envelopes.Any(e => e.BookmarkData is not null);
                Interlocked.Exchange(ref _hasBookmarkableSource, hasBookmarkableSource ? 1 : 0);
            }

            // If this is not a bookmarkable source, return immediately.
            if (_hasBookmarkableSource == 0)
            {
                return;
            }

            // The events may not be in order, and we might have records from multiple sources, so we need to do a grouping.
            var bookmarks = envelopes
                .Select(e => e.BookmarkData)
                .Where(b => b is not null)
                .GroupBy(b => b.SourceKey);

            foreach (var bm in bookmarks)
            {
                // If the bookmarkId is 0 then bookmarking isn't enabled on the source, so we'll drop it.
                await _bookmarkManager.BookmarkCallback(bm.Key, bm);
            }
        }

        public ValueTask StartAsync(CancellationToken stopToken)
        {
            _logger.LogInformation($"Sink {Id} started");
            return ValueTask.CompletedTask;
        }

        public ValueTask StopAsync(CancellationToken gracefulStopToken)
        {
            _logger.LogInformation($"Sink {Id} stopped");
            return ValueTask.CompletedTask;
        }
    }
}
