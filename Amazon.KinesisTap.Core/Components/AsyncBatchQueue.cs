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
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Data structure providing buffering functionality.
    /// </summary>
    /// <typeparam name="T">Type of elements.</typeparam>
    public class AsyncBatchQueue<T>
    {
        private readonly ISimpleQueue<List<T>> _secondaryQueue;
        private readonly long[] _limits;
        private readonly Func<T, long>[] _counters;
        private readonly Channel<T> _channel;
        private readonly ConcurrentQueue<T> _outstandingQ = new();

        public AsyncBatchQueue(int maxSize, long[] limits, Func<T, long>[] counters)
            : this(maxSize, limits, counters, null)
        {
        }

        public AsyncBatchQueue(int capacity, long[] limits, Func<T, long>[] counters, ISimpleQueue<List<T>> secondaryQueue)
        {
            Guard.ArgumentNotNull(limits, nameof(limits));
            Guard.ArgumentNotNull(counters, nameof(counters));

            if (limits.Length != counters.Length)
            {
                throw new ArgumentException("Limits and calculators lengths must match");
            }

            _channel = Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)
            {
                FullMode = BoundedChannelFullMode.Wait
            });

            Capacity = capacity;
            _limits = limits;
            _counters = counters;
            _secondaryQueue = secondaryQueue;
        }

        /// <summary>
        /// Queue capacity
        /// </summary>
        public int Capacity { get; }

        /// <summary>
        /// Returns an estimate of the current queue size
        /// </summary>
        public int EstimateSize() => _outstandingQ.Count + (_channel.Reader.CanCount ? _channel.Reader.Count : 0);

        /// <summary>
        /// Returns an estimate of the secondary queue size
        /// </summary>
        public int EstimateSecondaryQueueSize() => _secondaryQueue?.Count ?? 0;

        /// <summary>
        /// Determine if this queue has a secondary queue.
        /// </summary>
        /// <returns>True iff the queue has a secondary queue</returns>
        public bool HasSecondaryQueue() => _secondaryQueue is not null;

        /// <summary>
        /// Push to the secondary queue.
        /// </summary>
        /// <param name="items">List of items.</param>
        public ValueTask PushSecondaryAsync(List<T> items)
        {
            if (_secondaryQueue is null)
            {
                throw new InvalidOperationException("Secondary queue does not exist");
            }

            _secondaryQueue.TryEnqueue(items);
            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// Push an item to the primary queue.
        /// </summary>
        /// <param name="item">Item to push.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public ValueTask PushAsync(T item, CancellationToken cancellationToken = default) => _channel.Writer.WriteAsync(item, cancellationToken);

        /// <summary>
        /// Get the next batch of items within a timeout period.
        /// </summary>
        /// <param name="output">List to store the items.</param>
        /// <param name="timeoutMs">Timeout period in ms. Value of -1 stands for infinite timeout. Value of 0 to get what is available without waiting.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public async ValueTask GetNextBatchAsync(List<T> output, int timeoutMs, CancellationToken cancellationToken = default)
        {
            // check if we can go to the fast path
            if (_secondaryQueue is null || _secondaryQueue.Count == 0)
            {
                // this condition happens either when the secondary queue is not used, or if it is empty.
                // in the 'empty' case, this means the sink is sending records faster than produced,  
                // in which case we no longer fetch from the secondary queue
                await GetNextBatchFromBuffer(output, timeoutMs, cancellationToken);
                return;
            }

            GetNextBatchFromSecondaryQueue(output);
            var ownOutput = new List<T>();

            // get the next batch from buffer with '0' timeout, to get whatever is in the buffer right away
            await GetNextBatchFromBuffer(ownOutput, 0, cancellationToken);
            if (ownOutput.Count > 0)
            {
                await PushSecondaryAsync(ownOutput);
            }
        }

        private void GetNextBatchFromSecondaryQueue(List<T> output)
        {
            var counts = new long[_limits.Length];

            while (_outstandingQ.TryDequeue(out var item))
            {
                for (var i = 0; i < counts.Length; i++)
                {
                    counts[i] += _counters[i].Invoke(item);
                    if (counts[i] > _limits[i])
                    {
                        _outstandingQ.Enqueue(item);
                        return;
                    }
                }
                output.Add(item);
            }

            var stop = false;
            while (!stop)
            {
                if (!_secondaryQueue.TryDequeue(out var listItem))
                {
                    return;
                }

                while (listItem.Count > 0 && !stop)
                {
                    var item = listItem[0];
                    for (var i = 0; i < counts.Length; i++)
                    {
                        counts[i] += _counters[i].Invoke(item);
                        if (counts[i] > _limits[i])
                        {
                            stop = true;
                            break;
                        }
                    }
                    if (!stop)
                    {
                        output.Add(item);
                        listItem.RemoveAt(0);
                    }

                    if (stop && listItem.Count > 0)
                    {
                        _secondaryQueue.TryEnqueue(listItem);
                    }
                }
            }
        }

        private async ValueTask GetNextBatchFromBuffer(List<T> output, int timeoutMs, CancellationToken cancellationToken)
        {
            var counts = new long[_limits.Length];

            while (_outstandingQ.TryDequeue(out var item))
            {
                for (var i = 0; i < counts.Length; i++)
                {
                    counts[i] += _counters[i].Invoke(item);
                    if (counts[i] > _limits[i])
                    {
                        _outstandingQ.Enqueue(item);
                        return;
                    }
                }
                output.Add(item);
            }

            while (_channel.Reader.TryRead(out var item))
            {
                for (var i = 0; i < counts.Length; i++)
                {
                    counts[i] += _counters[i].Invoke(item);
                    if (counts[i] > _limits[i])
                    {
                        _outstandingQ.Enqueue(item);
                        return;
                    }
                }

                output.Add(item);
            }

            if (timeoutMs == 0)
            {
                return;
            }

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(timeoutMs);

            try
            {
                while (!cts.IsCancellationRequested)
                {
                    var item = await _channel.Reader.ReadAsync(cts.Token);
                    for (var i = 0; i < counts.Length; i++)
                    {
                        counts[i] += _counters[i].Invoke(item);
                        if (counts[i] > _limits[i])
                        {
                            _outstandingQ.Enqueue(item);
                            return;
                        }
                    }

                    output.Add(item);
                }
            }
            catch (OperationCanceledException)
            {
                // this can either be due to timeout or the cancellationToken is cancelled
                // if this is a timeout then the try/catch clause will exit, the function would just return
                cancellationToken.ThrowIfCancellationRequested();
            }
        }
    }
}
