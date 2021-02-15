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
namespace Amazon.KinesisTap.Core
{
    using System;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Buffer with high capacity. If the buffer fills up, immediately pushes any new incoming events into a persistent
    /// queue.
    /// </summary>
    /// <typeparam name="T">The type of item in the queue.</typeparam>
    public class HighCapacityBuffer<T> : Buffer<T>
    {
        private readonly ISimpleQueue<T> lowPriorityQueue;

        public HighCapacityBuffer(int sizeHint, ILogger logger, Action<T> onNext, ISimpleQueue<T> lowPriorityQueue)
            : base(sizeHint, logger, onNext)
        {
            this.lowPriorityQueue = lowPriorityQueue;
        }

        /// <inheritdoc />
        public override bool Requeue(T item, bool highPriority)
        {
            if (highPriority) return base.Requeue(item, highPriority);
            return this.lowPriorityQueue.TryEnqueue(item);
        }

        /// <inheritdoc />
        public override void Add(T item)
        {
            // If the buffer isn't full, just go with the default behavior and add the item to the buffer.
            if (Count < _sizeHint)
            {
                this._logger.LogTrace("[{0}] Buffer not full, adding batch to buffer with default behavior.", nameof(HighCapacityBuffer<T>.Add));
                base.Add(item);
                return;
            }

            // Otherwise, add it to the persistent queue and unblock this thread so we can continue pulling in more
            // items.
            this._logger.LogTrace("[{0}] Buffer full, adding item to persistent queue instead.", nameof(HighCapacityBuffer<T>.Add));
            if (this.lowPriorityQueue.TryEnqueue(item)) return;

            // If adding to the persistent queue fails, revert back to the base adding behavior (block till there is
            // space in the buffer).
            this._logger.LogWarning("[{0}] Failed to enqueue item in lower priority queue. Attempting to requeue in buffer", nameof(HighCapacityBuffer<T>.Add));

            base.Add(item);
        }

        /// <inheritdoc />
        public override int GetCurrentPersistentQueueSize()
        {
            return this.lowPriorityQueue.Count;
        }

        /// <inheritdoc />
        public override int IsPersistentQueueFull()
        {
            return (this.lowPriorityQueue.Count >= this.lowPriorityQueue.Capacity) ? 1 : 0;
        }

        /// <inheritdoc />
        protected override bool LowPriorityPumpOne(Action<T> onNext)
        {
            // Try to dequeue an item and pass it to the onNext action,
            // Return true if there an item was processed, otherwise false.
            if (!this.lowPriorityQueue.TryDequeue(out T item)) return false;
            this._logger.LogTrace("[{0}] Pumping one batch from low priority queue to sink.", nameof(HighCapacityBuffer<T>.LowPriorityPumpOne));
            onNext(item);
            return true;
        }
    }
}
