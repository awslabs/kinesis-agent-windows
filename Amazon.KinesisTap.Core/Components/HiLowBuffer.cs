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
    /// Buffer with high and low priority queue allowing requeue at lower priority
    /// </summary>
    /// <typeparam name="T">The type of item in the queue.</typeparam>
    public class HiLowBuffer<T> : Buffer<T>
    {
        private readonly ISimpleQueue<T> lowPriorityQueue;

        public HiLowBuffer(int sizeHint, ILogger logger, Action<T> onNext, ISimpleQueue<T> lowPriorityQueue)
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
        public override int GetCurrentPersistentQueueSize()
        {
            return this.lowPriorityQueue.Count;
        }

        /// <inheritdoc />
        public override int IsPersistentQueueFull()
        {
            return (this.lowPriorityQueue.Count >= this.lowPriorityQueue.Capacity) ? 1 : 0;
        }

        protected override bool LowPriorityPumpOne(Action<T> onNext)
        {
            // Try to dequeue an item and pass it to the onNext action,
            // Return true if there an item was processed, otherwise false.
            if (!this.lowPriorityQueue.TryDequeue(out T item)) return false;
            onNext(item);
            return true;
        }
    }
}
