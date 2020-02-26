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
    using System.Collections.Concurrent;

    /// <summary>
    /// An in-memory implementation of the <see cref="ISimpleQueue{T}"/> interface.
    /// This class is built on a <see cref="ConcurrentQueue{T}"/> and is thread-safe.
    /// </summary>
    /// <typeparam name="T">The type of item stored in the queue.</typeparam>
    public class InMemoryQueue<T> : ISimpleQueue<T>
    {
        private readonly ConcurrentQueue<T> queue = new ConcurrentQueue<T>();

        public InMemoryQueue(int capacity)
        {
            this.Capacity = capacity;
        }

        /// <inheritdoc />
        public int Count => this.queue.Count;

        /// <inheritdoc />
        public int Capacity { get; }

        /// <inheritdoc />
        public bool TryDequeue(out T item)
        {
            return this.queue.TryDequeue(out item);
        }

        /// <inheritdoc />
        public bool TryEnqueue(T item)
        {
            if (this.Count >= this.Capacity) return false;
            this.queue.Enqueue(item);
            return true;
        }
    }
}
