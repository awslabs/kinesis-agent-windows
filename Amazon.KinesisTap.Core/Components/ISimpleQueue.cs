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
    /// <summary>
    /// A simple queue interface with capacity and queue/enqueue operations
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface ISimpleQueue<T>
    {
        /// <summary>
        /// Tries to add an item from the queue, returns true if successful and false if not.
        /// </summary>
        /// <param name="item">The item to add to the queue.</param>
        bool TryEnqueue(T item);

        /// <summary>
        /// Tries to remove an item from the queue, returns true if successful and false if not.
        /// </summary>
        /// <param name="item">The item removed from the queue.</param>
        bool TryDequeue(out T item);

        /// <summary>
        /// Gets the number of items currently in the queue.
        /// </summary>
        int Count { get; }

        /// <summary>
        /// Gets the capacity of the queue.
        /// </summary>
        int Capacity { get; }
    }
}
