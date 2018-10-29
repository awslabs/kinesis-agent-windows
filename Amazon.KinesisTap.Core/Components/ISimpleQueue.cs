using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// A simple queue interface with capacity and queue/enqueue operations
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface ISimpleQueue<T>
    {
        /// <summary>
        /// Expect InvalidOperationException when exceeding capacity
        /// </summary>
        /// <param name="item"></param>
        void Enqueue(T item);

        /// <summary>
        /// Expect InvalidOperationException when queue is empty
        /// </summary>
        /// <returns></returns>
        T Dequeue();

        int Count { get; }

        int Capacity { get; }
    }
}
