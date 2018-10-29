using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public class InMemoryQueue<T> : ISimpleQueue<T>
    {
        private readonly int _capacity;
        private Queue<T> _queue;

        public InMemoryQueue(int capacity)
        {
            _capacity = capacity;
            _queue = new Queue<T>();
        }

        public int Count => _queue.Count;

        public int Capacity => _capacity;

        public T Dequeue()
        {
            return _queue.Dequeue();
        }

        public void Enqueue(T item)
        {
            if (Count >= Capacity)
                throw new InvalidOperationException("Exceed capacity.");

            _queue.Enqueue(item);
        }
    }
}
