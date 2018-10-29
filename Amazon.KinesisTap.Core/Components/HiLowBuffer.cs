using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Buffer with high and low priority queue allowing requeue at lower priority
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class HiLowBuffer<T> : Buffer<T>
    {
        private ISimpleQueue<T> _lowPriorityQueue;

        public HiLowBuffer(
            int sizeHint, 
            Action<T> onNext,
            ISimpleQueue<T> lowPriorityQueue)
            : base(sizeHint, onNext)
        {
            _lowPriorityQueue = lowPriorityQueue;
        }

        public override bool Requeue(T item, bool highPriority)
        {
            if (highPriority)
            {
                return base.Requeue(item, highPriority);
            }
            else
            {
                try
                {
                    _lowPriorityQueue.Enqueue(item);
                    return true;
                }
                catch
                {
                    return false;
                }
            }
        }

        //Return true if there are items to process
        protected override bool LowPriorityPumpOne(Action<T> onNext)
        {
            if (_lowPriorityQueue.Count > 0)
            {
                onNext(_lowPriorityQueue.Dequeue());
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}
