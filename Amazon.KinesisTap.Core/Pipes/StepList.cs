using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Implement a single linked-list for steps
    /// </summary>
    public class StepList<TIn>
    {
        private readonly IStep<TIn> _head;
        private IStep _tail;

        public StepList(IStep<TIn> head)
        {
            Guard.ArgumentNotNull(head, "head");
            _head = head;
            _tail = head;
        }

        public IStep<TIn> Head
        {
            get => _head;
        }

        public IStep Tail
        {
            get => _tail;
        }

        public void Append(IStep step)
        {
            _tail.Next = step;
            _tail = step;
        }
    }
}
