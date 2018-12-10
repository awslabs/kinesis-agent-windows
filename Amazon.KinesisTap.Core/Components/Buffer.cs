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
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Provide a buffer.
    /// Add is blocked when the buffer is full.
    /// UnconditionalAdd allow one to add even when the buffer is full.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Buffer<T>
    {
        private readonly int _sizeHint;
        private readonly Action<T> _onNext;
        private AutoResetEvent _sourceSideWaitHandle = new AutoResetEvent(false);
        private AutoResetEvent _sinkSideWaitHandle = new AutoResetEvent(false);
        private int _pumping = 0;
        private CancellationTokenSource _cancellationSource;
        private CancellationToken _cancellationToken;
        private Queue<T> _queue = new Queue<T>();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sizeHint">A size hint. "Add" obeys. UnconditionalAdd does not.</param>
        /// <param name="onNext">A method to push to the next stage.</param>
        public Buffer(int sizeHint, Action<T> onNext)
        {
            _sizeHint = sizeHint;
            _onNext = onNext;
            _cancellationSource = new CancellationTokenSource();
            _cancellationToken = _cancellationSource.Token;
        }

        /// <summary>
        /// Add an item. If the size is exceeded, the thread is blocked.
        /// </summary>
        /// <param name="item"></param>
        public void Add(T item)
        {
            if (Count >= _sizeHint)
            {
                _sourceSideWaitHandle.WaitOne();
            }
            AddInternal(item);
        }

        //Used to add failed request back to queue
        public virtual bool Requeue(T item, bool highPriority)
        {
            AddInternal(item);
            return true;
        }

        public virtual int Count
        {
            get
            {
                lock(_queue)
                {
                    return QueueCount;
                }
            }
        }

        //Provide a way to stop bumping and persist the buffer if backed up by persistence
        public virtual void Stop()
        {
            _cancellationSource.Cancel();
        }

        protected virtual bool LowPriorityPumpOne(Action<T> onNext)
        {
            return false;
        }

        private void AddInternal(T item)
        {
            lock (_queue)
            {
                _queue.Enqueue(item);
            }
            StartPump();
        }

        private int QueueCount => _queue.Count;

        private void Pump()
        {
            while (true)
            {
                _sinkSideWaitHandle.WaitOne();
                while (true)
                {
                    //Send all the higher priority ones
                    while (Count > 0)
                    {
                        T item;
                        lock (_queue)
                        {
                            item = _queue.Dequeue();
                            if (QueueCount < _sizeHint)
                            {
                                //Allow input to send more
                                _sourceSideWaitHandle.Set();
                            }
                        }
                        _onNext(item);
                    }

                    if (_cancellationToken.IsCancellationRequested)
                    {
                        Interlocked.Exchange(ref _pumping, 0);
                        return;
                    }

                    //Send one from the lower priority and then check the higher priority queue
                    if (!LowPriorityPumpOne(_onNext))
                    {
                        break;
                    }
                }
            }
        }

        private void StartPump()
        {
            _sinkSideWaitHandle.Set();
            if (Interlocked.Exchange(ref _pumping, 1) == 0)
            {
                Task.Run((Action)Pump)
                    .ContinueWith(t =>
                    {
                        Interlocked.Exchange(ref _pumping, 0);
                        if (t.IsFaulted && t.Exception is AggregateException aex)
                        {
                            //Todo: possibly wire a logger to here to log it.
                            aex.Handle(ex => { return true; });
                        }
                    });
            }
        }
    }
}
