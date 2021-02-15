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
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Provide a buffer.
    /// Add is blocked when the buffer is full.
    /// UnconditionalAdd allow one to add even when the buffer is full.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Buffer<T>
    {
        protected readonly int _sizeHint;
        protected readonly ILogger _logger;
        private readonly Action<T> _onNext;
        private readonly AutoResetEvent _sourceSideWaitHandle = new AutoResetEvent(false);
        private readonly AutoResetEvent _sinkSideWaitHandle = new AutoResetEvent(false);
        private readonly CancellationTokenSource _cancellationSource;
        private readonly CancellationToken _cancellationToken;
        private readonly ConcurrentQueue<T> _queue = new ConcurrentQueue<T>();
        private int _pumping = 0;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sizeHint">A size hint. "Add" obeys. UnconditionalAdd does not.</param>
        /// <param name="onNext">A method to push to the next stage.</param>
        public Buffer(int sizeHint, ILogger logger, Action<T> onNext)
        {
            _sizeHint = sizeHint;
            _onNext = onNext;
            _logger = logger;
            _cancellationSource = new CancellationTokenSource();
            _cancellationToken = _cancellationSource.Token;
        }

        /// <summary>
        /// Add an item. If the size is exceeded, the thread is blocked.
        /// </summary>
        /// <param name="item"></param>
        public virtual void Add(T item)
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
                return _queue.Count;
            }
        }

        //Provide a way to stop bumping and persist the buffer if backed up by persistence
        public virtual void Stop()
        {
            _cancellationSource.Cancel();
        }

        /// <summary>
        /// Gets the number of batches of events currently in the buffer.
        /// </summary>
        /// <returns>int representing the number of batches currently in the buffer.</returns>
        public virtual int GetCurrentBufferSize()
        {
            return Count;
        }

        /// <summary>
        /// Gets the number of batches of events currently in the persistent queue.
        /// A regular Buffer doesn't have a persistent queue, so this always returns 0. Any subclasses that have a
        /// persistent queue should override this method.
        /// </summary>
        /// <returns>int representing the number of batches currently in the persistent queue.</returns>
        public virtual int GetCurrentPersistentQueueSize()
        {
            return 0;
        }

        /// <summary>
        /// Returns whether or not the buffer is currently full.
        /// </summary>
        /// <returns>1 if the buffer is full, 0 otherwise.</returns>
        public virtual int IsBufferFull()
        {
            return (Count >= _sizeHint) ? 1 : 0;
        }

        /// <summary>
        /// Returns whether or not the persistent queue is currently full.
        /// A regular Buffer doesn't have a persistent queue, so this always returns 0. Any subclasses that have a
        /// persistent queue should override this method.
        /// </summary>
        /// <returns>0 if the persistent queue is full, 1 otherwise.</returns>
        public virtual int IsPersistentQueueFull()
        {
            return 0;
        }

        protected virtual bool LowPriorityPumpOne(Action<T> onNext)
        {
            return false;
        }

        private void AddInternal(T item)
        {
            _queue.Enqueue(item);
            StartPump();
        }

        private void Pump()
        {
            if (Utility.IsMacOs)
            {
                //Profiler shows that WaitOne() occasionally cause high CPU on macOS. So we use polling instead of signaling
                while (true)
                {
                    if (!DequeueTillEmpty()) return;
                    Thread.Sleep(200); //Sleep for 200 ms before trying to dequeue again. Hard-code for now until there is a need to configure.
                }
            }
            else
            {
                while (true)
                {
                    if (_sinkSideWaitHandle.WaitOne())
                    {
                        if (!DequeueTillEmpty()) return;
                    }
                }
            }
        }

        /// <summary>
        /// Pump items to the next stage until the queue is empty
        /// </summary>
        /// <returns>true: the caller can continue. false: cancellation is requested. The caller should exit.</returns>
        private bool DequeueTillEmpty()
        {
            while (true)
            {
                //Send all the higher priority ones
                while (_queue.TryDequeue(out T item))
                {
                    if (_queue.Count < _sizeHint)
                    {
                        //Allow input to send more
                        _sourceSideWaitHandle.Set();
                    }
                    _onNext(item);
                }

                if (_cancellationToken.IsCancellationRequested)
                {
                    Interlocked.Exchange(ref _pumping, 0);
                    return false;
                }

                //Send one from the lower priority and then check the higher priority queue
                if (!LowPriorityPumpOne(_onNext))
                {
                    break;
                }
            }
            return true;
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
                            aex.Handle(ex => 
                                {
                                    _logger?.LogError($"Buffer.StartPump exception: {ex.ToMinimized()}");
                                    return true;
                                });
                        }
                    });
            }
        }
    }
}
