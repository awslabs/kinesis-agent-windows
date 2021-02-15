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
using System.Threading;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Batch element <typeparamref name="T"/> and emits IList<typeparamref name="T"/> on timer and other limits
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class Batch<T>
    {
        TimeSpan _interval;
        private long[] _limits;
        private Func<T, long>[] _getCounts;
        private readonly Action<List<T>, long[], FlushReason> _onBatch;

        protected List<T> _queue = new List<T>();
        protected long[] _counts;

        protected object _lockObject = new object();
        protected Timer _maxTimeSpanTimer;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="interval">Timer interval</param>
        /// <param name="limits">An array of limit</param>
        /// <param name="getCounts">An array of functions to convert T to count</param>
        /// <param name="onBatch">Call back handler.</param>
        public Batch(TimeSpan interval, long[] limits, Func<T, long>[] getCounts, Action<List<T>, long[], FlushReason> onBatch)
        {
            //Some fail-fast checking
            Guard.ArgumentNotNull(onBatch, "onBatch");
            if (limits.Length != getCounts.Length)
                throw new ArgumentException("The length of limits must be equal to length of getCounts");

            _interval = interval;
            _limits = limits;
            _counts = new long[limits.Length];
            _getCounts = getCounts;
            _onBatch = onBatch;
            _maxTimeSpanTimer = new Timer(
                OnTimer, 
                null, 
                (int)(_interval.TotalMilliseconds * Utility.Random.NextDouble()), //Randomize the first start time
                (int)_interval.TotalMilliseconds);
        }

        public Batch(TimeSpan interval, long limit, Func<T, long> getCount, Action<IList<T>, long[], FlushReason> onBatch)
            : this(interval, new long[] { limit }, new Func<T, long>[] {  getCount }, onBatch)
        {
        }

        /// <summary>
        /// This method could block until there is room to add the item
        /// </summary>
        /// <param name="item">Item to be added</param>
        public void Add(T item)
        {
            long[] newCounts = GetNewCounts(item);

            //Need to block and synchronize so that counts does not become outdated
            lock (_lockObject)
            {
                //Precheck
                if (ShouldFlushBeforeAdd(newCounts))
                {
                    Flush(FlushReason.BeforeAdd);
                }

                //Add Item
                _queue.Add(item);
                UpdateCounts(newCounts);
                //Postcheck
                if (ShouldFlushAfterAdd())
                {
                    Flush(FlushReason.AfterAdd);
                }
            }
        }

        /// <summary>
        /// Stop timer and flush queue;
        /// </summary>
        public void Stop()
        {
            //If already locked, probably will be flashed by other calls so don't wait indefinitely
            if (Monitor.TryEnter(_lockObject, 1000))
            {
                try
                {
                    Flush(FlushReason.Stop);
                }
                finally
                {
                    Monitor.Exit(_lockObject);
                }
            }
        }

        private void Flush(FlushReason reason)
        {
            if (_queue.Count > 0)
            {
                _maxTimeSpanTimer.Change(Timeout.Infinite, Timeout.Infinite);
                _onBatch(_queue, _counts, reason);
                Reset();
                if (reason != FlushReason.Stop)
                {
                    _maxTimeSpanTimer.Change((int)_interval.TotalMilliseconds, (int)_interval.TotalMilliseconds);
                }
            }
        }

        private void Reset()
        {
            _queue = new List<T>();
            _counts = new long[_limits.Length];
        }

        private void OnTimer(object state)
        {
            //If already locked, simply skip
            if (Monitor.TryEnter(_lockObject))
            {
                try
                {
                    Flush(FlushReason.Timer);
                }
                finally
                {
                    Monitor.Exit(_lockObject);
                }
            }
        }

        private long[] GetNewCounts(T item)
        {
            long[] newCounts = new long[_getCounts.Length];
            for (int i = 0; i < newCounts.Length; i++)
            {
                newCounts[i] = _getCounts[i](item);
            }
            return newCounts;
        }

        private bool ShouldFlushBeforeAdd(long[] newCounts)
        {
            for(int i = 0; i < _limits.Length; i++)
            {
                if (newCounts[i] + _counts[i] > _limits[i])
                {
                    return true;
                }
            }
            return false;
        }

        private void UpdateCounts(long[] newCounts)
        {
            for (int i = 0; i < _limits.Length; i++)
            {
                _counts[i] += newCounts[i];
            }
        }

        private bool ShouldFlushAfterAdd()
        {
            for (int i = 0; i < _limits.Length; i++)
            {
                if (_counts[i] >= _limits[i])
                {
                    return true;
                }
            }
            return false;
        }
    }
}
