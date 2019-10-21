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
using System.Text;
using System.Threading;

using Microsoft.Extensions.Logging;

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
            ILogger logger,
            Action<T> onNext,
            ISimpleQueue<T> lowPriorityQueue)
            : base(sizeHint, logger, onNext)
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
