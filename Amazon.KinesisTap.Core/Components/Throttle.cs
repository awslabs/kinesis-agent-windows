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

namespace Amazon.KinesisTap.Core
{
    public class Throttle
    {
        private TokenBucket[] _tokenBuckets;

        protected double _rateAdjustmentFactor;
        protected int _consecutiveErrorCount;

        public Throttle(TokenBucket tokenBucket) 
            : this(new TokenBucket[] { tokenBucket })
        {
        }

        public Throttle(TokenBucket[] tokenBuckets)
        {
            _tokenBuckets = tokenBuckets;
            _rateAdjustmentFactor = 1.0d;
        }

        public long GetDelayMilliseconds(long tokensNeeded)
        {
            if (_tokenBuckets.Length != 1)
                throw new ArgumentException("This overload requires a single bucket.");
            return GetDelayMilliseconds(new long[] { tokensNeeded });
        }

        public virtual long GetDelayMilliseconds(long[] tokensNeededArray)
        {
            long maxDelay = 0;
            for(int i = 0; i < tokensNeededArray.Length; i++)
            {
                long delay = _tokenBuckets[i].GetMillisecondsDelay(tokensNeededArray[i], _rateAdjustmentFactor);
                if (delay > maxDelay)
                {
                    maxDelay = delay;
                }
            }
            if (maxDelay > 0)
            {
                SetThrottled();
            }
            return maxDelay;
        }

        //Notify that the previous call was successful
        public virtual void SetSuccess()
        {
            _consecutiveErrorCount = 0;
        }

        //Notify that the previous call failed
        public virtual void SetError()
        {
            _consecutiveErrorCount++;
        }

        //Notify that the call is currently throttled
        protected virtual void SetThrottled()
        {

        }

        public int ConsecutiveErrorCount => _consecutiveErrorCount;

        public double RateAdjustmentFactor => _rateAdjustmentFactor;
    }
}
