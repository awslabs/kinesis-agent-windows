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
    public class TokenBucket
    {
        private readonly long _bucketSize;
        private readonly double _rate;
        private long _tokens;
        private long _prevMillisecondsLapsed;

        /// <summary>
        /// An implementation of token bucket algorithm
        /// </summary>
        /// <param name="bucketSize">Size of the bucket for tokens, e.g., number of bytes or records.</param>
        /// <param name="rate">Throttled token consumptions per second.</param>
        public TokenBucket(long bucketSize, double rate)
        {
            _bucketSize = bucketSize;
            _rate = rate;
            _tokens = _bucketSize; //Initialize as full
            _prevMillisecondsLapsed = Utility.GetElapsedMilliseconds();
        }

        public long GetMillisecondsDelay(long tokensNeeded, double rateAdjustmentFactor)
        {
            double effectiveRate = _rate * rateAdjustmentFactor;
            //Update tokens
            UpdateTokens(effectiveRate);
            //Calculate delay
            long delay;
            if (_tokens >= tokensNeeded)
            {
                delay = 0;
            }
            else
            {
                delay = (long)Math.Ceiling((tokensNeeded - _tokens) * 1000 / effectiveRate);
            }
            //Update token consumed
            _tokens -= tokensNeeded;
            return delay;
        }

        private void UpdateTokens(double effectiveRate)
        {
            long newElapsedMilliseconds = Utility.GetElapsedMilliseconds();
            long elapsedMilliseconds = newElapsedMilliseconds - _prevMillisecondsLapsed;
            _prevMillisecondsLapsed = newElapsedMilliseconds;
            _tokens += (long)Math.Floor(elapsedMilliseconds * effectiveRate / 1000);
            if (_tokens > _bucketSize)
            {
                _tokens = _bucketSize;
            }
        }

        public long GetMillisecondsDelay(long tokensNeeded)
        {
            return GetMillisecondsDelay(tokensNeeded, 1.0d);
        }
    }
}
