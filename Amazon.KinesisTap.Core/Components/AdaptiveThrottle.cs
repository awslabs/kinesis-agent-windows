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
    public class AdaptiveThrottle : Throttle
    {
        private readonly double _backoffFactor;
        private readonly double _recoveryFactor;
        private readonly double _minRateFactor;

        public AdaptiveThrottle(TokenBucket tokenBucket, double backoffFactor, double recoveryFactor, double minRateFactor) 
            : this(new TokenBucket[] { tokenBucket }, backoffFactor, recoveryFactor, minRateFactor)
        {
        }

        public AdaptiveThrottle(TokenBucket[] tokenBuckets, double backoffFactor, double recoveryFactor, double minRateFactor) : base(tokenBuckets)
        {
            if (backoffFactor >= 1 || backoffFactor <= 0)
                throw new ArgumentException("Backoff factor must be between 0 and 1");

            if (recoveryFactor >= 1 || recoveryFactor <= 0)
                throw new ArgumentException("Recovery factor must be between 0 and 1");

            if (minRateFactor >= 1 || minRateFactor <= 0)
                throw new ArgumentException("Minimum rate factor must between 0 and 1");

            _backoffFactor = backoffFactor;
            _recoveryFactor = recoveryFactor;
            _minRateFactor = minRateFactor;
        }

        public override void SetError()
        {
            base.SetError();
            _rateAdjustmentFactor = Math.Max(_minRateFactor, _rateAdjustmentFactor * _backoffFactor);
        }

        protected override void SetThrottled()
        {
            // try to recover the rate if no error
            if (this.ConsecutiveErrorCount == 0)
            {
                _rateAdjustmentFactor += (1 - _rateAdjustmentFactor) * _recoveryFactor;
            }
        }
    }
}
