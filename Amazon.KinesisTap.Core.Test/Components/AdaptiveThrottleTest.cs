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
using Xunit;

namespace Amazon.KinesisTap.Core.Test.Components
{
    public class AdaptiveThrottleTest
    {
        [Fact]
        public void TestAdaptiveThrottle()
        {
            AdaptiveThrottle throttle = new AdaptiveThrottle(new TokenBucket(1000, 2000), 1.0d / 2, 1.0d / 2, 1.0d / 8);
            throttle.SetError();
            Assert.Equal(1.0d / 2, throttle.RateAdjustmentFactor);
            throttle.SetError();
            throttle.SetError();
            throttle.SetError();
            double rateAdjustmentFactor = throttle.RateAdjustmentFactor;
            //Should backoff
            Assert.Equal(1.0d / 8, rateAdjustmentFactor);
            throttle.SetSuccess();
            //Should stay
            Assert.Equal(rateAdjustmentFactor, throttle.RateAdjustmentFactor);
            long millisecondsDelay = throttle.GetDelayMilliseconds(1200);
            //Should be throttled
            Assert.True(millisecondsDelay > 0);
            //Should recover
            Assert.True(throttle.RateAdjustmentFactor > rateAdjustmentFactor);
        }
    }
}
