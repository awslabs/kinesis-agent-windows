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
using Xunit;

namespace Amazon.KinesisTap.Core.Test.Components
{
    public class TokenBucketTest
    {
        [Fact]
        public void TestTokenBucket()
        {
            TokenBucket tb = new TokenBucket(1000, 2000);
            long delay1 = tb.GetMillisecondsDelay(1000);
            Assert.Equal(0, delay1);

            long delay2 = tb.GetMillisecondsDelay(1000);
            Assert.InRange(delay2, 400, 500);

            Thread.Sleep(1000);
            long delay3 = tb.GetMillisecondsDelay(1000);
            Assert.Equal(0, delay3);

            Thread.Sleep(1000);
            long delay4 = tb.GetMillisecondsDelay(2000);
            Assert.InRange(delay4, 400, 500);
        }

        [Fact]
        public void TestTokenBucketWithRateAdjustmentFactor()
        {
            TokenBucket tb = new TokenBucket(1000, 2000);
            long delay1 = tb.GetMillisecondsDelay(1000, 0.5);
            Assert.Equal(0, delay1);

            long delay2 = tb.GetMillisecondsDelay(1000, 0.5);
            Assert.InRange(delay2, 800, 1000);
        }
    }
}
