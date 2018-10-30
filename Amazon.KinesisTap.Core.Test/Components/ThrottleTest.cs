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
    public class ThrottleTest
    {
        [Fact]
        public void TestThrottle()
        {
            Throttle throttle = new Throttle(new TokenBucket(1000, 2000));
            long delay1 = throttle.GetDelayMilliseconds(1000);
            Assert.Equal(0, delay1);
            long delay2 = throttle.GetDelayMilliseconds(1000);
            Assert.InRange(delay2, 400, 500);
        }

        [Fact]
        public void TestThrottleWith2Buckets()
        {
            Throttle throttle = new Throttle(new TokenBucket[]
                {
                    new TokenBucket(1000, 2000),
                    new TokenBucket(1000 * 1000, 5 * 1000 * 1000)
                });

            Assert.ThrowsAny<Exception>(() => throttle.GetDelayMilliseconds(1000));

            long delay1 = throttle.GetDelayMilliseconds(new long[] {1000, 1000 * 1000});
            Assert.Equal(0, delay1);

            long delay2 = throttle.GetDelayMilliseconds(new long[] { 0, 1000 * 1000 });
            Assert.InRange(delay2, 150, 200);

            long delay3 = throttle.GetDelayMilliseconds(new long[] { 1000, 0 });
            Assert.InRange(delay3, 400, 500);
        }

        [Fact]
        public void TestThrottleStates()
        {
            Throttle throttle = new Throttle(new TokenBucket(1000, 2000));
            throttle.SetError();
            throttle.SetError();
            Assert.Equal(2, throttle.ConsecutiveErrorCount);
            throttle.SetSuccess();
            Assert.Equal(0, throttle.ConsecutiveErrorCount);
        }
    }
}
