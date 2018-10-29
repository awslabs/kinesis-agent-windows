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
