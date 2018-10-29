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
