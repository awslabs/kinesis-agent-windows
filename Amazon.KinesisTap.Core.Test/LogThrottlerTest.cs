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

namespace Amazon.KinesisTap.Core.Test
{
    public class LogThrottlerTest
    {
        [Fact]
        public void TestLogThrottler()
        {
            int logTypeId = 1;
            TimeSpan deplay = TimeSpan.FromSeconds(1);

            bool shouldWrite = LogThrottler.ShouldWrite(logTypeId, deplay);
            Assert.True(shouldWrite);

            //Try again and should return false
            shouldWrite = LogThrottler.ShouldWrite(logTypeId, deplay);
            Assert.False(shouldWrite);

            //Wait for 2 seconds and should return true
            Thread.Sleep(2000);
            shouldWrite = LogThrottler.ShouldWrite(logTypeId, deplay);
            Assert.True(shouldWrite);
        }
    }
}
