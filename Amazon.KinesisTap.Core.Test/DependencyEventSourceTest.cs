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
using System.Threading;
using Xunit;


namespace Amazon.KinesisTap.Core.Test
{
    public class DependencyEventSourceTest
    {
        /// <summary>
        /// Tests the case where the dependency is not available at initial startup of the source.
        /// </summary>
        [Fact]
        public void TestStartupWithUnavailableDependency()
        {
            //Setup
            var eventSource = GetMockDependentEventSource();
            eventSource.DelayBetweenDependencyPoll = TimeSpan.FromMilliseconds(500);
            eventSource.IsAvailable = false;

            //Execute
            eventSource.Start();

            //Verify
            Assert.Equal(MockSourceStates.Stopped, eventSource.State);
            Thread.Sleep(TimeSpan.FromSeconds(1));
            Assert.Equal(MockSourceStates.Stopped, eventSource.State);

            eventSource.IsAvailable = true;
            Thread.Sleep(TimeSpan.FromSeconds(2));
            Assert.Equal(MockSourceStates.Started, eventSource.State);
        }

        /// <summary>
        /// Tests the case where a dependency temporarily becomes unavailable while the source is running.
        /// </summary>
        [Fact]
        public void TestRunningWithUnavailableDependency()
        {
            //Setup
            var eventSource = GetMockDependentEventSource();
            eventSource.DelayBetweenDependencyPoll = TimeSpan.FromMilliseconds(500);
            eventSource.IsAvailable = true;

            //Execute
            eventSource.Start();

            //Verify
            Assert.Equal(MockSourceStates.Started, eventSource.State);
            Thread.Sleep(TimeSpan.FromSeconds(1));
            Assert.Equal(MockSourceStates.Started, eventSource.State);

            //Simulate detecting a problem during use of source
            eventSource.IsAvailable = false;
            eventSource.Reset();
            Assert.Equal(MockSourceStates.Stopped, eventSource.State);
            Thread.Sleep(TimeSpan.FromSeconds(1));
            Assert.Equal(MockSourceStates.Stopped, eventSource.State);

            //Simulate recovery
            eventSource.IsAvailable = true;
            Thread.Sleep(TimeSpan.FromSeconds(2));
            Assert.Equal(MockSourceStates.Started, eventSource.State);
            Thread.Sleep(TimeSpan.FromSeconds(1));
            Assert.Equal(MockSourceStates.Started, eventSource.State);
        }

        private MockDependentEventSource<int> GetMockDependentEventSource()
        {
            var config = TestUtility.GetConfig("Sources", "JsonLog1");
            return new MockDependentEventSource<int>(new PluginContext(config, null, null, new BookmarkManager()));
        }
    }
}
