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
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    public class EventSourceTest
    {
        [Fact]
        public void TestInitialPositionUnspecified()
        {
            RunInitialPositionTest("InitialPositionUnspecified", InitialPositionEnum.Bookmark);
        }

        [Fact]
        public void TestInitialPositionEOS()
        {
            RunInitialPositionTest("InitialPositionEOS", InitialPositionEnum.EOS);
        }

        [Fact]
        public void TestInitialPosition0()
        {
            RunInitialPositionTest("InitialPosition0", InitialPositionEnum.BOS);
        }

        [Fact]
        public void TestInitialPosition0NoQuote()
        {
            RunInitialPositionTest("InitialPosition0NoQuote", InitialPositionEnum.BOS);
        }

        [Fact]
        public void TestInitialPositionBookmark()
        {
            RunInitialPositionTest("InitialPositionBookmark", InitialPositionEnum.Bookmark);
        }

        [Fact]
        public void TestInitialPositionTimestamp()
        {
            var source = RunInitialPositionTest("InitialPositionTimestamp", InitialPositionEnum.Timestamp);
            Assert.Equal(new DateTime(2017, 8, 20, 12, 3, 0), source.InitialPositionTimestamp);
        }

        [Fact]
        public void InitialPositionTimestampMissingTimestamp()
        {
            Assert.ThrowsAny<Exception>(() => RunInitialPositionTest("InitialPositionTimestampMissingTimestamp", InitialPositionEnum.Timestamp));
        }

        [Fact]
        public void InitialPositionTimestampBadTimestamp()
        {
            Assert.ThrowsAny<Exception>(() => RunInitialPositionTest("InitialPositionTimestampBadTimestamp", InitialPositionEnum.Bookmark));
        }

        [Fact]
        public void BadInitialPosition()
        {
            Assert.ThrowsAny<Exception>(() => RunInitialPositionTest("BadInitialPosition", InitialPositionEnum.Bookmark));
        }

        private static EventSource<string> RunInitialPositionTest(string id, InitialPositionEnum expectedInitialPosition)
        {
            var config = TestUtility.GetConfig("Sources", id);
            var source = new MockEventSource<string>(new PluginContext(config, null, null, new BookmarkManager()));
            EventSource<string>.LoadCommonSourceConfig(config, source);
            Assert.Equal(expectedInitialPosition, source.InitialPosition);
            return source;
        }
    }
}
