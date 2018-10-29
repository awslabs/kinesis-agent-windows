using System;
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    public class EventSourceTest
    {
        [Fact]
        public void TestInitialPositionUnspecified()
        {
            RunInitialPositionTest("InitialPositionUnspecified", InitialPositionEnum.EOS);
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
            var source = new MockEventSource<string>(new PluginContext(config, null, null));
            EventSource<string>.LoadCommonSourceConfig(config, source);
            Assert.Equal(expectedInitialPosition, source.InitialPosition);
            return source;
        }
    }
}
