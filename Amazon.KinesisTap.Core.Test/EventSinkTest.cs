using System;
using System.Linq;
using Microsoft.Extensions.Logging;
using Xunit;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace Amazon.KinesisTap.Core.Test
{
    public class EventSinkTest
    {
        [Fact]
        public void TestUnsupportedFormat()
        {
            string id = "UnsupportedFormat";
            MemoryLogger logger = new MemoryLogger(null);
            MockEventSink sink = CreateEventSink(id, logger);
            Assert.Contains(logger.Entries, s => s.IndexOf("unexpected", StringComparison.CurrentCultureIgnoreCase) > -1);
        }

        [Fact]
        public void TestTextDecoration()
        {
            string id = "TextDecoration" + (Utility.IsWindow ? string.Empty : TestUtility.LINUX);
            MemoryLogger logger = new MemoryLogger(null);
            MockEventSource<string> mockEventSource = CreateEventsource<string>("InitialPositionUnspecified");
            MockEventSink sink = CreateEventSink(id, logger);
            mockEventSource.Subscribe(sink);
            string data = "some text";
            DateTime timestamp = DateTime.UtcNow;
            mockEventSource.MockEvent(data, timestamp);
            Assert.Equal($"{ComputerOrHostName}:::{timestamp.ToString("yyyy-MM-dd HH:mm:ss")}:::{data}",
                sink.Records[0]);
        }

        [Fact]
        public void TextDecorationWithFileName()
        {
            string id = "TextDecorationWithFileName" + (Utility.IsWindow ? string.Empty : TestUtility.LINUX);
            MemoryLogger logger = new MemoryLogger(null);
            MockEventSource<string> mockEventSource = CreateEventsource<string>("InitialPositionUnspecified");
            MockEventSink sink = CreateEventSink(id, logger);
            mockEventSource.Subscribe(sink);
            string data = "some text";
            DateTime timestamp = DateTime.UtcNow;
            string filePath = Path.Combine(TestUtility.GetTestHome(), "test.log");
            long position = 11;
            long lineNumber = 1;
            mockEventSource.MockLogEvent(data, timestamp, data, filePath, position, lineNumber);
            Assert.Equal($"{ComputerOrHostName}:::{Path.GetFileName(filePath)}:::{position}:::{data}",
                sink.Records[0]);
        }

        [Fact]
        public void TestLocalVariableDictionary()
        {
            MemoryLogger logger = new MemoryLogger(null);
            MockEventSource<IDictionary<string, string>> mockEventSource = CreateEventsource<IDictionary<string, string>>("InitialPositionUnspecified");
            MockEventSink sink = CreateEventSink("TextDecorationLocalVariable", logger); //"TextDecoration": "{$myvar2}"
            mockEventSource.Subscribe(sink);
            var data1 = new Dictionary<string, string>()
            {
                {"myvar1", "myval1" }
            };
            var data2 = new Dictionary<string, string>()
            {
                {"myvar2", "myval2" }
            };
            DateTime timestamp = DateTime.UtcNow;
            mockEventSource.MockEvent(data1, timestamp);
            Assert.Empty(sink.Records);
            mockEventSource.MockEvent(data2, timestamp);
            Assert.Equal("myval2", sink.Records[0]);
        }


        private class testClass
        {
            public string myvar1 { get; set;}
            public string myvar2 { get; set; }
        }

        [Fact]
        public void TestLocalVariableObject()
        {
            MemoryLogger logger = new MemoryLogger(null);
            MockEventSource<testClass> mockEventSource = CreateEventsource<testClass>("InitialPositionUnspecified");
            MockEventSink sink = CreateEventSink("TextDecorationLocalVariable", logger); //"TextDecoration": "{$myvar2}"
            mockEventSource.Subscribe(sink);
            var data1 = new testClass
            {
                myvar1 = "myval1"
            };
            var data2 = new testClass
            {
                myvar2 = "myval2"
            };
            DateTime timestamp = DateTime.UtcNow;
            mockEventSource.MockEvent(data1, timestamp);
            Assert.Empty(sink.Records);
            mockEventSource.MockEvent(data2, timestamp);
            Assert.Equal("myval2", sink.Records[0]);
        }

        [Fact]
        public void TestLocalVariableAnonymousObject()
        {
            MemoryLogger logger = new MemoryLogger(null);
            MockEventSource<object> mockEventSource = CreateEventsource<object>("InitialPositionUnspecified");
            MockEventSink sink = CreateEventSink("TextDecorationLocalVariable", logger); //"TextDecoration": "{$myvar2}"
            mockEventSource.Subscribe(sink);
            var data1 = new 
            {
                myvar1 = "myval1"
            };
            var data2 = new 
            {
                myvar2 = "myval2"
            };
            DateTime timestamp = DateTime.UtcNow;
            mockEventSource.MockEvent(data1, timestamp);
            Assert.Empty(sink.Records);
            mockEventSource.MockEvent(data2, timestamp);
            Assert.Equal("myval2", sink.Records[0]);
        }

        [Fact]
        public void TestLocalVariableJObject()
        {
            MemoryLogger logger = new MemoryLogger(null);
            MockEventSource<JObject> mockEventSource = CreateEventsource<JObject>("InitialPositionUnspecified");
            MockEventSink sink = CreateEventSink("TextDecorationLocalVariable", logger); //"TextDecoration": "{$myvar2}"
            mockEventSource.Subscribe(sink);
            var data1 = JObject.Parse("{\"myvar1\": \"myval1\"}");
            var data2 = JObject.Parse("{\"myvar2\": \"myval2\"}");
            DateTime timestamp = DateTime.UtcNow;
            mockEventSource.MockEvent(data1, timestamp);
            Assert.Empty(sink.Records);
            mockEventSource.MockEvent(data2, timestamp);
            Assert.Equal("myval2", sink.Records[0]);
        }

        [Fact]
        public void TestObjectDecoration()
        {
            string id = "ObjectDecoration" + (Utility.IsWindow ? string.Empty : TestUtility.LINUX);
            MemoryLogger logger = new MemoryLogger(null);
            MockEventSource<IDictionary<string, string>> mockEventSource = CreateEventsource<IDictionary<string, string>>("InitialPositionUnspecified");
            MockEventSink sink = CreateEventSink(id, logger);
            mockEventSource.Subscribe(sink);
            string text = "some text";
            Dictionary<string, string> data = new Dictionary<string, string>() { { "data", text } };
            DateTime timestamp = DateTime.UtcNow;
            mockEventSource.MockEvent(data, timestamp);
            string result = sink.Records[0];
            Assert.Equal($"{{\"data\":\"{text}\",\"ComputerName\":\"{ComputerOrHostName}\",\"DT\":\"{timestamp.ToString("yyyy-MM-dd HH:mm:ss")}\"}}",
                result);
        }

        [Fact]
        public void TestObjectDecorationWithFileName()
        {
            string id = "ObjectDecorationWithFileName" + (Utility.IsWindow ? string.Empty : TestUtility.LINUX);
            MemoryLogger logger = new MemoryLogger(null);
            MockEventSource<IDictionary<string, string>> mockEventSource = CreateEventsource<IDictionary<string, string>>("InitialPositionUnspecified");
            MockEventSink sink = CreateEventSink(id, logger);
            mockEventSource.Subscribe(sink);
            string text = "some text";
            Dictionary<string, string> data = new Dictionary<string, string>() { {"data", text } };
            DateTime timestamp = DateTime.UtcNow;
            string filePath = Path.Combine(TestUtility.GetTestHome(), "test.log");
            long position = 11;
            long lineNumber = 1;
            mockEventSource.MockLogEvent(data, timestamp, text, filePath, position, lineNumber);
            string result = sink.Records[0];
            Assert.Equal($"{{\"data\":\"{text}\",\"ComputerName\":\"{ComputerOrHostName}\",\"FileName\":\"{Path.GetFileName(filePath)}\",\"Position\":\"{position}\"}}",
                result);
        }


        private static MockEventSink CreateEventSink(string id, ILogger logger)
        {
            var config = TestUtility.GetConfig("Sinks", id);
            var source = new MockEventSink(new PluginContext(config, logger, null));
            return source;
        }

        private static MockEventSource<T> CreateEventsource<T>(string id)
        {
            var config = TestUtility.GetConfig("Sources", id);
            var source = new MockEventSource<T>(new PluginContext(config, null, null));
            EventSource<T>.LoadCommonSourceConfig(config, source);
            return source;
        }

        private static string ComputerOrHostName
        {
            get
            {
                return Utility.IsWindow ? Utility.ComputerName : Utility.HostName;
            }
        }
    }
}
