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
        private readonly BookmarkManager _bookmarkManager = new BookmarkManager();

        [Fact]
        public void TestUnsupportedFormat()
        {
            string id = "UnsupportedFormat";
            MemoryLogger logger = new MemoryLogger(null);
            MockEventSink sink = CreateEventSink(id, logger);
            Assert.Contains(logger.Entries, s => s.IndexOf("unexpected", StringComparison.CurrentCultureIgnoreCase) > -1);
        }

        [Theory]
        [InlineData("TextDecoration")]
        [InlineData("TextDecorationEx")]
        public void TestTextDecoration(string sinkId)
        {
            string id = sinkId + (Utility.IsWindows ? string.Empty : TestUtility.LINUX);
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

        [Theory]
        [InlineData("TextDecorationWithFileName")]
        [InlineData("TextDecorationExWithFileName")]
        public void TextDecorationWithFileName(string sinkId)
        {
            string id = sinkId + (Utility.IsWindows ? string.Empty : TestUtility.LINUX);
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

        [Theory]
        [InlineData("TextDecorationLocalVariable")]
        [InlineData("TextDecorationExLocalVariable")]
        public void TestLocalVariableDictionary(string sinkId)
        {
            MemoryLogger logger = new MemoryLogger(null);
            MockEventSource<IDictionary<string, string>> mockEventSource = CreateEventsource<IDictionary<string, string>>("InitialPositionUnspecified");
            MockEventSink sink = CreateEventSink(sinkId, logger); //"TextDecoration": "{$myvar2}"
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
            public string myvar1 { get; set; }
            public string myvar2 { get; set; }
        }

        [Theory]
        [InlineData("TextDecorationLocalVariable")]
        [InlineData("TextDecorationExLocalVariable")]
        public void TestLocalVariableObject(string sinkId)
        {
            MemoryLogger logger = new MemoryLogger(null);
            MockEventSource<testClass> mockEventSource = CreateEventsource<testClass>("InitialPositionUnspecified");
            MockEventSink sink = CreateEventSink(sinkId, logger); //"TextDecoration": "{$myvar2}"
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

        [Theory]
        [InlineData("TextDecorationLocalVariable")]
        [InlineData("TextDecorationExLocalVariable")]
        public void TestLocalVariableAnonymousObject(string sinkId)
        {
            MemoryLogger logger = new MemoryLogger(null);
            MockEventSource<object> mockEventSource = CreateEventsource<object>("InitialPositionUnspecified");
            MockEventSink sink = CreateEventSink(sinkId, logger); //"TextDecoration": "{$myvar2}"
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

        [Theory]
        [InlineData("TextDecorationLocalVariable")]
        [InlineData("TextDecorationExLocalVariable")]
        public void TestLocalVariableJObject(string sinkId)
        {
            MemoryLogger logger = new MemoryLogger(null);
            MockEventSource<JObject> mockEventSource = CreateEventsource<JObject>("InitialPositionUnspecified");
            MockEventSink sink = CreateEventSink(sinkId, logger); //"TextDecoration": "{$myvar2}"
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
        public void TestTextDecorationExJson()
        {
            MemoryLogger logger = new MemoryLogger(null);
            MockEventSource<JObject> mockEventSource = CreateEventsource<JObject>("InitialPositionUnspecified");
            MockEventSink sink = CreateEventSink("TextDecorationExJson", logger);
            mockEventSource.Subscribe(sink);
            var data1 = JObject.Parse("{\"myvar1\": \"myval1\"}");
            DateTime timestamp = DateTime.UtcNow;
            mockEventSource.MockEvent(data1, timestamp);
            Assert.Equal("{ \"var\": \"MYVAL1\" }", sink.Records[0]);
        }

        [Theory]
        [InlineData("ObjectDecoration")]
        [InlineData("ObjectDecorationEx")]
        public void TestObjectDecoration(string sinkId)
        {
            string id = sinkId + (Utility.IsWindows ? string.Empty : TestUtility.LINUX);
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

        [Theory]
        [InlineData("ObjectDecorationWithFileName")]
        [InlineData("ObjectDecorationExWithFileName")]
        public void TestObjectDecorationWithFileName(string sinkId)
        {
            string id = sinkId + (Utility.IsWindows ? string.Empty : TestUtility.LINUX);
            MemoryLogger logger = new MemoryLogger(null);
            MockEventSource<IDictionary<string, string>> mockEventSource = CreateEventsource<IDictionary<string, string>>("InitialPositionUnspecified");
            MockEventSink sink = CreateEventSink(id, logger);
            mockEventSource.Subscribe(sink);
            string text = "some text";
            Dictionary<string, string> data = new Dictionary<string, string>() { { "data", text } };
            DateTime timestamp = DateTime.UtcNow;
            string filePath = Path.Combine(TestUtility.GetTestHome(), "test.log");
            long position = 11;
            long lineNumber = 1;
            mockEventSource.MockLogEvent(data, timestamp, text, filePath, position, lineNumber);
            string result = sink.Records[0];
            Assert.Equal($"{{\"data\":\"{text}\",\"ComputerName\":\"{ComputerOrHostName}\",\"FileName\":\"{Path.GetFileName(filePath)}\",\"Position\":\"{position}\"}}",
                result);
        }

        [Theory]
        [InlineData("{ \"Message\": \"Info: MID 368937710 ICID 448324092 From: <bunny@acme.com>\" }", "From", "<bunny@acme.com>")]
        [InlineData("{ \"Message\": \"Info: MID 118880431 ICID 198591155 RID 0 To: <tweety@acme.com>\" }", "To", "<tweety@acme.com>")]
        [InlineData("{ \"Message\": \"Info: MID 115503592 Subject 'Cat alert!'\" }", "Subject", "'Cat alert!'")]
        public void TestObjectDecorationWithExpression(string input, string attribute, string value)
        {
            MemoryLogger logger = new MemoryLogger(null);
            MockEventSource<JObject> mockEventSource = CreateEventsource<JObject>("InitialPositionUnspecified");
            MockEventSink sink = CreateEventSink("ObjectDecorationExWithExpression", logger);
            mockEventSource.Subscribe(sink);
            DateTime timestamp = DateTime.UtcNow;
            JObject data = JObject.Parse(input);
            mockEventSource.MockEvent(data, timestamp);
            data.Add(attribute, value);
            Assert.Equal(data.ToString(Formatting.None), sink.Records[0]);
        }

        /// <summary>
        /// Customer requested that we make earlier attributes evaluated visible as a local variable to later attributes. This is the test.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="attribute"></param>
        /// <param name="value"></param>
        [Theory]
        [InlineData("{ \"Message\": \"Info: MID 368937710 ICID 448324092 From: <bunny@acme.com>\" }", "Combined", "<bunny@acme.com>")]
        [InlineData("{ \"Message\": \"Info: MID 118880431 ICID 198591155 RID 0 To: <tweety@acme.com>\" }", "Combined", "<tweety@acme.com>")]
        public void TestObjectDecorationResolutionOrder(string input, string attribute, string value)
        {
            MemoryLogger logger = new MemoryLogger(null);
            MockEventSource<JObject> mockEventSource = CreateEventsource<JObject>("InitialPositionUnspecified");
            MockEventSink sink = CreateEventSink("ObjectDecorationExResolutionOrder", logger);
            mockEventSource.Subscribe(sink);
            DateTime timestamp = DateTime.UtcNow;
            JObject data = JObject.Parse(input);
            mockEventSource.MockEvent(data, timestamp);
            Assert.Contains($"\"{attribute}\":\"{value}\"", sink.Records[0]);
        }

        [Theory]
        [InlineData("ObjectDecorationExWithBadExpression")]
        [InlineData("ObjectDecorationExWithBadSyntax")]
        public void TestObjectDecorationWithBadExpression(string sinkId)
        {
            MemoryLogger logger = new MemoryLogger(null);
            Assert.ThrowsAny<Exception>(() => CreateEventSink(sinkId, logger));
        }

        private MockEventSink CreateEventSink(string id, ILogger logger)
        {
            var config = TestUtility.GetConfig("Sinks", id);
            var source = new MockEventSink(new PluginContext(config, logger, null, _bookmarkManager));
            return source;
        }

        private MockEventSource<T> CreateEventsource<T>(string id)
        {
            var config = TestUtility.GetConfig("Sources", id);
            var source = new MockEventSource<T>(new PluginContext(config, null, null, _bookmarkManager));
            EventSource<T>.LoadCommonSourceConfig(config, source);
            return source;
        }

        private static string ComputerOrHostName
        {
            get
            {
                return Utility.IsWindows ? Utility.ComputerName : Utility.HostName;
            }
        }
    }
}
