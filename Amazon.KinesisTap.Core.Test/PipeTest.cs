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
using Amazon.KinesisTap.Core.Pipes;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    public class PipeTest
    {
        [Fact]
        public void TestStringPipe()
        {
            var config = TestUtility.GetConfig("Pipes", "TestPipe");
            var context = new PluginContext(config, null, null);
            var source = new MockEventSource<string>(context);
            var sink = new MockEventSink(context);
            context.ContextData[PluginContext.SOURCE_TYPE] = source.GetType();
            context.ContextData[PluginContext.SINK_TYPE] = sink.GetType();
            var pipe = new PipeFactory().CreateInstance(PipeFactory.REGEX_FILTER_PIPE, context);
            source.Subscribe(pipe);
            pipe.Subscribe(sink);
            string record1 = "24,09/29/17,00:00:04,Database Cleanup Begin,,,,,0,6,,,,,,,,,0";
            source.MockEvent(record1);
            source.MockEvent("25,09/29/17,00:00:04,0 leases expired and 0 leases deleted,,,,,0,6,,,,,,,,,0");
            Assert.Single(sink.Records);
            Assert.Equal(record1, sink.Records[0]);
        }
    }
}
