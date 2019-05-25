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

using Newtonsoft.Json.Linq;
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    public class DirectorySourceFactoryTest
    {
        [Fact]
        public void TestNonGenericCreateEventSource()
        {
            var config = TestUtility.GetConfig("Sources", "JsonLog1");
            string timetampFormat = config["TimestampFormat"];
            string timestampField = config["TimestampField"];
            IRecordParser parser = new SingleLineJsonParser(timestampField, timetampFormat);

            PluginContext context = new PluginContext(config, null, null);
            var source = DirectorySourceFactory.CreateEventSource(context, parser);
            Assert.NotNull(source);
            Assert.IsType<DirectorySource<JObject, LogContext>>(source);
        }

        [Fact]
        public void TestNonGenericCreateEventSourceWithDelimitedParser()
        {
            var config = TestUtility.GetConfig("Sources", "DHCPParsed");
            string timestampField = config["TimestampField"];
            PluginContext context = new PluginContext(config, null, null);
            IRecordParser parser = DirectorySourceFactory.CreateDelimitedLogParser(context, timestampField, DateTimeKind.Utc);
            var source = DirectorySourceFactory.CreateEventSource(context, parser);
            Assert.NotNull(source);
            Assert.IsType<DirectorySource<DelimitedLogRecord, DelimitedLogContext>>(source);
        }
    }
}
