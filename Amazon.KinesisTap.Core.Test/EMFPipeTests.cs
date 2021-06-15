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
using System.Linq;
using Amazon.KinesisTap.Core.EMF;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    public class EMFPipeTests
    {
        [Fact]
        public void ConvertsIISLogs()
        {
            var logs = new string[]
            {
                "2017-05-31 06:00:30 W3SVC1 EC2AMAZ-HCNHA1G 10.10.10.10 POST /DoWork - 443 EXAMPLE\\jonsmith 11.11.11.11 HTTP/1.1 SEA-HDFEHW23455/1.0.9/jonsmith - - localhost 500 2 0 1950 348 158",
                "2017-05-31 06:00:30 W3SVC1 EC2AMAZ-HCNHA1G ::1 GET / - 80 - ::1 HTTP/1.1 Mozilla/5.0+(Windows+NT+10.0;+WOW64;+Trident/7.0;+rv:11.0)+like+Gecko - - localhost 200 0 0 950 348 128",
                "2017-05-31 06:00:30 W3SVC1 EC2AMAZ-HCNHA1G ::1 GET / - 80 - ::1 HTTP/1.1 Mozilla/5.0+(Windows+NT+10.0;+WOW64;+Trident/7.0;+rv:11.0)+like+Gecko - - localhost 401 1 0 50 348 150",
                "2017-05-31 06:00:30 W3SVC1 EC2AMAZ-HCNHA1G ::1 GET / - 80 - ::1 HTTP/1.1 Mozilla/5.0+(Windows+NT+10.0;+WOW64;+Trident/7.0;+rv:11.0)+like+Gecko - - localhost 503 7 0 550 348 192",
                "2017-05-31 06:00:30 W3SVC1 EC2AMAZ-HCNHA1G ::1 GET /iisstart.png - 80 - ::1 HTTP/1.1 Mozilla/5.0+(Windows+NT+10.0;+WOW64;+Trident/7.0;+rv:11.0)+like+Gecko - http://localhost/ localhost 200 0 0 99960 317 3"
            };

            var config = TestUtility.GetConfig("Pipes", "IISEMFTestPipe");
            var context = new PluginContext(config, NullLogger.Instance, null);
            var sink = new MockEventSink(context);
            context.ContextData[PluginContext.SINK_TYPE] = sink.GetType();

            var pipe = new EMFPipe<IDictionary<string, string>>(context);
            pipe.Subscribe(sink);

            var records = ParseW3SVCLogs(logs);
            foreach (var record in records)
            {
                pipe.OnNext(new Envelope<IDictionary<string, string>>(record));
            }

            Assert.Equal(5, sink.Records.Count);
            var jo = JObject.Parse(sink.Records.First());
            Assert.Equal("10.10.10.10", jo["s-ip"].ToString());
            Assert.Equal("POST", jo["cs-method"].ToString());
            Assert.Equal("/DoWork", jo["cs-uri-stem"].ToString());
            Assert.Equal("443", jo["s-port"].ToString());
            Assert.Equal("11.11.11.11", jo["c-ip"].ToString());
            Assert.Equal(@"EXAMPLE\jonsmith", jo["cs-username"].ToString());
            Assert.Equal(@"SEA-HDFEHW23455/1.0.9/jonsmith", jo["cs-User-Agent"].ToString());
            Assert.Equal("500", jo["sc-status"].ToString());
            Assert.Equal("2", jo["sc-substatus"].ToString());
            Assert.Equal("IISNamespace", jo["_aws"]["CloudWatchMetrics"][0]["Namespace"].ToString());
            Assert.Equal("time-taken", jo["_aws"]["CloudWatchMetrics"][0]["Metrics"][0]["Name"].ToString());
        }

        [Fact]
        public void ConvertsPowerShellSource()
        {
            var records = new List<Envelope<JObject>>
            {
                new Envelope<JObject>(JObject.Parse("{\"ComputerName\":\"MyComputer\",\"Name\":\"TrustedInstaller\",\"Status\":\"Running\"}")),
                new Envelope<JObject>(JObject.Parse("{\"ComputerName\":\"MyComputer\",\"Name\":\"WinRM\",\"Status\":\"Stopped\"}"))
            };

            var config = TestUtility.GetConfig("Pipes", "PSEMFTestPipe");
            using (var logger = new MemoryLogger(nameof(EMFPipeTests)))
            {
                var context = new PluginContext(config, logger, null);
                var source = new MockEventSource<JObject>(context);
                var sink = new MockEventSink(context);
                context.ContextData[PluginContext.SOURCE_TYPE] = source.GetType();
                context.ContextData[PluginContext.SOURCE_OUTPUT_TYPE] = source.GetOutputType();
                context.ContextData[PluginContext.SINK_TYPE] = sink.GetType();
                var pipe = new EMFPipe<JObject>(context);
                source.Subscribe(pipe);
                pipe.Subscribe(sink);

                foreach (var r in records)
                    source.MockEvent(r.Data);

                Assert.Equal(2, sink.Records.Count);
                var jo = JObject.Parse(sink.Records.First());
                Assert.Equal("PSNamespace", jo["_aws"]["CloudWatchMetrics"][0]["Namespace"].ToString());
                Assert.Equal("ServiceStatus", jo["_aws"]["CloudWatchMetrics"][0]["Metrics"][0]["Name"].ToString());
                Assert.Equal(1, jo["ServiceStatus"].ToObject<int>());
                Assert.Equal("Running", jo["Status"].ToString());
                Assert.Equal("TrustedInstaller", jo["Name"].ToString());

                var dims = jo["_aws"]["CloudWatchMetrics"][0]["Dimensions"][0].ToArray().Select(i => i.ToString()).ToList();
                Assert.Equal(3, dims.Count);
                Assert.Contains("Name", dims);
                Assert.Contains("ComputerName", dims);
                Assert.Contains("Status", dims);

                jo = JObject.Parse(sink.Records.Last());
                Assert.Equal("Stopped", jo["Status"].ToString());
                Assert.Equal("WinRM", jo["Name"].ToString());
            }
        }

        private static List<IDictionary<string, string>> ParseW3SVCLogs(string[] logs)
        {
            const string headerLine = "date time s-sitename s-computername s-ip cs-method cs-uri-stem cs-uri-query s-port cs-username c-ip cs-version cs(User-Agent) cs(Cookie) cs(Referer) cs-host sc-status sc-substatus sc-win32-status sc-bytes cs-bytes time-taken";
            var headers = headerLine.Split(' ', StringSplitOptions.RemoveEmptyEntries);

            var results = new List<IDictionary<string, string>>();

            foreach (var log in logs)
            {
                var data = new Dictionary<string, string>();
                var fragments = log.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                for (var i = 0; i < headers.Length; i++)
                {
                    data[headers[i]] = fragments[i];
                }
                results.Add(data);
            }
            return results;
        }
    }
}
