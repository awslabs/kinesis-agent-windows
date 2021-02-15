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
using System.IO;
using System.Linq;
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    public class W3SVCLogTest
    {
        [Fact]
        public void TestW3SVCLogRecords()
        {
            var lines = new List<string>
            {
                "#Software: Microsoft Internet Information Services 10.0",
                "#Version: 1.0",
                "#Date: 2017-05-31 06:00:30",
                "#Fields: date time s-sitename s-computername s-ip cs-method cs-uri-stem cs-uri-query s-port cs-username c-ip cs-version cs(User-Agent) cs(Cookie) cs(Referer) cs-host sc-status sc-substatus sc-win32-status sc-bytes cs-bytes time-taken",
                "2017-05-31 06:00:30 W3SVC1 EC2AMAZ-HCNHA1G ::1 GET / - 80 - ::1 HTTP/1.1 Mozilla/5.0+(Windows+NT+10.0;+WOW64;+Trident/7.0;+rv:11.0)+like+Gecko - - localhost 200 0 0 950 348 128",
                "2017-05-31 06:00:30 W3SVC1 EC2AMAZ-HCNHA1G ::1 GET /iisstart.png - 80 - ::1 HTTP/1.1 Mozilla/5.0+(Windows+NT+10.0;+WOW64;+Trident/7.0;+rv:11.0)+like+Gecko - http://localhost/ localhost 200 0 0 99960 317 3"
            };

            var parser = new W3SVCLogParser(null, null);
            var log = string.Join(Environment.NewLine, lines);
            using (var sr = new StreamReader(Utility.StringToStream(log)))
            {
                var records = parser.ParseRecords(sr, new DelimitedLogContext() { FilePath = "Memory" })
                    .ToList();
                Assert.Equal(2, records.Count);
                var record = records[0].Data;
                Assert.Equal("2017-05-31", record["date"]);
                Assert.Equal("06:00:30", record["time"]);
                Assert.Equal("128", record["time-taken"]);
                Assert.Equal("3", records[1].Data["time-taken"]);

                string json = record.ToJson();
                Assert.True(json.IndexOf("Timestamp") > 0);

                var envelope = (ILogEnvelope)records[1];
                Assert.Equal(6, envelope.LineNumber);
            }
        }

        [Fact]
        public void TestW3SVCLogs_NoHeaderLine_NoDefaultMapping()
        {
            var lines = new List<string>
            {
                "#Software: Microsoft Internet Information Services 10.0",
                "#Version: 1.0",
                "#Date: 2017-05-31 06:00:30",
                "2017-05-31 06:00:30 W3SVC1 EC2AMAZ-HCNHA1G ::1 GET / - 80 - ::1 HTTP/1.1 Mozilla/5.0+(Windows+NT+10.0;+WOW64;+Trident/7.0;+rv:11.0)+like+Gecko - - localhost 200 0 0 950 348 128",
                "2017-05-31 06:00:30 W3SVC1 EC2AMAZ-HCNHA1G ::1 GET /iisstart.png - 80 - ::1 HTTP/1.1 Mozilla/5.0+(Windows+NT+10.0;+WOW64;+Trident/7.0;+rv:11.0)+like+Gecko - http://localhost/ localhost 200 0 0 99960 317 3"
            };

            var parser = new W3SVCLogParser(null, null);
            var log = string.Join(Environment.NewLine, lines);
            using (var sr = new StreamReader(Utility.StringToStream(log)))
            {
                Assert.Throws<ArgumentNullException>(() => parser.ParseRecords(sr, new DelimitedLogContext() { FilePath = "Memory" }).ToList());
            }
        }

        [Fact]
        public void TestW3SVCLogs_NoHeaderLine_WithDefaultMapping()
        {
            var lines = new List<string>
            {
                "#Software: Microsoft Internet Information Services 10.0",
                "#Version: 1.0",
                "#Date: 2017-05-31 06:00:30",
                "2017-05-31 06:00:30 W3SVC1 EC2AMAZ-HCNHA1G ::1 GET / - 80 - ::1 HTTP/1.1 Mozilla/5.0+(Windows+NT+10.0;+WOW64;+Trident/7.0;+rv:11.0)+like+Gecko - - localhost 200 0 0 950 348 128",
                "2017-05-31 06:00:30 W3SVC1 EC2AMAZ-HCNHA1G ::1 GET /iisstart.png - 80 - ::1 HTTP/1.1 Mozilla/5.0+(Windows+NT+10.0;+WOW64;+Trident/7.0;+rv:11.0)+like+Gecko - http://localhost/ localhost 200 0 0 99960 317 3"
            };

            var parser = new W3SVCLogParser(null,
                "#Fields: date time s-sitename s-computername s-ip cs-method cs-uri-stem cs-uri-query s-port cs-username c-ip cs-version cs(User-Agent) cs(Cookie) cs(Referer) cs-host sc-status sc-substatus sc-win32-status sc-bytes cs-bytes time-taken");
            var log = string.Join(Environment.NewLine, lines);
            using (var sr = new StreamReader(Utility.StringToStream(log)))
            {
                var records = parser.ParseRecords(sr, new DelimitedLogContext() { FilePath = "Memory" })
                    .ToList();
                Assert.Equal(2, records.Count);
                var record = records[0].Data;
                Assert.Equal("2017-05-31", record["date"]);
                Assert.Equal("06:00:30", record["time"]);
                Assert.Equal("128", record["time-taken"]);
                Assert.Equal("3", records[1].Data["time-taken"]);

                string json = record.ToJson();
                Assert.True(json.IndexOf("Timestamp") > 0);

                var envelope = (ILogEnvelope)records[1];
                Assert.Equal(5, envelope.LineNumber);
            }
        }

        [Fact]
        public void TestW3SVCLogs_HeaderLineInTheMiddle_WithDefaultMapping()
        {
            var lines = new List<string>
            {
                "2017-05-31 06:00:30 value1",
            };

            var parser = new W3SVCLogParser(null, "#Fields: date time key1");
            var log = string.Join(Environment.NewLine, lines);
            var context = new DelimitedLogContext() { FilePath = "Memory" };
            using (var sr = new StreamReader(Utility.StringToStream(log)))
            {
                var records = parser.ParseRecords(sr, context).ToList();
                Assert.Single(records);
                Assert.Equal("value1", records[0].Data["key1"]);

                context.Position = sr.BaseStream.Position;
            }

            lines.Add("#Fields: date time key2");
            lines.Add("2017-05-31 06:00:30 value2");
            log = string.Join(Environment.NewLine, lines);
            using (var sr = new StreamReader(Utility.StringToStream(log)))
            {
                var records = parser.ParseRecords(sr, context).ToList();
                Assert.Single(records);
                Assert.Equal("value2", records[0].Data["key2"]);
            }
        }
    }
}
