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
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Amazon.KinesisTap.Filesystem.Test
{
    public class W3SVCLogParserTest : IDisposable
    {
        private readonly string _testFile = Path.Combine(AppContext.BaseDirectory, Guid.NewGuid().ToString() + ".txt");
        private static readonly List<string> _samples = new()
        {
            "#Software: Microsoft Internet Information Services 10.0",
            "#Version: 1.0",
            "#Date: 2017-05-31 06:00:30",
            "#Fields: date time s-sitename s-computername s-ip cs-method cs-uri-stem cs-uri-query s-port cs-username c-ip cs-version cs(User-Agent) cs(Cookie) cs(Referer) cs-host sc-status sc-substatus sc-win32-status sc-bytes cs-bytes time-taken",
            "2017-05-31 06:00:30 W3SVC1 EC2AMAZ-HCNHA1G ::1 GET / - 80 - ::1 HTTP/1.1 Mozilla/5.0+(Windows+NT+10.0;+WOW64;+Trident/7.0;+rv:11.0)+like+Gecko - - localhost 200 0 0 950 348 128",
            "2017-05-31 06:00:30 W3SVC1 EC2AMAZ-HCNHA1G ::1 GET /iisstart.png - 80 - ::1 HTTP/1.1 Mozilla/5.0+(Windows+NT+10.0;+WOW64;+Trident/7.0;+rv:11.0)+like+Gecko - http://localhost/ localhost 200 0 0 99960 317 3"
        };

        public void Dispose()
        {
            if (File.Exists(_testFile))
            {
                File.Delete(_testFile);
            }
        }

        [Fact]
        public async Task TestCancelledTask()
        {
            using var cts = new CancellationTokenSource();
            await File.WriteAllLinesAsync(_testFile, _samples);
            var parser = new AsyncW3SVCLogParser(NullLogger.Instance, null, new DelimitedLogParserOptions());
            var records = new List<IEnvelope<W3SVCRecord>>();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => parser.ParseRecordsAsync(new DelimitedTextLogContext
            {
                FilePath = _testFile
            }, records, 10, cts.Token));
        }

        [Fact]
        public async Task TestW3SVCLogRecords()
        {
            await File.WriteAllLinesAsync(_testFile, _samples);

            var parser = new AsyncW3SVCLogParser(NullLogger.Instance, null, new DelimitedLogParserOptions());
            var output = new List<IEnvelope<W3SVCRecord>>();
            await parser.ParseRecordsAsync(new DelimitedTextLogContext
            {
                FilePath = _testFile
            }, output, _samples.Count);

            Assert.Equal(2, output.Count);
            var record = output[0].Data;
            Assert.Equal("2017-05-31", record["date"]);
            Assert.Equal("06:00:30", record["time"]);
            Assert.Equal("128", record["time-taken"]);
            Assert.Equal("3", output[1].Data["time-taken"]);

            string json = record.ToJson();
            Assert.True(json.IndexOf("Timestamp") > 0);

            var envelope = (ILogEnvelope)output[1];
            Assert.Equal(6, envelope.LineNumber);
        }

        [Fact]
        public async Task NoHeaderLine_NoDefaultMapping()
        {
            await File.WriteAllLinesAsync(_testFile, _samples.Where(l => !l.StartsWith("#Fields:")));

            var parser = new AsyncW3SVCLogParser(NullLogger.Instance, null, new DelimitedLogParserOptions());
            var output = new List<IEnvelope<W3SVCRecord>>();
            await parser.ParseRecordsAsync(new DelimitedTextLogContext
            {
                FilePath = _testFile
            }, output, _samples.Count);
            Assert.Empty(output);
        }

        [Fact]
        public async Task NoHeaderLine_WithDefaultMapping()
        {
            var defaultHeaders = "#Fields: date time s-sitename s-computername s-ip cs-method cs-uri-stem cs-uri-query s-port cs-username c-ip cs-version cs(User-Agent) cs(Cookie) cs(Referer) cs-host sc-status sc-substatus sc-win32-status sc-bytes cs-bytes time-taken";

            await File.WriteAllLinesAsync(_testFile, _samples.Where(l => !l.StartsWith("#Fields:")));
            var parser = new AsyncW3SVCLogParser(NullLogger.Instance, defaultHeaders, new DelimitedLogParserOptions());
            var records = new List<IEnvelope<W3SVCRecord>>();
            await parser.ParseRecordsAsync(new DelimitedTextLogContext
            {
                FilePath = _testFile
            }, records, _samples.Count);

            Assert.Equal(2, records.Count);
            var record = records[0].Data;
            Assert.Equal("2017-05-31", record["date"]);
            Assert.Equal("06:00:30", record["time"]);
            Assert.Equal("128", record["time-taken"]);
            Assert.Equal("3", records[1].Data["time-taken"]);

            var json = record.ToJson();
            Assert.True(json.IndexOf("Timestamp") > 0);

            var envelope = (ILogEnvelope)records[1];
            Assert.Equal(5, envelope.LineNumber);
        }

        [Fact]
        public async Task HeaderLineInTheMiddle_WithDefaultMapping()
        {
            await File.WriteAllLinesAsync(_testFile, new string[] { "2017-05-31 06:00:30 value1" });
            var parser = new AsyncW3SVCLogParser(NullLogger.Instance, "#Fields: date time key1", new DelimitedLogParserOptions());
            var records = new List<IEnvelope<W3SVCRecord>>();
            var context = new DelimitedTextLogContext
            {
                FilePath = _testFile
            };
            await parser.ParseRecordsAsync(context, records, 10);
            Assert.Single(records);
            Assert.Equal("value1", records[0].Data["key1"]);
            records.Clear();

            // append another header and line
            await File.AppendAllLinesAsync(_testFile, new string[] {
                "#Fields: date time key2",
                "2017-05-31 06:00:30 value2"
            });

            await parser.ParseRecordsAsync(context, records, 10);
            Assert.Single(records);
            Assert.Equal("value2", records[0].Data["key2"]);
        }

        [Fact]
        public async Task LogExpansionMark_ParserShouldRewind()
        {
            var parser = new AsyncW3SVCLogParser(NullLogger.Instance, "#Fields: date time key1", new DelimitedLogParserOptions());
            var records = new List<IEnvelope<W3SVCRecord>>();
            var context = new DelimitedTextLogContext
            {
                FilePath = _testFile
            };

            // write the initial file content
            await File.WriteAllLinesAsync(_testFile, new string[]
            {
                "#Fields: field1 field2 field3 field4",
                "before before before before"
            });
            var positionBeforeExpansionBlock = new FileInfo(_testFile).Length;

            // write expansion block
            await File.AppendAllLinesAsync(_testFile, new string[]
            {
                "\x00\x00\x00\x00\x00\x00\x00\x00"
            });

            // make sure we see only one record
            await parser.ParseRecordsAsync(context, records, 10);
            Assert.Single(records);

            // write the next record
            using (var stream = File.OpenWrite(_testFile))
            using (var writer = new StreamWriter(stream))
            {
                stream.Position = positionBeforeExpansionBlock;
                await writer.WriteLineAsync("after after after after");
                await writer.FlushAsync();
            }

            // make sure the parser catches this record
            await parser.ParseRecordsAsync(context, records, 10);
            Assert.Equal(2, records.Count);
            Assert.Equal(3, (records[1] as LogEnvelope<W3SVCRecord>).LineNumber);
            foreach (var kvp in records[1].Data)
            {
                Assert.Equal("after", kvp.Value);
            }
        }
    }
}
