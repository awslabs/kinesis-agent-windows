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
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace Amazon.KinesisTap.Filesystem.Test
{
    public class SingleLineJsonParserTest : IDisposable
    {
        const string LOG = @"{""ul-log-data"":{""method_name"":""UserProfile"",""module_name"":""ACMEController""},""ul-tag-status"":""INFO"",""ul-timestamp-epoch"":1537519130972, ""Timestamp"":""2018-09-21T08:38:50.972Z""}
{""ul-log-data"":{""http_url"":""http://acme/org"",""http_request_headers"":{""Accept"":""*/*"",""Accept-Encoding"":""gzip;deflate""},""http_response_code"":401},""ul-tag-status"":""INFO"",""ul-timestamp-epoch"":1537519131241, ""Timestamp"":""2018-09-21T08:38:51.241Z""}
";

        private readonly string _testFile = Path.Combine(AppContext.BaseDirectory, Guid.NewGuid().ToString() + ".txt");

        public void Dispose()
        {
            if (File.Exists(_testFile))
            {
                File.Delete(_testFile);
            }
        }

        [Theory]
        [InlineData("ul-timestamp-epoch", "epoch")]
        [InlineData("Timestamp", null)]
        public async Task TestJsonLog(string timestampField, string timestampFormat)
        {
            await File.WriteAllTextAsync(_testFile, LOG);

            var parser = new SingleLineJsonTextParser(NullLogger.Instance, timestampField, timestampFormat, null);
            var records = new List<IEnvelope<JObject>>();
            await parser.ParseRecordsAsync(new LogContext { FilePath = _testFile }, records, 100);

            Assert.Equal(2, records.Count);
            var record0 = records[0] as LogEnvelope<JObject>;
            Assert.Equal(new DateTime(2018, 9, 21, 8, 38, 50, 972), record0.Timestamp);
            Assert.Equal("UserProfile", record0.Data["ul-log-data"]["method_name"]);
            Assert.Equal("INFO", record0.Data["ul-tag-status"]);
        }

        /// <summary>
        /// Test parsing a log file where lines may not be written completely at first
        /// </summary>
        [Fact]
        public async Task TestParseInterleavedWrites()
        {
            var parser = new SingleLineJsonTextParser(NullLogger.Instance, null, null, null);
            var context = new LogContext { FilePath = _testFile };
            var records = new List<IEnvelope<JObject>>();

            await File.AppendAllTextAsync(_testFile, $"{{\"a\":1}}{Environment.NewLine}");
            await parser.ParseRecordsAsync(context, records, 100);
            Assert.Single(records);
            records.Clear();

            await File.AppendAllTextAsync(_testFile, $"{{\"b\":");
            await parser.ParseRecordsAsync(context, records, 100);
            Assert.Empty(records);

            await File.AppendAllTextAsync(_testFile, $"2}}{Environment.NewLine}");
            await parser.ParseRecordsAsync(context, records, 100);
            Assert.Single(records);
        }

        /// <summary>
        /// The parser should ignore invalid JSON.
        /// </summary>
        [Fact]
        public async Task TestParseInvalidLines()
        {
            var parser = new SingleLineJsonTextParser(NullLogger.Instance, null, null, null);
            var context = new LogContext { FilePath = _testFile };
            var records = new List<IEnvelope<JObject>>();

            await File.AppendAllTextAsync(_testFile, $"{{\"a\":1}}{Environment.NewLine}");
            await parser.ParseRecordsAsync(context, records, 100);
            Assert.Single(records);
            records.Clear();

            // write an in valid json line
            await File.AppendAllTextAsync(_testFile, $"{{\"b\":{Environment.NewLine}");
            await parser.ParseRecordsAsync(context, records, 100);
            Assert.Empty(records);

            // write another valid line, make sure the parser moves on
            await File.AppendAllTextAsync(_testFile, $"{{\"c\":3}}{Environment.NewLine}");
            await parser.ParseRecordsAsync(context, records, 100);
            Assert.Single(records);
        }

        /// <summary>
        /// Make sure the parser can handle long records that are not written to the file immediately.
        /// </summary>
        [Theory]
        [InlineData(4000)]
        [InlineData(8000)]
        public async Task TestParseLongRecords(int size)
        {
            var parser = new SingleLineJsonTextParser(NullLogger.Instance, null, null, null);
            var context = new LogContext { FilePath = _testFile };
            var records = new List<IEnvelope<JObject>>();
            var value = new string('a', size);

            File.AppendAllText(_testFile, $"{{\"a\":\"{value}\"}}{Environment.NewLine}");
            await parser.ParseRecordsAsync(context, records, 100);
            Assert.Single(records);
            Assert.Equal(value, (string)records[0].Data["a"]);

            records.Clear();
            File.AppendAllText(_testFile, $"{{\"b\":");
            await parser.ParseRecordsAsync(context, records, 100);
            Assert.Empty(records);

            File.AppendAllText(_testFile, $"\"{value}\"}}{Environment.NewLine}");
            await parser.ParseRecordsAsync(context, records, 100);
            Assert.Single(records);
            Assert.Equal(value, (string)records[0].Data["b"]);
        }
    }
}
