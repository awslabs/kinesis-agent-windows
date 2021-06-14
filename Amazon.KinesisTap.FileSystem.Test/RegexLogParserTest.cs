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
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Amazon.KinesisTap.Filesystem.Test
{
    public class RegexLogParserTest : IDisposable
    {
        private readonly string _testFile = Path.Combine(AppContext.BaseDirectory, Guid.NewGuid().ToString() + ".txt");

        private static readonly string[] _printLogs = new string[]
        {
            "[FATAL][2017/05/03 21:31:00.534][0x00003ca8][0000059c][][EQCASLicensingSubSystem][eqGetLicenseForSystemID][0] 'EQException.File: EQCASLicensingSubSystem.cpp'",
            "[FATAL][2017/05/03 21:31:00.535][0x00003ca8][0000059c][][EQCASLicensingSubSystem][eqGetLicenseForSystemID][0] 'EQException.Line: 3999'"
        };

        public void Dispose()
        {
            if (File.Exists(_testFile))
            {
                File.Delete(_testFile);
            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(100)]
        public async Task ParseMultiLinesRecords(int recordCount)
        {
            var recordLines = new List<string>();
            var noOfLines = 0;
            for (var i = 0; i < recordCount; i++)
            {
                // vary the # of lines from 0-9
                noOfLines = (noOfLines + 1) % 10;
                recordLines.Add($"Header: Record {i}");
                for (var j = 0; j < noOfLines; j++)
                {
                    recordLines.Add($"Line {j}");
                }
            }

            await File.WriteAllLinesAsync(_testFile, recordLines);
            var records = new List<IEnvelope<IDictionary<string, string>>>();
            var regexTextParser = new RegexLogParser(NullLogger.Instance, "^Header:", new RegexParserOptions
            {
                TimeZoneKind = DateTimeKind.Utc
            }, null, 1024);

            await regexTextParser.ParseRecordsAsync(new RegexLogContext
            {
                FilePath = _testFile
            }, records, recordCount * 2);

            Assert.Equal(recordCount, records.Count);
        }

        [Theory]
        [InlineData("yyyy/MM/dd HH:mm:ss.fff")]
        [InlineData(null)]
        public async Task TimestampOnSecondLine(string timestampFormat)
        {
            var timestamp = DateTime.Now.ToString(timestampFormat, CultureInfo.InvariantCulture);
            var lines = new string[]
            {
                "Header: Record",
                $"Time: {timestamp}"
            };
            await File.WriteAllLinesAsync(_testFile, lines);

            var records = new List<IEnvelope<IDictionary<string, string>>>();
            var regexTextParser = new RegexLogParser(NullLogger.Instance, "^Header:", new RegexParserOptions
            {
                ExtractionPattern = $@"^Header: (?<Title>.*)({Environment.NewLine})Time: (?<Timestamp>.*)$",
                TimeZoneKind = DateTimeKind.Local,
                TimestampFormat = timestampFormat
            }, null, 1024);

            await regexTextParser.ParseRecordsAsync(new RegexLogContext
            {
                FilePath = _testFile
            }, records, 100);

            Assert.Single(records);
            var record = records[0];
            Assert.Equal("Record", record.Data["Title"]);

            Assert.Equal(DateTime.Parse(timestamp, CultureInfo.InvariantCulture), records[0].Timestamp);
        }

        [Fact]
        public async Task PrintLog()
        {
            await File.WriteAllLinesAsync(_testFile, _printLogs);

            var regexTextParser = new RegexLogParser(NullLogger.Instance, @"^\[\w+\]\[(?<TimeStamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]", new RegexParserOptions
            {
                TimestampFormat = "yyyy/MM/dd HH:mm:ss.fff",
                TimeZoneKind = DateTimeKind.Utc
            }, null, 1024);

            var records = new List<IEnvelope<IDictionary<string, string>>>();

            await regexTextParser.ParseRecordsAsync(new RegexLogContext
            {
                FilePath = _testFile
            }, records, 100);
            Assert.Equal("[FATAL][2017/05/03 21:31:00.534][0x00003ca8][0000059c][][EQCASLicensingSubSystem][eqGetLicenseForSystemID][0] 'EQException.File: EQCASLicensingSubSystem.cpp'", records[0].GetMessage(null));
            Assert.Equal(new DateTime(2017, 5, 3, 21, 31, 0, 534, DateTimeKind.Utc), records[0].Timestamp);

            Assert.Equal("[FATAL][2017/05/03 21:31:00.535][0x00003ca8][0000059c][][EQCASLicensingSubSystem][eqGetLicenseForSystemID][0] 'EQException.Line: 3999'", records[1].GetMessage(null));
            Assert.Equal(new DateTime(2017, 5, 3, 21, 31, 0, 535, DateTimeKind.Utc), records[1].Timestamp);

            var envelope = (ILogEnvelope)records[1];
            Assert.Equal(2, envelope.LineNumber);
        }

        [Fact]
        public async Task PrintLog_RegExtractor()
        {
            await File.WriteAllLinesAsync(_testFile, _printLogs);
            var xtractPattern = @"^\[(?<Severity>\w+)\]" +
                    @"\[(?<TimeStamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]" +
                    @"\[[^]]*\]" +
                    @"\[[^]]*\]" +
                    @"\[[^]]*\]" +
                    @"\[(?<SubSystem>\w+)\]" +
                    @"\[(?<Module>\w+)\]" +
                    @"\[[^]]*\]" +
                    @" '(?<Message>.*)'$";
            var regexTextParser = new RegexLogParser(NullLogger.Instance, @"^\[\w+\]\[(?<TimeStamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]", new RegexParserOptions
            {
                ExtractionPattern = xtractPattern,
                TimeZoneKind = DateTimeKind.Utc,
                TimestampFormat = "yyyy/MM/dd HH:mm:ss.fff"
            }, null, 1024);
            var records = new List<IEnvelope<IDictionary<string, string>>>();

            await regexTextParser.ParseRecordsAsync(new RegexLogContext
            {
                FilePath = _testFile
            }, records, 100);

            Assert.Equal("FATAL", records[0].Data["Severity"]);
            Assert.Equal("2017/05/03 21:31:00.534", records[0].Data["TimeStamp"]);
            Assert.Equal("EQCASLicensingSubSystem", records[0].Data["SubSystem"]);
            Assert.Equal("eqGetLicenseForSystemID", records[0].Data["Module"]);
            Assert.Equal("EQException.File: EQCASLicensingSubSystem.cpp", records[0].Data["Message"]);

            Assert.Equal("2017/05/03 21:31:00.535", records[1].Data["TimeStamp"]);
            Assert.Equal("EQException.Line: 3999", records[1].Data["Message"]);

            var envelope = (ILogEnvelope)records[1];
            Assert.Equal(2, envelope.LineNumber);
        }
    }
}
