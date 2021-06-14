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
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Amazon.KinesisTap.Filesystem.Test
{
    public class TimestampParserTest : IDisposable
    {
        private readonly string _testFile = Path.Combine(AppContext.BaseDirectory, Guid.NewGuid().ToString() + ".log");

        public void Dispose()
        {
            if (File.Exists(_testFile))
            {
                File.Delete(_testFile);
            }
        }

        [Fact]
        public void TimestampFormatIsRequired()
        {
            Assert.Throws<ArgumentNullException>(() => new TimestampLogParser(NullLogger.Instance, new RegexParserOptions(), null, 1024));
        }

        [Fact]
        public async Task TestTimestampLog()
        {
            var expectedTime1 = new DateTime(2017, 5, 18, 0, 0, 28, DateTimeKind.Utc).AddTicks(665000);
            var expectedTime2 = new DateTime(2017, 5, 18, 0, 0, 56, DateTimeKind.Utc).AddTicks(8128000);

            await TestTimestampLogInternal(expectedTime1, expectedTime2, DateTimeKind.Utc);
        }

        [Theory]
        [InlineData(DateTimeKind.Local)]
        [InlineData(DateTimeKind.Utc)]
        public async Task TestSSMLog(DateTimeKind dateTimeKind)
        {
            var logs = new string[]
            {
                "2017-03-31 10:06:20 ERROR [instanceID=i-0ad46e850f00b20ba] [MessageProcessor] error when calling AWS APIs. error details - GetMessages Error: AccessDeniedException: User: arn:aws:sts::266928793956:assumed-role/ds2amazonli/i-0ad46e850f00b20ba is not authorized to perform: ec2messages:GetMessages on resource: *",
                "status code: 400, request id: afe4d9d7-15f9-11e7-8eb7-193e481a50d1",
                "2017-03-31 10:06:21 INFO [instanceID=i-0ad46e850f00b20ba] [MessageProcessor] increasing error count by 1"
            };
            await File.WriteAllLinesAsync(_testFile, logs);
            var records = new List<IEnvelope<IDictionary<string, string>>>();
            var parser = new TimestampLogParser(NullLogger.Instance, new RegexParserOptions
            {
                TimestampFormat = "yyyy-MM-dd HH:mm:ss",
                TimeZoneKind = dateTimeKind
            }, null, 1024);
            await parser.ParseRecordsAsync(new RegexLogContext { FilePath = _testFile }, records, 100);
            Assert.Equal("2017-03-31 10:06:20 ERROR [instanceID=i-0ad46e850f00b20ba] [MessageProcessor] error when calling AWS APIs. error details - GetMessages Error: AccessDeniedException: User: arn:aws:sts::266928793956:assumed-role/ds2amazonli/i-0ad46e850f00b20ba is not authorized to perform: ec2messages:GetMessages on resource: *" +
                    Environment.NewLine + "status code: 400, request id: afe4d9d7-15f9-11e7-8eb7-193e481a50d1", records[0].GetMessage(null));

            var timestamp0 = new DateTime(2017, 3, 31, 10, 6, 20, dateTimeKind);
            Assert.Equal(timestamp0, records[0].Timestamp);
            Assert.Equal(timestamp0.Kind, records[0].Timestamp.Kind);

            Assert.Equal("2017-03-31 10:06:21 INFO [instanceID=i-0ad46e850f00b20ba] [MessageProcessor] increasing error count by 1", records[1].GetMessage(null));
            var timestamp1 = new DateTime(2017, 3, 31, 10, 6, 21, dateTimeKind);
            Assert.Equal(timestamp1, records[1].Timestamp);
            Assert.Equal(timestamp1.Kind, records[1].Timestamp.Kind);
        }

        [Fact]
        public async Task TestTimeZoneConversion()
        {
            var expectedTime1 = new DateTime(2017, 5, 18, 0, 0, 28, DateTimeKind.Utc).AddTicks(665000);
            var expectedTime2 = new DateTime(2017, 5, 18, 0, 0, 56, DateTimeKind.Utc).AddTicks(8128000);

            await TestTimestampLogInternal(expectedTime1, expectedTime2, DateTimeKind.Utc);
        }

        private async Task TestTimestampLogInternal(DateTime expectedTime1, DateTime expectedTime2, DateTimeKind timeZoneKind)
        {
            var logs = new string[]
            {
                "2017-05-18 00:00:28.0665 Quartz.Core.QuartzSchedulerThread DEBUG Batch acquisition of 0 triggers",
                "2017-05-18 00:00:56.8128 Quartz.Core.QuartzSchedulerThread DEBUG Batch acquisition of 0 triggers"
            };

            await File.WriteAllLinesAsync(_testFile, logs);
            var records = new List<IEnvelope<IDictionary<string, string>>>();
            var parser = new TimestampLogParser(NullLogger.Instance, new RegexParserOptions
            {
                TimestampFormat = "yyyy-MM-dd HH:mm:ss.ffff",
                TimeZoneKind = timeZoneKind
            }, null, 1024);
            await parser.ParseRecordsAsync(new RegexLogContext { FilePath = _testFile }, records, 100);

            Assert.Equal("2017-05-18 00:00:28.0665 Quartz.Core.QuartzSchedulerThread DEBUG Batch acquisition of 0 triggers", records[0].GetMessage(null));
            Assert.Equal(expectedTime1, records[0].Timestamp);

            Assert.Equal("2017-05-18 00:00:56.8128 Quartz.Core.QuartzSchedulerThread DEBUG Batch acquisition of 0 triggers", records[1].GetMessage(null));
            Assert.Equal(expectedTime2, records[1].Timestamp);
        }

        [Fact]
        public async Task TestSQLLog()
        {
            var log = @"2019-02-13 04:50:33.89 Server      Microsoft SQL Server 2014 (SP2-GDR) (KB4057120) - 12.0.5214.6 (X64)
                Jan  9 2018 15:03:12
                Copyright (c) Microsoft Corporation
                Enterprise Edition (64-bit) on Windows NT 6.3 <X64> (Build 9600: ) (Hypervisor)
2019-02-13 04:50:34.00 Server      UTC adjustment: 0:00";

            await File.WriteAllLinesAsync(_testFile, new string[] { log });
            var records = new List<IEnvelope<IDictionary<string, string>>>();
            var parser = new TimestampLogParser(NullLogger.Instance, new RegexParserOptions
            {
                TimestampFormat = "yyyy-MM-dd HH:mm:ss.ff",
                TimeZoneKind = DateTimeKind.Utc,
                ExtractionPattern = "^\\s*(?<TimeStamp>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{2})\\s(?<Source>\\w+)\\s+(?<Message>.*)$",
                ExtractionRegexOptions = nameof(RegexOptions.Singleline)
            }, null, 1024);
            await parser.ParseRecordsAsync(new RegexLogContext { FilePath = _testFile }, records, 100);
            Assert.Equal(2, records.Count);

            Assert.Equal("2019-02-13 04:50:33.89", records[0].Data["TimeStamp"]);
            Assert.Equal("Server", records[0].Data["Source"]);
            Assert.True(records[0].Data["Message"].Length > 0);
        }
    }
}
