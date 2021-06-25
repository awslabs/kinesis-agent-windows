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
using System.IO;
using Xunit;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging.Abstractions;
using System.Threading.Tasks;
using Amazon.KinesisTap.Filesystem;
using System.Collections.Generic;
using System.Linq;

namespace Amazon.KinesisTap.Uls.Test
{
    public class ULSLogParserTest : IDisposable
    {
        private readonly string _testFile = Path.Combine(AppContext.BaseDirectory, Guid.NewGuid().ToString() + ".txt");

        public void Dispose()
        {
            if (File.Exists(_testFile))
            {
                File.Delete(_testFile);
            }
        }

        /// <summary>
        /// Make sure record keys and values are trimmed
        /// </summary>
        [Fact]
        public async Task KeysAndValuesAreTrimmed()
        {
            var logs = new string[]
            {
                "Timestamp\t field1\tfield2 \t field3 ",
                $"{DateTime.Now} \t value1\t value2 \t value3 "
            };

            await File.WriteAllLinesAsync(_testFile, logs);
            var parser = new AsyncULSLogParser(NullLogger.Instance, null, 1024);
            var records = new List<IEnvelope<KeyValueLogRecord>>();
            await parser.ParseRecordsAsync(new DelimitedTextLogContext
            {
                FilePath = _testFile
            }, records, int.MaxValue);

            Assert.Single(records);
            var record = records[0];
            Assert.True(record.Data.Keys.All(k => !k.StartsWith(" ") && !k.EndsWith(" ")));
            Assert.True(record.Data.Values.All(v => !v.StartsWith(" ") && !v.EndsWith(" ")));
        }

        [Theory]
        [InlineData("Samples/SharepointULSSample_Timestamp.log", 17)]
        [InlineData("Samples/SharepointULSSample_TimestampUtc.log", 20)]
        public async Task ParseSharepointLogs(string fileName, int recordCount)
        {
            var parser = new AsyncULSLogParser(NullLogger.Instance, null, 1024);
            var records = new List<IEnvelope<KeyValueLogRecord>>();
            await parser.ParseRecordsAsync(new DelimitedTextLogContext
            {
                FilePath = Path.Combine(AppContext.BaseDirectory, fileName)
            }, records, int.MaxValue);

            Assert.Equal(recordCount, records.Count);
        }

        [Theory]
        [InlineData("Timestamp")]
        [InlineData("TimestampUtc")]
        public async Task RemoveStartFromTimestamp(string timestampField)
        {
            var logs = new string[]
            {
                $"{timestampField}\t field1\tfield2 \t field3 ",
                $"{DateTime.Now}* \t value1\t value2 \t value3 "
            };

            await File.WriteAllLinesAsync(_testFile, logs);
            var parser = new AsyncULSLogParser(NullLogger.Instance, null, 1024);
            var records = new List<IEnvelope<KeyValueLogRecord>>();
            await parser.ParseRecordsAsync(new DelimitedTextLogContext
            {
                FilePath = _testFile
            }, records, int.MaxValue);

            Assert.Single(records);
            var record = records[0];
            Assert.DoesNotContain("*", record.Data[timestampField]);
        }

        [Fact]
        public async Task TestWssLogRecords()
        {
            string log = "Timestamp              \tProcess                                 \tTID   \tArea                          \tCategory                      \tEventID\tLevel     \tMessage \tCorrelation" + Environment.NewLine +
"03/07/2018 22:54:54.97 \twsstracing.exe (0x08C4)                 \t0x1520\tSharePoint Foundation         \tTracing Controller Service    \t8094\tWarning \tTrace logs are reaching to the configured storage limit (5). Please increase the maximum storage settings. Otherwise, older files will be deleted once the limit is reached.	\t" + Environment.NewLine +
"03/07/2018 22:54:54.97* \twsstracing.exe (0x08C4)                 \t0x1520\tSharePoint Foundation         \tUnified Logging Service       \tb9wt\tHigh    \tLog retention limit reached.  Log file 'E:\\ULS\\EC2AMAZ-O66EQR2-20180305-2111.log' has been deleted.	\t" + Environment.NewLine;

            await File.WriteAllTextAsync(_testFile, log);
            await TestWssLogsWithContext(new DelimitedTextLogContext
            {
                FilePath = _testFile
            });

            //Retest for the case we start with position > 0
            long position = 0;
            using var fs = File.OpenRead(_testFile);
            using var reader = new LineReader(fs);
            // read past the header line
            var (_, consumed) = await reader.ReadAsync();
            position += consumed;

            await TestWssLogsWithContext(new DelimitedTextLogContext
            {
                FilePath = _testFile,
                Position = position,
                LineNumber = 1
            });
        }

        private static async Task TestWssLogsWithContext(DelimitedTextLogContext context)
        {
            var parser = new AsyncULSLogParser(NullLogger.Instance, null, 1024);
            var output = new List<IEnvelope<KeyValueLogRecord>>();

            await parser.ParseRecordsAsync(context, output, 10);
            Assert.Equal(2, output.Count);
            var record = output[0].Data;
            _ = output[0].GetMessage("json");
            Assert.Equal("03/07/2018 22:54:54.97", record["Timestamp"]);
            Assert.Equal(new DateTime(2018, 3, 7, 22, 54, 54, 970), record.Timestamp);
            Assert.Equal("SharePoint Foundation", record["Area"]);
            Assert.Equal("Warning", record["Level"]);

            var envelope = (ILogEnvelope)output[0];
            Assert.Equal(2, envelope.LineNumber);

            Assert.Equal("03/07/2018 22:54:54.97", output[1].Data["Timestamp"]);
        }
    }
}
