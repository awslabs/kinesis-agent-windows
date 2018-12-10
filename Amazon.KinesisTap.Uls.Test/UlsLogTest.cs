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
using System.Linq;

namespace Amazon.KinesisTap.Uls.Test
{
    public class UlsLogTest
    {
        [Fact]
        public void TestUlsLogRecord()
        {
            string log = "Timestamp              \tProcess                                 \tTID   \tArea                          \tCategory                      \tEventID\tLevel     \tMessage \tCorrelation" + Environment.NewLine +
"03/07/2018 22:54:54.97 \twsstracing.exe (0x08C4)                 \t0x1520\tSharePoint Foundation         \tTracing Controller Service    \t8094\tWarning \tTrace logs are reaching to the configured storage limit (5). Please increase the maximum storage settings. Otherwise, older files will be deleted once the limit is reached.	\t" + Environment.NewLine + 
"03/07/2018 22:54:54.97* \twsstracing.exe (0x08C4)                 \t0x1520\tSharePoint Foundation         \tUnified Logging Service       \tb9wt\tHigh    \tLog retention limit reached.  Log file 'E:\\ULS\\EC2AMAZ-O66EQR2-20180305-2111.log' has been deleted.	\t" + Environment.NewLine;
            using (var sr = new StreamReader(Utility.StringToStream(log)))
            {
                TestUlsLogParser(sr, 0);
            }

            //Retest for the case we start with position > 0
            long position = 0;
            using (var sr = new StreamReader(Utility.StringToStream(log)))
            {
                //Consume one line to set the file pointer passing the headerline
                sr.ReadLine();
                position = sr.BaseStream.Position;
            }

            //Test if the parser can still pick up the metadata if position > 0
            using (var sr = new StreamReader(Utility.StringToStream(log)))
            {
                TestUlsLogParser(sr, position);
            }

        }

        private static void TestUlsLogParser(StreamReader sr, long position)
        {
            var parser = new UlsLogParser();
            var records = parser.ParseRecords(sr,
                new DelimitedLogContext() { FilePath = "Memory", Position = position })
                .ToList();
            Assert.Equal(2, records.Count);
            var record = records[0].Data;
            string json = records[0].GetMessage("json");
            Assert.Equal("03/07/2018 22:54:54.97", record["Timestamp"]);
            Assert.Equal(new DateTime(2018, 3, 7, 22, 54, 54, 970), record.TimeStamp);
            Assert.Equal("SharePoint Foundation", record["Area"]);
            Assert.Equal("Warning", record["Level"]);

            var envelope = (ILogEnvelope)records[0];
            Assert.Equal(2, envelope.LineNumber);

            Assert.Equal("03/07/2018 22:54:54.97", records[1].Data["Timestamp"]);
        }
    }
}
