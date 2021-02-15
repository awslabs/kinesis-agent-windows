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
using System.Linq;
using System.IO;
using Xunit;
using Microsoft.Extensions.Logging.Abstractions;

namespace Amazon.KinesisTap.Core.Test
{
    public class RegexRecordParserTest
    {
        [Fact]
        public void TestPrintLog()
        {
            string log = @"[FATAL][2017/05/03 21:31:00.534][0x00003ca8][0000059c][][EQCASLicensingSubSystem][eqGetLicenseForSystemID][0] 'EQException.File: EQCASLicensingSubSystem.cpp'
[FATAL][2017/05/03 21:31:00.535][0x00003ca8][0000059c][][EQCASLicensingSubSystem][eqGetLicenseForSystemID][0] 'EQException.Line: 3999'";
            using (Stream stream = Utility.StringToStream(log))
            using (StreamReader sr = new StreamReader(stream))
            {
                RegexRecordParser parser = new RegexRecordParser(@"^\[\w+\]\[(?<TimeStamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]",
                    "yyyy/MM/dd HH:mm:ss.fff", null, null, null, DateTimeKind.Utc, new RegexRecordParserOptions());
                var records = parser.ParseRecords(sr, new LogContext()).ToList();

                Assert.Equal("[FATAL][2017/05/03 21:31:00.534][0x00003ca8][0000059c][][EQCASLicensingSubSystem][eqGetLicenseForSystemID][0] 'EQException.File: EQCASLicensingSubSystem.cpp'", records[0].GetMessage(null));
                Assert.Equal(new DateTime(2017, 5, 3, 21, 31, 0, 534), records[0].Timestamp);

                Assert.Equal("[FATAL][2017/05/03 21:31:00.535][0x00003ca8][0000059c][][EQCASLicensingSubSystem][eqGetLicenseForSystemID][0] 'EQException.Line: 3999'", records[1].GetMessage(null));
                Assert.Equal(new DateTime(2017, 5, 3, 21, 31, 0, 535), records[1].Timestamp);

                var envelope = (ILogEnvelope)records[1];
                Assert.Equal(2, envelope.LineNumber);
            }
        }

        [Fact]
        public void TestRegExtractor()
        {
            string log = @"[FATAL][2017/05/03 21:31:00.534][0x00003ca8][0000059c][][EQCASLicensingSubSystem][eqGetLicenseForSystemID][0] 'EQException.File: EQCASLicensingSubSystem.cpp'
[FATAL][2017/05/03 21:31:00.535][0x00003ca8][0000059c][][EQCASLicensingSubSystem][eqGetLicenseForSystemID][0] 'EQException.Line: 3999'";
            using (Stream stream = Utility.StringToStream(log))
            using (StreamReader sr = new StreamReader(stream))
            {
                string extractionPatterm = @"^\[(?<Severity>\w+)\]" +
                    @"\[(?<TimeStamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]" +
                    @"\[[^]]*\]" +
                    @"\[[^]]*\]" +
                    @"\[[^]]*\]" +
                    @"\[(?<SubSystem>\w+)\]" +
                    @"\[(?<Module>\w+)\]" +
                    @"\[[^]]*\]" +
                    @" '(?<Message>.*)'$";
                RegexRecordParser parser = new RegexRecordParser(@"^\[\w+\]\[(?<TimeStamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\]",
                    "yyyy/MM/dd HH:mm:ss.fff",
                    null,
                    extractionPatterm,
                    null,
                    DateTimeKind.Utc,
                    new RegexRecordParserOptions()
                );
                var records = parser.ParseRecords(sr, new LogContext()).ToList();

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
}
