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
using System.IO;
using System.Text;
using Xunit;

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
                    "yyyy/MM/dd HH:mm:ss.fff", null, null, DateTimeKind.Utc, new RegexRecordParserOptions());
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

        [Fact]
        public void TestSysLog()
        {
            DateTime expectedTime = new DateTime(DateTime.Now.Year, 3, 12, 12, 0, 8);
            DateTimeKind timeZoneKind = DateTimeKind.Utc;

            TestSysLogInternal(expectedTime, timeZoneKind);
        }

        [Fact]
        public void TestTimeZoneConversion()
        {
            DateTime expectedTime = new DateTime(DateTime.Now.Year, 3, 12, 12, 0, 8).ToUniversalTime();
            DateTimeKind timeZoneKind = DateTimeKind.Local;

            TestSysLogInternal(expectedTime, timeZoneKind);
        }

        private static void TestSysLogInternal(DateTime expectedTime, DateTimeKind timeZoneKind)
        {
            string log = @"Mar 12 12:00:08 server2 rcd[308]: Unable to downloaded licenses info: Unable to authenticate - Authorization Required (https://server2/data/red-carpet.rdf)
Mar 12 12:10:00 server2 /USR/SBIN/CRON[6808]: (root) CMD ( /usr/lib/sa/sa1 )
Mar 7 04:22:00 avas CROND[11460]: (cronjob) CMD (run-parts /etc/cron.weekly)
Mar 7 04:22:00 avas anacron[11464]: Updated timestamp for job `cron.weekly' to 2004-03-07
Mar 12 12:01:02 server4 snort: alert_multiple_requests: ACTIVE
Mar 12 12:17:03 server7 sshd[26501]: pam_authenticate: error Authentication failed";
            using (Stream stream = Utility.StringToStream(log))
            using (StreamReader sr = new StreamReader(stream))
            {
                SysLogParser parser = new SysLogParser(null, timeZoneKind);
                var records = parser.ParseRecords(sr, new LogContext()).ToList();

                Assert.Equal(expectedTime, records[0].Timestamp);
                Assert.Equal("Mar 12 12:00:08", records[0].Data["SysLogTimeStamp"]);
                Assert.Equal("server2", records[0].Data["Hostname"]);
                Assert.Equal("rcd[308]:", records[0].Data["Program"]);
                Assert.Equal("Unable to downloaded licenses info: Unable to authenticate - Authorization Required (https://server2/data/red-carpet.rdf)", records[0].Data["Message"]);

                var envelope = (ILogEnvelope)records[5];
                Assert.Equal(6, envelope.LineNumber);
            }
        }
    }
}
