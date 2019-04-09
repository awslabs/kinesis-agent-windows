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
using System.Text;
using Xunit;
using System.Linq;

namespace Amazon.KinesisTap.Core.Test
{
    public class SyslogParserTest
    {
        public static IEnumerable<object[]> SyslogTestData =>
            new List<object[]>
            {
                new object[] 
                {
                    @"Feb 26 07:19:14 ip-172-31-1-7 sshd[4367]: pam_unix(sshd:session): session opened for user ec2-user by (uid=0)",
                    new DateTime(DateTime.Now.Year, 2, 26, 7, 19, 14, DateTimeKind.Utc),
                    "ip-172-31-1-7",
                    "sshd[4367]:",
                    "pam_unix(sshd:session): session opened for user ec2-user by (uid=0)"
                },
                new object[] 
                {
                    @"Mar  1 01:22:20 ip-172-31-1-7 sshd[8320]: pam_unix(sshd:session): session opened for user ec2-user by (uid=0)",
                    new DateTime(DateTime.Now.Year, 3, 1, 1, 22, 20, DateTimeKind.Utc),
                    "ip-172-31-1-7",
                    "sshd[8320]:",
                    "pam_unix(sshd:session): session opened for user ec2-user by (uid=0)"
                }
            };

        [Theory]
        [MemberData(nameof(SyslogTestData))]
        public void TestSyslogParser(string logLine, DateTime expectedDateTime, string hostname, string program, string message)
        {
            using (Stream logStream = Utility.StringToStream(logLine))
            using (StreamReader logStreamReader = new StreamReader(logStream))
            {
                SysLogParser parser = new SysLogParser(null, DateTimeKind.Utc);
                var records = parser.ParseRecords(logStreamReader, new LogContext()).ToArray();
                Assert.NotNull(records);
                Assert.Single(records);

                var record = records[0];
                Assert.Equal(expectedDateTime, record.Timestamp);
                VerifySyslogDataItem(record, "Hostname", hostname);
                VerifySyslogDataItem(record, "Program", program);
                VerifySyslogDataItem(record, "Message", message);
            }
        }

        private void VerifySyslogDataItem(IEnvelope<IDictionary<string,string>> record, string dataItemName, string expectedValue)
        {
            Assert.True(record.Data.ContainsKey(dataItemName));
            Assert.Equal(expectedValue, record.Data[dataItemName]);
        }
    }
}
