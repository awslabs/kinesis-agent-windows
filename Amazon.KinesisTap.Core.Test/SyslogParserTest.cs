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
using Xunit;
using System.Linq;
using Microsoft.Extensions.Logging.Abstractions;

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
                    new DateTime(DateTime.Now.Year, 2, 26, 7, 19, 14, DateTimeKind.Local),
                    "ip-172-31-1-7",
                    "sshd",
                    "pam_unix(sshd:session): session opened for user ec2-user by (uid=0)",
                    "Feb 26 07:19:14"
                },
                new object[]
                {
                    @"Mar  1 01:22:20 ip-172-31-1-7 sshd[8320]: pam_unix(sshd:session): session opened for user ec2-user by (uid=0)",
                    new DateTime(DateTime.Now.Year, 3, 1, 1, 22, 20, DateTimeKind.Local),
                    "ip-172-31-1-7",
                    "sshd",
                    "pam_unix(sshd:session): session opened for user ec2-user by (uid=0)",
                    "Mar 01 01:22:20"
                },
                new object[]
                {
                    @"Mar 12 12:01:02 server4 snort: alert_multiple_requests: ACTIVE",
                    new DateTime(DateTime.Now.Year, 3, 12, 12, 1, 2, DateTimeKind.Local),
                    "server4",
                    "snort",
                    "alert_multiple_requests: ACTIVE",
                    "Mar 12 12:01:02"
                },
                new object[]
                {
                    // log with ISO 8601 timestamp
                    @"2010-04-20T14:17:40+00:00 netsec-scanner-corp-pdx-62001 sudo: webuser : (command continued)",
                    new DateTime(2010, 4, 20, 14, 17, 40, DateTimeKind.Utc),
                    "netsec-scanner-corp-pdx-62001",
                    "sudo",
                    "webuser : (command continued)",
                    "Apr 20 14:17:40"
                }
            };

        [Theory]
        [MemberData(nameof(SyslogTestData))]
        public void TestSyslogParser(string logLine, DateTime expectedDateTime, string hostname, string program, string message, string syslogTimestamp)
        {
            using (Stream logStream = Utility.StringToStream(logLine))
            using (StreamReader logStreamReader = new StreamReader(logStream))
            {
                SyslogParser parser = new SyslogParser(NullLogger.Instance, false);
                var records = parser.ParseRecords(logStreamReader, new LogContext()).ToArray();
                Assert.NotNull(records);
                Assert.Single(records);

                var record = records[0];

                // make sure the Timestamp of the envelope is in universal time 
                Assert.Equal(expectedDateTime.ToUniversalTime(), record.Timestamp);

                // assert extracted syslog data
                Assert.Equal(hostname, record.Data.Hostname);
                Assert.Equal(program, record.Data.Program);
                Assert.Equal(message, record.Data.Message);
                Assert.Equal(syslogTimestamp, record.Data.SyslogTimestamp);
            }
        }
    }
}
