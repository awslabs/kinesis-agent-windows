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
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    public class DelimitedLogParserTest
    {
        [Fact]
        public void TestDHCPLog()
        {
            string log = @"		Microsoft DHCP Service Activity Log


Event ID  Meaning
00	The log was started.
01	The log was stopped.
02	The log was temporarily paused due to low disk space.
10	A new IP address was leased to a client.
11	A lease was renewed by a client.
12	A lease was released by a client.
13	An IP address was found to be in use on the network.
14	A lease request could not be satisfied because the scope's address pool was exhausted.
15	A lease was denied.
16	A lease was deleted.
17	A lease was expired and DNS records for an expired leases have not been deleted.
18	A lease was expired and DNS records were deleted.
20	A BOOTP address was leased to a client.
21	A dynamic BOOTP address was leased to a client.
22	A BOOTP request could not be satisfied because the scope's address pool for BOOTP was exhausted.
23	A BOOTP IP address was deleted after checking to see it was not in use.
24	IP address cleanup operation has began.
25	IP address cleanup statistics.
30	DNS update request to the named DNS server.
31	DNS update failed.
32	DNS update successful.
33	Packet dropped due to NAP policy.
34	DNS update request failed.as the DNS update request queue limit exceeded.
35	DNS update request failed.
36	Packet dropped because the server is in failover standby role or the hash of the client ID does not match.
50+	Codes above 50 are used for Rogue Server Detection information.

QResult: 0: NoQuarantine, 1:Quarantine, 2:Drop Packet, 3:Probation,6:No Quarantine Information ProbationTime:Year-Month-Day Hour:Minute:Second:MilliSecond.

ID,Date,Time,Description,IP Address,Host Name,MAC Address,User Name, TransactionID, QResult,Probationtime, CorrelationID,Dhcid,VendorClass(Hex),VendorClass(ASCII),UserClass(Hex),UserClass(ASCII),RelayAgentInformation,DnsRegError.
24,09/29/17,00:00:04,Database Cleanup Begin,,,,,0,6,,,,,,,,,0
25,09/29/17,00:00:04,0 leases expired and 0 leases deleted,,,,,0,6,,,,,,,,,0
25,09/29/17,00:00:04,0 leases expired and 0 leases deleted,,,,,0,6,,,,,,,,,0";
            using (Stream stream = Utility.StringToStream(log))
            using (StreamReader sr = new StreamReader(stream))
            {
                var config = TestUtility.GetConfig("Sources", "DHCPParsed");
                var records = ParseRecords(sr, config);

                var record0 = records[0];
                Assert.Equal("24", record0.Data["ID"]);
                Assert.Equal(new DateTime(2017, 9, 29, 0, 0, 4), records[0].Timestamp);

                var record1 = records[1];
                Assert.Equal("25", record1.Data["ID"]);
                Assert.Equal("0 leases expired and 0 leases deleted", record1.Data["Description"]);

                Assert.Equal(36, ((ILogEnvelope)record1).LineNumber);
            }
        }

        [Fact]
        public void TestNPSLog()
        {
            string log = @"""NPS-MASTER"",""IAS"",03/22/2018,23:07:55,1,""demouser"",""demodomain\demouser"",,,,,,,,0,""10.62.86.137"",""Nate - Test 1"",,,,,,,1,,0,""311 1 10.1.0.213 03/15/2018 08:14:29 1"",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,""Use Windows authentication for all users"",1,,,,
""NPS-MASTER"",""IAS"",03/22/2018,23:07:55,3,,""demodomain\demouser"",,,,,,,,0,""10.62.86.137"",""Nate - Test 1"",,,,,,,1,,16,""311 1 10.1.0.213 03/15/2018 08:14:29 1"",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,""Use Windows authentication for all users"",1,,,,
""NPS-MASTER"",""IAS"",03/22/2018,23:08:38,1,""demouser"",""demodomain\demouser"",,,,,,,,0,""10.62.86.137"",""Nate - Test 1"",,,,,,,1,,0,""311 1 10.1.0.213 03/15/2018 08:14:29 2"",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,""Use Windows authentication for all users"",1,,,,";
            using (Stream stream = Utility.StringToStream(log))
            using (StreamReader sr = new StreamReader(stream))
            {
                var config = TestUtility.GetConfig("Sources", "NPS");
                var records = ParseRecords(sr, config);

                var record0 = records[0];
                Assert.Equal(new DateTime(2018, 3, 22, 23, 7, 55), records[0].Timestamp);
                Assert.Equal("NPS-MASTER", record0.Data["ComputerName"]);
                Assert.Equal("IAS", record0.Data["ServiceName"]);

                var record1 = records[1];
                Assert.Equal("demodomain\\demouser", record1.Data["Fully-Qualified-Distinguished-Name"]);
                Assert.Equal("10.62.86.137", record1.Data["Client-IP-Address"]);
            }
        }

        private static List<IEnvelope<DelimitedLogRecord>> ParseRecords(StreamReader sr, Microsoft.Extensions.Configuration.IConfigurationSection config)
        {
            string timetampFormat = config["TimestampFormat"];

            var parser = DirectorySourceFactory.CreateDelimitedLogParser(new PluginContext(config, null, null, new BookmarkManager()), timetampFormat, DateTimeKind.Utc);

            var records = parser.ParseRecords(sr, new DelimitedLogContext()).ToList();
            return records;
        }
    }
}
