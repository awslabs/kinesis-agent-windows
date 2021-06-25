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
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Amazon.KinesisTap.Filesystem.Test
{
    public class GenericDelimitedLogParserTest : IDisposable
    {
        private readonly string _testFile = Path.Combine(AppContext.BaseDirectory, Guid.NewGuid().ToString() + ".txt");

        private static readonly string[] _sampleLogs = new string[]
        {
            "line1 line1 line1 line1",
            "line2 line2 line2 line2",
            "line3 line3 line3 line3"
        };

        public void Dispose()
        {
            if (File.Exists(_testFile))
            {
                File.Delete(_testFile);
            }
        }

        [Fact]
        public async Task CancelledRead()
        {
            await File.WriteAllLinesAsync(_testFile, _sampleLogs);
            var records = new List<IEnvelope<KeyValueLogRecord>>();
            var parser = new GenericDelimitedLogParser(NullLogger.Instance, " ", new GenericDelimitedLogParserOptions
            {
                Headers = "h1 h2 h3 h4"
            });
            var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => parser.ParseRecordsAsync(new DelimitedTextLogContext
            {
                FilePath = _testFile
            }, records, int.MaxValue, cts.Token));
        }

        [Fact]
        public void RequireHeaderSpecification()
        {
            Assert.Throws<ArgumentException>(() => new GenericDelimitedLogParser(NullLogger.Instance, " ", new GenericDelimitedLogParserOptions()));
        }

        [Fact]
        public async Task RecognizeHeaderPattern()
        {
            var headers = "h1 h2 h3 h4";
            await File.WriteAllLinesAsync(_testFile, new string[] { headers });
            await File.AppendAllLinesAsync(_testFile, _sampleLogs);

            var records = new List<IEnvelope<KeyValueLogRecord>>();
            var parser = new GenericDelimitedLogParser(NullLogger.Instance, " ", new GenericDelimitedLogParserOptions
            {
                HeadersPattern = "^h1"
            });
            await parser.ParseRecordsAsync(new DelimitedTextLogContext
            {
                FilePath = _testFile
            }, records, int.MaxValue);

            AssertSampleRecords(records, headers);
        }

        [Fact]
        public async Task HeaderPatternExtraction()
        {
            var headers = "h1 h2 h3 h4";
            await File.WriteAllLinesAsync(_testFile, new string[] { $"Headers: {headers}" });
            await File.AppendAllLinesAsync(_testFile, _sampleLogs);

            var records = new List<IEnvelope<KeyValueLogRecord>>();
            var parser = new GenericDelimitedLogParser(NullLogger.Instance, " ", new GenericDelimitedLogParserOptions
            {
                HeadersPattern = "^Headers: (?<Headers>.+)$"
            });
            await parser.ParseRecordsAsync(new DelimitedTextLogContext
            {
                FilePath = _testFile
            }, records, int.MaxValue);

            AssertSampleRecords(records, headers);
        }

        [Fact]
        public async Task CsvModeDisabled()
        {
            await File.WriteAllLinesAsync(_testFile, new string[] { "value1,\"value2\",value\",3" });
            var records = new List<IEnvelope<KeyValueLogRecord>>();
            var parser = new GenericDelimitedLogParser(NullLogger.Instance, ",", new GenericDelimitedLogParserOptions
            {
                Headers = "h1,h2,h3,h4",
                CSVEscapeMode = false
            });
            await parser.ParseRecordsAsync(new DelimitedTextLogContext
            {
                FilePath = _testFile,
            }, records, int.MaxValue);

            var record = records.Single();
            Assert.Equal("value1", record.Data["h1"]);
            Assert.Equal("\"value2\"", record.Data["h2"]);
            Assert.Equal("value\"", record.Data["h3"]);
            Assert.Equal("3", record.Data["h4"]);
        }

        private static void AssertSampleRecords(List<IEnvelope<KeyValueLogRecord>> records, string headers)
        {
            var headerFields = headers.Split(' ');
            for (var i = 0; i < _sampleLogs.Length; i++)
            {
                var values = _sampleLogs[i].Split(' ');
                for (var j = 0; j < values.Length; j++)
                {
                    Assert.Equal(values[j], records[i].Data[headerFields[j]]);
                }
            }
        }

        [Fact]
        public async Task ParseDHCPSampleLog()
        {
            var parserOptions = new GenericDelimitedLogParserOptions
            {
                CSVEscapeMode = false,
                TimestampField = "{Date} {Time}",
                TimestampFormat = "MM/dd/yy HH:mm:ss",
                RecordPattern = "^\\d{2},\\d{2}/\\d{2}/\\d{2},\\d{2}:\\d{2}:\\d{2}.*",
                HeadersPattern = "^ID.*",
            };
            var records = new List<IEnvelope<KeyValueLogRecord>>();
            var parser = new GenericDelimitedLogParser(NullLogger.Instance, ",", parserOptions);
            await parser.ParseRecordsAsync(new DelimitedTextLogContext
            {
                FilePath = Path.Combine(AppContext.BaseDirectory, "Samples/DHCPSample.log")
            }, records, 1000);

            Assert.Equal(3, records.Count);

            var record0 = records[0];
            Assert.Equal("24", record0.Data["ID"]);
            Assert.Equal(new DateTime(2017, 9, 29, 0, 0, 4), records[0].Timestamp);

            var record1 = records[1];
            Assert.Equal("25", record1.Data["ID"]);
            Assert.Equal("0 leases expired and 0 leases deleted", record1.Data["Description"]);

            Assert.Equal(36, ((ILogEnvelope)record1).LineNumber);
        }

        [Fact]
        public async Task ParseNPSSampleLog()
        {
            var parserOptions = new GenericDelimitedLogParserOptions
            {
                CSVEscapeMode = false,
                TimestampField = "{Record-Date} {Record-Time}",
                TimestampFormat = "MM/dd/yyyy HH:mm:ss",
                Headers = "ComputerName,ServiceName,Record-Date,Record-Time,Packet-Type,User-Name,Fully-Qualified-Distinguished-Name,Called-Station-ID,Calling-Station-ID,Callback-Number,Framed-IP-Address,NAS-Identifier,NAS-IP-Address,NAS-Port,Client-Vendor,Client-IP-Address,Client-Friendly-Name,Event-Timestamp,Port-Limit,NAS-Port-Type,Connect-Info,Framed-Protocol,Service-Type,Authentication-Type,Policy-Name,Reason-Code,Class,Session-Timeout,Idle-Timeout,Termination-Action,EAP-Friendly-Name,Acct-Status-Type,Acct-Delay-Time,Acct-Input-Octets,Acct-Output-Octets,Acct-Session-Id,Acct-Authentic,Acct-Session-Time,Acct-Input-Packets,Acct-Output-Packets,Acct-Terminate-Cause,Acct-Multi-Ssn-ID,Acct-Link-Count,Acct-Interim-Interval,Tunnel-Type,Tunnel-Medium-Type,Tunnel-Client-Endpt,Tunnel-Server-Endpt,Acct-Tunnel-Conn,Tunnel-Pvt-Group-ID,Tunnel-Assignment-ID,Tunnel-Preference,MS-Acct-Auth-Type,MS-Acct-EAP-Type,MS-RAS-Version,MS-RAS-Vendor,MS-CHAP-Error,MS-CHAP-Domain,MS-MPPE-Encryption-Types,MS-MPPE-Encryption-Policy,Proxy-Policy-Name,Provider-Type,Provider-Name,Remote-Server-Address,MS-RAS-Client-Name,MS-RAS-Client-Version"
            };
            var records = new List<IEnvelope<KeyValueLogRecord>>();
            var parser = new GenericDelimitedLogParser(NullLogger.Instance, ",", parserOptions);

            await parser.ParseRecordsAsync(new DelimitedTextLogContext
            {
                FilePath = Path.Combine(AppContext.BaseDirectory, "Samples/NPSSample.log")
            }, records, int.MaxValue);
            Assert.Equal(20, records.Count);

            foreach (var record in records)
            {
                Assert.Equal("\"NPS-MASTER\"", record.Data["ComputerName"]);
                Assert.Equal("\"IAS\"", record.Data["ServiceName"]);
                Assert.Equal(2018, record.Timestamp.Year);
            }
        }
    }
}
