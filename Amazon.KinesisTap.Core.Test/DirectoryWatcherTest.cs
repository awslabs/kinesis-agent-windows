using System;
using System.Linq;
using Xunit;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging;
using System.IO;
using System.Threading;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Configuration;

namespace Amazon.KinesisTap.Core.Test
{
    public class DirectoryWatcherTest
    {
#region public members
        [Fact]
        public void StartStopTest()
        {
            string filter = "*.*";
            using (MemoryLogger logger = new MemoryLogger("memory"))
            {
                DirectorySource<string, LogContext> watcher = new DirectorySource<string, LogContext>(
                    TestUtility.GetTestHome(), 
                    filter, 
                    1000,
                    new PluginContext(null, logger, null), 
                    new SingeLineRecordParser(), 
                    DirectorySourceFactory.CreateLogSourceInfo);
                watcher.Start();
                Assert.Equal($"DirectorySource id {null} watching directory {TestUtility.GetTestHome()} with filter {filter} started.", logger.LastEntry);
                watcher.Stop();
                Assert.Equal($"DirectorySource id {null} watching directory {TestUtility.GetTestHome()} with filter {filter} stopped.", logger.LastEntry);
            }
        }

        [Fact]
        public void NullFileNameFilterTest()
        {
            using (MemoryLogger logger = new MemoryLogger("memory"))
            {
                DirectorySource<string, LogContext> watcher = new DirectorySource<string, LogContext>(
                    TestUtility.GetTestHome(),
                    null,
                    1000,
                    new PluginContext(null, logger, null),
                    new SingeLineRecordParser(),
                    DirectorySourceFactory.CreateLogSourceInfo);
                watcher.Start();
                Assert.Equal($"DirectorySource id {null} watching directory {TestUtility.GetTestHome()} with filter  started.", logger.LastEntry);
                watcher.Stop();
                Assert.Equal($"DirectorySource id {null} watching directory {TestUtility.GetTestHome()} with filter  stopped.", logger.LastEntry);
            }
        }


        /// <summary>
        /// Write to a single log file. New or existing.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task SingleLogTest()
        {
            string filter = "KinesisTapTest????????.log";
            string logFileNameFormat = "KinesisTapTest{0:yyyyMMdd}.log";

            await CreateAndRunWatcher(filter, async (logRecords) =>
            {
                int accumulatedRecords = 0;
                //Run one batch of test
                string filePath = GetFileNameFromTimeStamp(logFileNameFormat);
                accumulatedRecords = await RunDirectoryWatcherTest(filePath, logRecords, accumulatedRecords);
                //Run another batch of test with the same file name
                accumulatedRecords = await RunDirectoryWatcherTest(filePath, logRecords, accumulatedRecords);
            });
        }

        /// <summary>
        /// File name contains time stamp. Only write to the new file name. Never rename file.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task FileNameContainingDateTimeStampTest()
        {
            string filter = "KinesisTapDateTimeStamp??????????????.log";
            string logFileNameFormat = "KinesisTapDateTimeStamp{0:yyyyMMddhhmmss}.log";

            await CreateAndRunWatcher(filter, async (logRecords) =>
            {
                int accumulatedRecords = 0;
                //Run one batch of test
                string filePath = GetFileNameFromTimeStamp(logFileNameFormat);
                accumulatedRecords = await RunDirectoryWatcherTest(filePath, logRecords, accumulatedRecords);
                //Run another batch of test with a different file name
                filePath = GetFileNameFromTimeStamp(logFileNameFormat);
                accumulatedRecords = await RunDirectoryWatcherTest(filePath, logRecords, accumulatedRecords);
            });
        }

        /// <summary>
        /// xyz.log
        /// xyz.log.1
        /// xyz.log.2 ...
        /// Latest has the highest number
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task RotatingFileNameTest()
        {
            string filter = "KinesisTapxyz.log*";
            string logFileName = "KinesisTapxyz.log";

            //Clean up the temp files from the previous test rather than after each test so that we can inspect the test files
            DeleteFiles(TestUtility.GetTestHome(), filter);

            await CreateAndRunWatcher(filter, async (logRecords) =>
            {
                int accumulatedRecords = 0;
                string filePath = Path.Combine(TestUtility.GetTestHome(), logFileName);
                WriteRandomRecords(filePath, out int records, out string lastLine1, GenerateSingleLineRecord);
                accumulatedRecords += records;
                RenameLogFile(filePath);
                WriteRandomRecords(filePath, out records, out string lastLine2, GenerateSingleLineRecord);
                accumulatedRecords += records;
                await Task.Delay(2000);
                //Make sure that the last record of both batches are captured
                Assert.NotNull(logRecords.FirstOrDefault(l => lastLine1.Equals(l.GetMessage(null))));
                Assert.NotNull(logRecords.FirstOrDefault(l => lastLine2.Equals(l.GetMessage(null))));
                Assert.Equal(accumulatedRecords, logRecords.Count); //All records captured
            });
        }

        [Fact]
        public async Task SingleLineRecordParserBlankLineTest()
        {
            string filter = "SingleLineRecordParserTest.log";
            string logFileName = "SingleLineRecordParserTest.log";
            string filePath = Path.Combine(TestUtility.GetTestHome(), logFileName);
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }

            await CreateAndRunWatcher(filter, async (logRecords) =>
            {
                string log = @"
12/20/2017 15:40:58:	14117   Objects in OU=Servers,dc=ant,dc=amazon,dc=com	RunTime: 0 days, 0 hours, 0 minutes, 4.826 seconds

12/20/2017 15:43:40:	14117   Objects in OU=Servers,dc=ant,dc=amazon,dc=com	RunTime: 0 days, 0 hours, 0 minutes, 5.199 seconds

12/20/2017 16:05:55:	14117   Objects in OU=Servers,dc=ant,dc=amazon,dc=com	RunTime: 0 days, 0 hours, 0 minutes, 5.195 seconds

12/20/2017 16:35:49:	14121   Objects in OU=Servers,dc=ant,dc=amazon,dc=com	RunTime: 0 days, 0 hours, 0 minutes, 5.470 seconds
";
                File.WriteAllText(filePath, log);
                await Task.Delay(2000);
                Assert.Equal(4, logRecords.Count); //All records captured

                var envelope = (ILogEnvelope)logRecords[3];
                Assert.Equal(8, envelope.LineNumber);
            });
        }

        [Fact]
        public async Task DHCPSkipLinesTest()
        {
            string filter = "*.DHCPlog";
            string logFileName = "DHCP.DHCPlog";

            string filePath = Path.Combine(TestUtility.GetTestHome(), logFileName);
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }

            var config = TestUtility.GetConfig("Sources", "DHCPLog");

            await CreateAndRunWatcher(filter, config, async(logRecords) =>
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
24,09/29/17,00:00:04,Database Cleanup Begin,,,,,0,6,,,,,,,,,0";
                File.WriteAllText(filePath, log);
                await Task.Delay(2000);
                Assert.Single(logRecords); //All records captured
                var envelope = (ILogEnvelope)logRecords[0];
                Assert.Equal(35, envelope.LineNumber);
            }, new SingeLineRecordParser());
        }

        [Fact] 
        public async Task TimeStampedRecordTest()
        {
            string filter = "TimeStampLog????????.log";
            string logFileNameFormat = "TimeStampLog{0:yyyyMMdd}.log";
            string recordTimeStampFormat = "MM/dd/yyyy HH:mm:ss";

            await CreateAndRunWatcher(filter, null, async (logRecords) =>
            {
                int accumulatedRecords = 0;
                //Run one batch of test
                string filePath = GetFileNameFromTimeStamp(logFileNameFormat);
                accumulatedRecords = await RunDirectoryWatcherTest(filePath, logRecords, accumulatedRecords, GenerateMultiLineRecord);
                //Run another batch of test with the same file name
                accumulatedRecords = await RunDirectoryWatcherTest(filePath, logRecords, accumulatedRecords, GenerateMultiLineRecord);
            }, new TimeStampRecordParser(recordTimeStampFormat, null, DateTimeKind.Utc));
        }
#endregion

#region private members
        private void RenameLogFile(string filePath)
        {
            string directory = Path.GetDirectoryName(filePath);
            string fileName = Path.GetFileName(filePath);
            string[] files = Directory.GetFiles(directory, fileName + "*");
            string maxFile = files.Max();
            int fileNum = 1;
            if (!maxFile.Equals(filePath))
            {
                fileNum = int.Parse(maxFile.Substring(filePath.Length + 1)) + 1;
            }
            File.Move(filePath, $"{filePath}.{fileNum:D8}");
        }
    
        private async Task CreateAndRunWatcher(string filter, Func<List<IEnvelope>, Task> testBody)
        {
            await CreateAndRunWatcher(filter, null, testBody, new SingeLineRecordParser());
        }

        private async Task CreateAndRunWatcher<TData>(string filter, IConfiguration config, Func<List<IEnvelope>, Task> testBody, IRecordParser<TData, LogContext> recordParser)
        {
            ListEventSink logRecords = new ListEventSink();
            DirectorySource<TData, LogContext> watcher = new DirectorySource<TData, LogContext>
                (TestUtility.GetTestHome(), filter, 1000, new PluginContext(config, NullLogger.Instance, null), recordParser, DirectorySourceFactory.CreateLogSourceInfo);
            watcher.Subscribe(logRecords);
            watcher.Start();

            await testBody(logRecords);

            watcher.Stop();
        }

        private async Task<int> RunDirectoryWatcherTest(string filePath, List<IEnvelope> logRecords, int accumulatedRecords)
        {
            return await RunDirectoryWatcherTest(filePath, logRecords, accumulatedRecords, GenerateSingleLineRecord);
        }

        private async Task<int> RunDirectoryWatcherTest(string filePath, List<IEnvelope> logRecords, int accumulatedRecords, Func<DateTime, string> generateRecord)
        {
            WriteRandomRecords(filePath, out int records, out string lastLine, generateRecord);
            await Task.Delay(2000);
            Assert.Equal(lastLine, logRecords[logRecords.Count - 1].GetMessage(null)); //Last record captured
            accumulatedRecords += records;
            Assert.Equal(accumulatedRecords, logRecords.Count); //All records captured
            return accumulatedRecords;
        }

        private void WriteRandomRecords(string filePath, out int records, out string lastRecord, Func<DateTime, string> generateRecord)
        {
            DateTime timestamp = DateTime.Now;
            records = Utility.Random.Next(999) + 1;
            lastRecord = null;
            for (int i = 0; i < records; i++)
            {
                lastRecord = generateRecord(timestamp.AddMilliseconds(i));
                WriteLog(filePath, lastRecord);
            }
        }

        private string GenerateSingleLineRecord(DateTime timestamp)
        {
            return timestamp.ToString("yyyyMMddhhmmssfff");
        }

        private string GenerateMultiLineRecord(DateTime timestamp)
        {
            return timestamp.ToString("MM/dd/yyyy HH:mm:ss" + ".fff") + Environment.NewLine + "Second Line." + Environment.NewLine + "Third Line.";
        }

        private string GetFileNameFromTimeStamp(string fileNameFormat)
        {
            DateTime timestamp = DateTime.Now;
            string file = string.Format(fileNameFormat, timestamp);
            return Path.Combine(TestUtility.GetTestHome(), file); ;
        }

        private void AddRecords<T> (List<T> logRecords, IList<T> newRecord)
        {
            logRecords.AddRange(newRecord);
        }

        private void WriteLog(string filePath, string line)
        {
            using (var sw = File.AppendText(filePath))
            {
                sw.WriteLine(line);
            }
        }

        private static void DeleteFiles(string directory, string fileSpec)
        {
            foreach(string f in Directory.EnumerateFiles(directory, fileSpec))
            {
                File.Delete(f);
            }
        }
        #endregion
    }
}
