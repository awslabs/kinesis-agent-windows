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
using Xunit;
using Microsoft.Extensions.Logging;
using System.IO;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Configuration;
using System.Globalization;

namespace Amazon.KinesisTap.Core.Test
{
    public class DirectoryWatcherTest
    {
        private readonly BookmarkManager _bookmarkManager = new BookmarkManager();

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
                    new PluginContext(null, logger, null, _bookmarkManager),
                    new SingleLineRecordParser());
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
                    new PluginContext(null, logger, null, _bookmarkManager),
                    new SingleLineRecordParser());
                watcher.Start();
                Assert.Equal($"DirectorySource id {null} watching directory {TestUtility.GetTestHome()} with filter  started.", logger.LastEntry);
                watcher.Stop();
                Assert.Equal($"DirectorySource id {null} watching directory {TestUtility.GetTestHome()} with filter  stopped.", logger.LastEntry);
            }
        }

        /// <summary>
        /// Include Subdirectories Test
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task IncludeSubdirectoriesTest()
        {
            var testName = "IncludeSubdirectoriesTest";
            var filter = "*.*";
            var subDir1 = "CPU";
            var subDir2 = "Memory";

            var directory = new TestDirectory(testName)
            {
                SubDirectories = new TestDirectory[]
                {
                     new TestDirectory(subDir1),
                     new TestDirectory(subDir2)
                }
            };

            var config = TestUtility.GetConfig("Sources", "IncludeSubdirectories");

            await CreateAndRunWatcher(testName, filter, config, async (logRecords) =>
            {
                var filePath1 = Path.Combine(TestUtility.GetTestHome(), testName, subDir1, "test");
                var filePath2 = Path.Combine(TestUtility.GetTestHome(), testName, subDir2, "test");

                this.WriteLog(filePath1, "this is a test");
                this.WriteLog(filePath2, "this is another test");
                await Task.Delay(2000);

                Assert.Equal(2, logRecords.Count);
                var env1 = (ILogEnvelope)logRecords[0];
                Assert.Equal("test", env1.FileName);
                Assert.Equal(filePath1, env1.FilePath);

                var env2 = (ILogEnvelope)logRecords[1];
                Assert.Equal("test", env2.FileName);
                Assert.Equal(filePath2, env2.FilePath);

            }, new SingleLineRecordParser(), directory);
        }

        /// <summary>
        /// Include MultipleLevel Subdirectories Test
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task IncludeMultipleLevelSubdirectoriesTest()
        {
            var testName = "IncludeMultipleLevelSubdirectoriesTest";
            var filter = "*.*";
            var subDir1 = "CPU";
            var subDir2 = "Memory";
            var subDir3 = "CPU-1";

            var directory = new TestDirectory(testName)
            {
                SubDirectories = new TestDirectory[]
                {
                     new TestDirectory(subDir1)
                     {
                         SubDirectories = new TestDirectory[] { new TestDirectory(subDir3) }
                     },
                     new TestDirectory(subDir2)
                }
            };

            var config = TestUtility.GetConfig("Sources", "IncludeSubdirectories");

            await CreateAndRunWatcher(testName, filter, config, async (logRecords) =>
            {
                var filePath1 = Path.Combine(Path.Combine(TestUtility.GetTestHome(), testName, subDir1), subDir3, "test");
                var filePath2 = Path.Combine(TestUtility.GetTestHome(), testName, subDir2, "test");

                this.WriteLog(filePath1, "test test");
                this.WriteLog(filePath2, "test test test test test test test test test test test test test test");
                await Task.Delay(2000);

                Assert.Equal(2, logRecords.Count);
                var env1 = (ILogEnvelope)logRecords[0];
                Assert.Equal("test", env1.FileName);
                Assert.Equal(filePath1, env1.FilePath);

                var env2 = (ILogEnvelope)logRecords[1];
                Assert.Equal("test", env2.FileName);
                Assert.Equal(filePath2, env2.FilePath);

            }, new SingleLineRecordParser(), directory);
        }

        /// <summary>
        /// Include Subdirectories With "IncludeDirectoryFilter" Test
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task IncludeSubdirectoriesWithIncludeDirectoryFilterTest()
        {
            var testName = "IncludeSubdirectoriesWithIncludeDirectoryFilterTest";
            var filter = "*.*";
            var subDir1 = "CPU";
            var subDir2 = "Memory";
            var subDir3 = "CPU-1";

            var directory = new TestDirectory(testName)
            {
                SubDirectories = new TestDirectory[]
                {
                     new TestDirectory(subDir1),
                     new TestDirectory(subDir2),
                     new TestDirectory(subDir3)
                }
            };

            var config = TestUtility.GetConfig("Sources", "IncludeSubdirectories");
            config["IncludeDirectoryFilter"] = "CPU;Memory";
            await CreateAndRunWatcher(testName, filter, config, async (logRecords) =>
            {
                var filePath1 = Path.Combine(TestUtility.GetTestHome(), testName, subDir1, "test");
                var filePath2 = Path.Combine(TestUtility.GetTestHome(), testName, subDir2, "test");
                var filePath3 = Path.Combine(TestUtility.GetTestHome(), testName, subDir3, "test");

                this.WriteLog(filePath1, "test test test");
                this.WriteLog(filePath2, "test test test test test test");
                this.WriteLog(filePath3, "test test test test test test test test test");
                await Task.Delay(2000);

                Assert.Equal(2, logRecords.Count);
                var env1 = (ILogEnvelope)logRecords[0];
                Assert.Equal("test", env1.FileName);
                Assert.Equal(filePath1, env1.FilePath);

                var env2 = (ILogEnvelope)logRecords[1];
                Assert.Equal("test", env2.FileName);
                Assert.Equal(filePath2, env2.FilePath);

            }, new SingleLineRecordParser(), directory);
        }

        /// <summary>
        /// Include Subdirectories With "IncludeDirectoryFilter" With MultipleLevel Subdirectories Test
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task IncludeSubdirectoriesWithIncludeDirectoryFilterWithMultipleLevelSubdirectoriesTest()
        {
            var testName = "IncludeSubdirectoriesWithIncludeDirectoryFilterWithMultipleLevelSubdirectoriesTest";
            var filter = "*.*";
            var subDir1 = "CPU";
            var subDir2 = "Memory";
            var subDir3 = "CPU-1";
            var subDir4 = "Network";

            var directory = new TestDirectory(testName)
            {
                SubDirectories = new TestDirectory[]
                {
                     new TestDirectory(subDir1)
                     {
                         SubDirectories = new TestDirectory[]{ new TestDirectory(subDir3) }
                     },
                     new TestDirectory(subDir2),
                     new TestDirectory(subDir4)
                }
            };

            var config = TestUtility.GetConfig("Sources", "IncludeSubdirectories");
            config["IncludeDirectoryFilter"] = $@"CPU;CPU{Path.DirectorySeparatorChar}CPU-1";
            await CreateAndRunWatcher(testName, filter, config, async (logRecords) =>
            {
                var filePath1 = Path.Combine(TestUtility.GetTestHome(), testName, subDir1, "test");
                var filePath2 = Path.Combine(TestUtility.GetTestHome(), testName, subDir2, "test");
                var filePath3 = Path.Combine(Path.Combine(TestUtility.GetTestHome(), testName, subDir1), subDir3, "test");
                var filePath4 = Path.Combine(TestUtility.GetTestHome(), testName, subDir4, "test");

                this.WriteLog(filePath1, "this is a test 1");
                this.WriteLog(filePath2, "this is a test 2");
                this.WriteLog(filePath3, "this is a test 3");
                this.WriteLog(filePath4, "this is a test 4");
                await Task.Delay(2000);

                Assert.Equal(2, logRecords.Count);
                var env1 = (ILogEnvelope)logRecords[0];
                Assert.Equal("test", env1.FileName);
                Assert.Equal(filePath1, env1.FilePath);

                var env2 = (ILogEnvelope)logRecords[1];
                Assert.Equal("test", env2.FileName);
                Assert.Equal(filePath3, env2.FilePath);

            }, new SingleLineRecordParser(), directory);
        }

        /// <summary>
        /// Write to a single log file. New or existing.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task SingleLogTest()
        {
            string testName = "SingleLogTest";
            string filter = "KinesisTapTest????????.log";
            string logFileNameFormat = "KinesisTapTest{0:yyyyMMdd}.log";

            await CreateAndRunWatcher(testName, filter, async (logRecords) =>
            {
                int accumulatedRecords = 0;
                //Run one batch of test
                string filePath = GetFileNameFromTimeStamp(testName, logFileNameFormat);
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
            string testName = "FileNameContainingDateTimeStampTest";
            string filter = "KinesisTapDateTimeStamp??????????????.log";
            string logFileNameFormat = "KinesisTapDateTimeStamp{0:yyyyMMddhhmmss}.log";

            await CreateAndRunWatcher(testName, filter, async (logRecords) =>
            {
                int accumulatedRecords = 0;
                //Run one batch of test
                string filePath = GetFileNameFromTimeStamp(testName, logFileNameFormat);
                accumulatedRecords = await RunDirectoryWatcherTest(filePath, logRecords, accumulatedRecords);
                //Run another batch of test with a different file name
                filePath = GetFileNameFromTimeStamp(testName, logFileNameFormat);
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
            string testName = "RotatingFileNameTest";
            string filter = "KinesisTapxyz.log*";
            string logFileName = "KinesisTapxyz.log";

            await CreateAndRunWatcher(testName, filter, async (logRecords) =>
            {
                int accumulatedRecords = 0;
                string filePath = Path.Combine(TestUtility.GetTestHome(), testName, logFileName);
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

        /// <summary>
        /// xyz.log
        /// xyz.log.1
        /// xyz.log.2 ...
        /// Latest has the highest number
        /// </summary>
        /// <returns></returns>
        [Fact]
        [Trait("Category", "Integration")]
        public async Task RotatingFileNameWithLockTest()
        {
            string testName = "RotatingFileNameWithLockTest";
            string logFileName = $"{testName}-{DateTime.UtcNow:yyyyMMddHHmmssfff}.log";
            string filter = $"{logFileName}*";

            using (var logger = new MemoryLogger(testName))
            {
                string filePath = Path.Combine(TestUtility.GetTestHome(), testName, logFileName);

                await CreateAndRunWatcher(
                    testName,
                    filter,
                    null,
                    async (logRecords) =>
                    {
                        int accumulatedRecords = 0;
                        WriteRandomRecords(filePath, out int records, out string lastLine1, GenerateSingleLineRecord);
                        accumulatedRecords += records;
                        string lastLine2 = null;
                        using (var fs = RenameLogFile(filePath, true))
                        {
                            WriteRandomRecords(filePath, out records, out lastLine2, GenerateSingleLineRecord);
                            accumulatedRecords += records;

                            // Give it some time to fail before releasing the file lock.
                            await Task.Delay(2000);
                        }

                        // Give it some time to succeed after releasing the file lock.
                        await Task.Delay(3000);

                        //Make sure that the last record of both batches are captured
                        Assert.NotNull(logRecords.FirstOrDefault(l => lastLine1.Equals(l.GetMessage(null))));
                        Assert.NotNull(logRecords.FirstOrDefault(l => lastLine2.Equals(l.GetMessage(null))));
                        Assert.Equal(accumulatedRecords, logRecords.Count); //All records captured
                    },
                    new SingleLineRecordParser(),
                    logger
                );

                // Ensure that the exception was encountered, so that we know the test handled it properly.
                var exTest = $"System.IO.IOException: The process cannot access the file '{filePath}.00000001' because it is being used by another process.";
                Assert.Contains(logger.Entries, i => i.StartsWith(exTest));
            }
        }

        [Fact]
        public async Task SingleLineRecordParserBlankLineTest()
        {
            string testName = "SingleLineRecordParserBlankLineTest";
            string filter = "SingleLineRecordParserTest.log";
            string logFileName = "SingleLineRecordParserTest.log";
            string filePath = Path.Combine(TestUtility.GetTestHome(), testName, logFileName);
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }

            await CreateAndRunWatcher(testName, filter, async (logRecords) =>
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
            string testName = "DHCPSkipLinesTest";
            string filter = "*.DHCPlog";
            string logFileName = "DHCP.DHCPlog";

            string filePath = Path.Combine(TestUtility.GetTestHome(), testName, logFileName);
            if (File.Exists(filePath))
            {
                File.Delete(filePath);
            }

            var config = TestUtility.GetConfig("Sources", "DHCPLog");

            await CreateAndRunWatcher(testName, filter, config, async (logRecords) =>
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
            }, new SingleLineRecordParser());
        }

        [Fact]
        public async Task TimeStampedRecordTest()
        {
            string testName = "TimeStampedRecordTest";
            string filter = "TimeStampLog????????.log";
            string logFileNameFormat = "TimeStampLog{0:yyyyMMdd}.log";
            string recordTimeStampFormat = "MM/dd/yyyy HH:mm:ss";

            await CreateAndRunWatcher(testName, filter, null, async (logRecords) =>
            {
                int accumulatedRecords = 0;
                //Run one batch of test
                string filePath = GetFileNameFromTimeStamp(testName, logFileNameFormat);
                accumulatedRecords = await RunDirectoryWatcherTest(filePath, logRecords, accumulatedRecords, GenerateMultiLineRecord);
                //Run another batch of test with the same file name
                accumulatedRecords = await RunDirectoryWatcherTest(filePath, logRecords, accumulatedRecords, GenerateMultiLineRecord);
            }, new TimeStampRecordParser(recordTimeStampFormat, null, DateTimeKind.Utc));
        }

        /// <summary>
        /// Filter specification containing multiple filters.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task MultipleFilterTest()
        {
            string testName = "MultipleFilterTest";
            string filter = "*.log|*.txt";

            await CreateAndRunWatcher(testName, filter, async (logRecords) =>
            {
                int accumulatedRecords = 0;
                //Run a test with extension 1
                string filePath1 = Path.Combine(TestUtility.GetTestHome(), testName, "test.log");
                accumulatedRecords = await RunDirectoryWatcherTest(filePath1, logRecords, accumulatedRecords);
                //Run a test with extension 2
                string filePath2 = Path.Combine(TestUtility.GetTestHome(), testName, "test.txt");
                accumulatedRecords = await RunDirectoryWatcherTest(filePath2, logRecords, accumulatedRecords);
            });
        }

        /// <summary>
        /// Files like .zip should not be pickup by the wild-card filter
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task ExcludedFileTest()
        {
            string testName = "ExcludedFileTest";
            string filter = "*.*";

            await CreateAndRunWatcher(testName, filter, async (logRecords) =>
            {
                string filePath = Path.Combine(TestUtility.GetTestHome(), testName, "test.zip");
                WriteRandomRecords(filePath, out int records, out string lastLine, GenerateSingleLineRecord);
                await Task.Delay(2000);
                Assert.Empty(logRecords); //Should not pickup anything
            });
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

        private FileStream RenameLogFile(string filePath, bool withLock)
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
            var newname = $"{filePath}.{fileNum:D8}";
            File.Move(filePath, newname);
            return new FileStream(newname, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        }

        private async Task CreateAndRunWatcher(string testName, string filter, Func<List<IEnvelope>, Task> testBody)
        {
            await CreateAndRunWatcher(testName, filter, null, testBody, new SingleLineRecordParser());
        }

        private async Task CreateAndRunWatcher<TData>(string testName, string filter, IConfiguration config, Func<List<IEnvelope>, Task> testBody, IRecordParser<TData, LogContext> recordParser, TestDirectory directory = null)
        {
            await this.CreateAndRunWatcher(testName, filter, config, testBody, recordParser, NullLogger.Instance, directory);
        }

        private async Task CreateAndRunWatcher<TData>(string testName, string filter, IConfiguration config, Func<List<IEnvelope>, Task> testBody, IRecordParser<TData, LogContext> recordParser, ILogger logger, TestDirectory directory = null)
        {
            //Create a distinct directory based on testName so that tests can run in parallel
            string testDir = Path.Combine(TestUtility.GetTestHome(), testName);

            if (directory == null)
            {
                //The following will creates all directories and subdirectories in the specified path unless they already exist.
                Directory.CreateDirectory(testDir);

                //Clean up before the test rather than after so that we can inspect the files
                DeleteFiles(testDir, "*.*");
            }
            else
            {
                this.SetUpTestDirectory(directory, TestUtility.GetTestHome());
            }

            ListEventSink logRecords = new ListEventSink();

            DirectorySource<TData, LogContext> watcher = new DirectorySource<TData, LogContext>
                (testDir, filter, 1000, new PluginContext(config, logger, null, new BookmarkManager()), recordParser);
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
            return timestamp.ToString("yyyyMMddhhmmssfff", CultureInfo.InvariantCulture);
        }

        private string GenerateMultiLineRecord(DateTime timestamp)
        {
            return timestamp.ToString("MM/dd/yyyy HH:mm:ss" + ".fff", CultureInfo.InvariantCulture) + Environment.NewLine + "Second Line." + Environment.NewLine + "Third Line.";
        }

        private string GetFileNameFromTimeStamp(string testName, string fileNameFormat)
        {
            DateTime timestamp = DateTime.Now;
            string file = string.Format(fileNameFormat, timestamp);
            return Path.Combine(TestUtility.GetTestHome(), testName, file); ;
        }

        private void AddRecords<T>(List<T> logRecords, IList<T> newRecord)
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

        private void SetUpTestDirectory(TestDirectory directory, string parentPath)
        {
            string testDir = Path.Combine(parentPath, directory.Name);
            Directory.CreateDirectory(testDir);
            DeleteFiles(testDir, "*.*"); // needs to delete everything

            if (directory.SubDirectories != null)
            {
                foreach (var dir in directory.SubDirectories)
                {
                    SetUpTestDirectory(dir, testDir);
                }
            }
        }

        private static void DeleteFiles(string directory, string fileSpec)
        {
            foreach (string f in Directory.EnumerateFiles(directory, fileSpec))
            {
                File.Delete(f);
            }
        }
        #endregion

        class TestDirectory
        {
            public TestDirectory(string name)
            {
                this.Name = name;
            }

            public string Name { get; set; }

            public TestDirectory[] SubDirectories { get; set; }
        }
    }
}
