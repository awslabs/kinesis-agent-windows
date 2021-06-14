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
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Amazon.KinesisTap.Core.Test;
using Amazon.KinesisTap.Shared;
using Amazon.KinesisTap.Test.Common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;
using Xunit.Abstractions;

namespace Amazon.KinesisTap.Filesystem.Test
{
    [Collection(nameof(AsyncDirectorySourceTest))]
    public class AsyncDirectorySourceTest : AsyncDirectorySourceTestBase
    {
        private readonly IBookmarkManager _bookmarkManager;
        private readonly CancellationTokenSource _cts = new();

        private enum SymbolicLink
        {
            File = 0,
            Directory = 1
        }

        /// <summary>
        /// Create a symlink for Windows platform.
        /// </summary>
        [DllImport("kernel32.dll", CharSet = CharSet.Unicode)]
        static extern bool CreateSymbolicLink(string lpSymlinkFileName, string lpTargetFileName, SymbolicLink dwFlags);

        /// <summary>
        /// Create a symlink for Unix platform.
        /// </summary>
        static void CreateSymbolicLinkUnix(string symlinkFilePath, string sourceFilePath)
        {
            var shell = new BashShell();
            _ = shell.RunCommand($"ln -s {sourceFilePath} {symlinkFilePath}", 1000);
        }

        public AsyncDirectorySourceTest(ITestOutputHelper output) : base(output)
        {
            var mock = new Mock<IBookmarkManager>();
            _bookmarkManager = mock.Object;
        }

        private bool _disposed;
        protected override void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                _cts.Dispose();
            }

            _disposed = true;
            base.Dispose(disposing);
        }

        [Fact]
        public async Task ReadSymlinkedFile()
        {
            const string testLog = "test log";
            // create the source dir that contains the actual file
            var sourceDir = Path.Combine(TestUtility.GetTestHome(), Guid.NewGuid().ToString());
            Directory.CreateDirectory(sourceDir);
            var sourceFile = Path.Combine(sourceDir, "test_source.log");
            await File.AppendAllLinesAsync(sourceFile, new string[] { testLog });

            // create the symbolic link
            var linkedFile = Path.Combine(_testDir, "test_linked.log");
            if (OperatingSystem.IsWindows())
            {
                CreateSymbolicLink(linkedFile, sourceFile, SymbolicLink.File);
            }
            else
            {
                CreateSymbolicLinkUnix(linkedFile, sourceFile);
            }

            // start AsyncDirectorySource
            var logger = NullLogger.Instance;
            var ctx = new PluginContext(null, logger, null, _bookmarkManager, null, null);
            using var source = new AsyncDirectorySource<string, LogContext>(_sourceId, _testDir, CreateParser(logger), _bookmarkManager, new DirectorySourceOptions
            {
                NameFilters = new string[] { "*.log" },
                QueryPeriodMs = 100,
                InitialPosition = InitialPositionEnum.EOS
            }, ctx);

            var sink = new ListEventSink();
            source.Subscribe(sink);
            await source.StartAsync(_cts.Token);

            // write something the source file
            await File.AppendAllLinesAsync(sourceFile, new string[] { testLog });
            await Task.Delay(500);

            // assert that sink has the data
            Assert.Single(sink);
            _cts.Cancel();
            await source.StopAsync(default);
        }

        [Fact]
        public async Task StartStop()
        {
            var logger = NullLogger.Instance;
            var ctx = new PluginContext(null, logger, null, _bookmarkManager, null, null);
            using var source = new AsyncDirectorySource<string, LogContext>(_sourceId, _testDir, CreateParser(logger), _bookmarkManager, new DirectorySourceOptions
            {
                NameFilters = new string[] { "*.log" },
                QueryPeriodMs = 100,
            }, ctx);
            var sink = new ListEventSink();
            source.Subscribe(sink);
            await source.StartAsync(_cts.Token);

            var testLog = "test log";
            var file = Path.Combine(_testDir, "test.log");
            File.AppendAllLines(file, new string[] { testLog });
            await Task.Delay(500);

            var envelope = (LogEnvelope<string>)sink.Single();
            Assert.Equal(file, envelope.FilePath);
            Assert.Equal(testLog, envelope.ToString());

            _cts.Cancel();
            await source.StopAsync(default);
            File.AppendAllLines(file, new string[] { testLog });
            await Task.Delay(100);

            Assert.Single(sink);
        }

        [Fact]
        public async Task MetricsPublish()
        {
            var logger = NullLogger.Instance;
            var metrics = new KinesisTapMetricsSource(nameof(KinesisTapMetricsSource), NullLogger.Instance);
            var ctx = new PluginContext(null, logger, metrics);
            using var source = new AsyncDirectorySource<string, LogContext>(_sourceId, _testDir, CreateParser(logger), _bookmarkManager, new DirectorySourceOptions
            {
                NameFilters = new string[] { "*.log" },
                QueryPeriodMs = 100,
            }, ctx);
            var metricsSink = new MockMetricsSink(1, ctx);
            metrics.Subscribe(metricsSink);

            await metricsSink.StartAsync(_cts.Token);
            await source.StartAsync(_cts.Token);

            _cts.Cancel();
            await source.StopAsync(default);
            await metricsSink.StopAsync(default);

            Assert.Equal(2, metricsSink.AccumlatedValues.Count);
        }

        [WindowsOnlyTheory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(10)]
        public async Task IncludeSubdirectories(int depth)
        {
            var ctx = new PluginContext(null, NullLogger.Instance, null, _bookmarkManager, null, null);
            using var source = new AsyncDirectorySource<string, LogContext>(_sourceId, _testDir, CreateParser(NullLogger.Instance), _bookmarkManager, new DirectorySourceOptions
            {
                NameFilters = new string[] { "*.log" },
                QueryPeriodMs = 100,
                IncludeSubdirectories = true
            }, ctx);
            var sink = new ListEventSink();
            source.Subscribe(sink);
            await source.StartAsync(_cts.Token);

            var path = _testDir;
            for (var d = 0; d < depth; d++)
            {
                path = Path.Combine(path, $"sub_{d}");
                Directory.CreateDirectory(path);
                var file = Path.Combine(path, "test.log");
                var line = $"From file '{file}'";
                File.AppendAllLines(file, new string[] { line });
            }

            await Task.Delay(1000);
            Assert.Equal(depth, sink.Count);
            foreach (var i in sink)
            {
                var envelope = (LogEnvelope<string>)i;
                Assert.Equal($"From file '{envelope.FilePath}'", i.ToString());
            }
            _cts.Cancel();
            await source.StopAsync(default);
        }

        [Fact]
        public async Task MultipleFilters()
        {
            var logger = NullLogger.Instance;
            var ctx = new PluginContext(null, logger, null, _bookmarkManager, null, null);
            using var source = new AsyncDirectorySource<string, LogContext>(_sourceId, _testDir, CreateParser(logger), _bookmarkManager, new DirectorySourceOptions
            {
                NameFilters = new string[] { "*.log", "*.txt", "abc?.usage" },
                QueryPeriodMs = 100,
                IncludeSubdirectories = true
            }, ctx);
            var sink = new ListEventSink();
            source.Subscribe(sink);
            await source.StartAsync(_cts.Token);

            var subdir = Path.Combine(_testDir, "sub");
            Directory.CreateDirectory(subdir);
            var line = "test line";
            File.AppendAllLines(Path.Combine(_testDir, "app.log"), new string[] { line });
            File.AppendAllLines(Path.Combine(_testDir, "app.txt"), new string[] { line });
            File.AppendAllLines(Path.Combine(_testDir, "abcd.log"), new string[] { line });

            File.AppendAllLines(Path.Combine(subdir, "app.log"), new string[] { line });
            File.AppendAllLines(Path.Combine(subdir, "app.txt"), new string[] { line });
            File.AppendAllLines(Path.Combine(subdir, "abcd.log"), new string[] { line });

            await Task.Delay(500);
            Assert.Equal(6, sink.Count);
            _cts.Cancel();
            await source.StopAsync(default);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(1000)]
        public async Task MultipleLogs(int count)
        {
            var rnd = new Random();
            var logger = NullLogger.Instance;
            var ctx = new PluginContext(null, logger, null, _bookmarkManager, null, null);
            using var source = new AsyncDirectorySource<string, LogContext>(_sourceId, _testDir, CreateParser(logger), _bookmarkManager, new DirectorySourceOptions
            {
                NameFilters = new string[] { "*.log" },
                QueryPeriodMs = 100,
                IncludeSubdirectories = true
            }, ctx);
            var sink = new ListEventSink();
            source.Subscribe(sink);
            await source.StartAsync(_cts.Token);

            for (var c = 0; c < count; c++)
            {
                var fileId = rnd.Next(0, 10);
                var text = new string((char)('a' + rnd.Next(0, 20)), rnd.Next(512, 2048));
                File.AppendAllLines(Path.Combine(_testDir, $"file_{fileId}.log"), new string[] { text });
            }

            await Task.Delay(500);
            Assert.Equal(count, sink.Count);
            _cts.Cancel();
            await source.StopAsync(default);
        }

        [Theory]
        [InlineData(".tar")]
        [InlineData(".gz")]
        [InlineData(".zip")]
        [InlineData(".bz2")]
        public async Task ExcludedFileExtensions(string extension)
        {
            var logger = NullLogger.Instance;
            var ctx = new PluginContext(null, logger, null, _bookmarkManager, null, null);
            using var source = new AsyncDirectorySource<string, LogContext>(_sourceId, _testDir, CreateParser(logger), _bookmarkManager, new DirectorySourceOptions
            {
                NameFilters = new string[] { $"*.*" },
                QueryPeriodMs = 100
            }, ctx);
            var sink = new ListEventSink();
            source.Subscribe(sink);
            await source.StartAsync(_cts.Token);

            var file = Path.Combine(_testDir, $"test{extension}");
            File.AppendAllLines(file, new string[] { "test log" });
            await Task.Delay(500);
            Assert.Empty(sink);

            _cts.Cancel();
            await source.StopAsync(default);
        }

        [Fact]
        public async Task LogRoration()
        {
            const string logFileNameFormat = "app_{0:yyyyMMddhhmmss}.log";
            var logger = NullLogger.Instance;
            var ctx = new PluginContext(null, logger, null, _bookmarkManager, null, null);
            using var source = new AsyncDirectorySource<string, LogContext>(_sourceId, _testDir, CreateParser(logger), _bookmarkManager, new DirectorySourceOptions
            {
                NameFilters = new string[] { "*.log" },
                QueryPeriodMs = 100,
                IncludeSubdirectories = true
            }, ctx);
            var sink = new ListEventSink();
            source.Subscribe(sink);
            await source.StartAsync(_cts.Token);

            var file = Path.Combine(_testDir, "app.log");
            var archived = Path.Combine(_testDir, string.Format(logFileNameFormat, DateTime.Now));

            // write 10 lines to file1
            for (var i = 0; i < 10; i++)
            {
                File.AppendAllLines(file, new string[] { $"day1_{i}" });
            }

            await Task.Delay(500);

            // rotate 
            File.Move(file, archived, true);

            // write 10 lines to new file
            for (var i = 0; i < 10; i++)
            {
                File.AppendAllLines(file, new string[] { $"day2_{i}" });
            }

            // write 5 more lines to old file
            for (var i = 10; i < 15; i++)
            {
                File.AppendAllLines(archived, new string[] { $"day1_{i}" });
            }

            await Task.Delay(1000);

            var file1Records = sink
                .Select(r => r as LogEnvelope<string>)
                .Where(r => r.ToString().StartsWith("day1_")).ToList();
            for (var i = 0; i < 15; i++)
            {
                Assert.Equal($"day1_{i}", file1Records[i].ToString());
            }

            var file2Records = sink
                .Select(r => r as LogEnvelope<string>)
                .Where(r => r.ToString().StartsWith("day2_")).ToList();
            for (var i = 0; i < 10; i++)
            {
                Assert.Equal($"day2_{i}", file2Records[i].ToString());
            }

            _cts.Cancel();
            await source.StopAsync(default);
        }

        /// <summary>
        /// If the log is rotated to a file name that does not match 'NameFilters',
        /// DirectorySource should not process that file.
        /// </summary>
        [Fact]
        public async Task LogRotationToDifferentNamePattern()
        {
            var logger = NullLogger.Instance;
            var ctx = new PluginContext(null, logger, null, _bookmarkManager, null, null);
            using var source = new AsyncDirectorySource<string, LogContext>(_sourceId, _testDir, CreateParser(logger), _bookmarkManager, new DirectorySourceOptions
            {
                NameFilters = new string[] { "*.log" },
                QueryPeriodMs = 100,
                IncludeSubdirectories = true
            }, ctx);
            var sink = new ListEventSink();
            source.Subscribe(sink);
            await source.StartAsync(_cts.Token);

            var file = Path.Combine(_testDir, "app.log");
            var archived = Path.Combine(_testDir, "app.log-old");

            // write 10 lines to app.log and check
            for (var i = 0; i < 10; i++)
            {
                File.AppendAllLines(file, new string[] { $"day1_{i}" });
            }
            await Task.Delay(500);
            Assert.Equal(10, sink.Count);
            sink.Clear();

            // rotate 
            File.Move(file, archived, true);

            // write 5 lines to archived
            for (var i = 0; i < 10; i++)
            {
                File.AppendAllLines(archived, new string[] { $"day2_{i}" });
            }
            await Task.Delay(1000);

            // make sure the sink does get records from archived
            Assert.Empty(sink);

            // write 7 lines to app.log and check
            for (var i = 0; i < 7; i++)
            {
                File.AppendAllLines(file, new string[] { $"day1_{i}" });
            }
            await Task.Delay(500);
            Assert.Equal(7, sink.Count);

            _cts.Cancel();
            await source.StopAsync(default);
        }

        [WindowsOnlyTheory]
        [InlineData(10, '/')]
        [InlineData(1, '/')]
        [InlineData(10, '\\')]
        [InlineData(1, '\\')]
        public async Task FilterSubdirectories(int depth, char separator)
        {
            var logger = NullLogger.Instance;
            var ctx = new PluginContext(null, logger, null, _bookmarkManager, null, null);
            var subSpec = string.Join(Path.DirectorySeparatorChar, Enumerable.Range(0, depth).Select(i => $"sub_{i}"));
            var subDir = Path.Combine(_testDir, subSpec);
            Directory.CreateDirectory(subDir);

            using var source = new AsyncDirectorySource<string, LogContext>(_sourceId, _testDir, CreateParser(logger), _bookmarkManager, new DirectorySourceOptions
            {
                NameFilters = new string[] { "*.log" },
                QueryPeriodMs = 100,
                IncludeSubdirectories = true,
                IncludeDirectoryFilter = new[] { string.Join(separator, Enumerable.Range(0, depth).Select(i => $"sub_{i}")) }
            }, ctx);
            var sink = new ListEventSink();
            source.Subscribe(sink);
            await source.StartAsync(_cts.Token);
            await Task.Delay(500);

            await File.AppendAllLinesAsync(Path.Combine(subDir, "subDir.log"), new string[] { $"log from {subDir}" });
            await File.AppendAllLinesAsync(Path.Combine(_testDir, "testDir.log"), new string[] { $"log from {_testDir}" });

            await Task.Delay(500);

            // sink should only contain one record
            Assert.Single(sink);
            // record should originate from the file in 
            Assert.Equal(subDir, Path.GetDirectoryName(sink.Select(r => r as LogEnvelope<string>).Single().FilePath));

            _cts.Cancel();
            await source.StopAsync(default);
        }

        [Fact]
        public async Task FileEncodingDetection()
        {
            var encodings = new Encoding[]
            {
                new UTF8Encoding(true),
                new UnicodeEncoding(true,true),
                new UnicodeEncoding(false,true),
                new UTF32Encoding(true,true),
                new UTF32Encoding(false,true)
            };

            var logger = NullLogger.Instance;
            var ctx = new PluginContext(null, logger, null, _bookmarkManager, null, null);
            using var source = new AsyncDirectorySource<string, LogContext>(_sourceId, _testDir, CreateParser(logger), _bookmarkManager, new DirectorySourceOptions
            {
                NameFilters = new string[] { "*.log" },
                QueryPeriodMs = 100
            }, ctx);
            var sink = new ListEventSink();
            source.Subscribe(sink);
            await source.StartAsync(_cts.Token);

            var texts = new string[]
            {
                "새해 복 많이 받으세요",
                "明けましておめでとうございます",
                "新年快乐",
                "नववर्ष की शुभकामना",
                "chúc mừng năm mới"
            };

            for (var i = 0; i < encodings.Length; i++)
            {
                File.WriteAllLines(Path.Combine(_testDir, $"file_{i}.log"), texts, encodings[i]);
            }

            await Task.Delay(500);
            for (var i = 0; i < encodings.Length; i++)
            {
                var records = sink.Select(r => r as LogEnvelope<string>).Where(r => r.FilePath == Path.Combine(_testDir, $"file_{i}.log")).ToList();
                for (var j = 0; j < texts.Length; j++)
                {
                    Assert.Equal(texts[j], records[j].ToString());
                }
            }

            _cts.Cancel();
            await source.StopAsync(default);
        }

        [Theory]
        [InlineData(InitialPositionEnum.Bookmark)]
        [InlineData(InitialPositionEnum.BOS)]
        [InlineData(InitialPositionEnum.EOS)]
        public async Task DirectoryIsDeleted(InitialPositionEnum initialPosition)
        {
            var logger = NullLogger.Instance;
            var ctx = new PluginContext(null, logger, null, _bookmarkManager, null, null);
            using var source = new AsyncDirectorySource<string, LogContext>(_sourceId, _testDir, CreateParser(logger), _bookmarkManager, new DirectorySourceOptions
            {
                NameFilters = new string[] { "*.log" },
                QueryPeriodMs = 100,
                InitialPosition = initialPosition
            }, ctx)
            {
                DelayBetweenDependencyPoll = TimeSpan.FromMilliseconds(100)
            };
            var sink = new ListEventSink();
            source.Subscribe(sink);
            await source.StartAsync(_cts.Token);

            File.AppendAllLines(Path.Combine(_testDir, "test.log"), new string[] { "test1" });
            await Task.Delay(500);
            Assert.Single(sink);

            Directory.Delete(_testDir, true);
            await Task.Delay(500);
            Directory.CreateDirectory(_testDir);
            await Task.Delay(500);

            var tries = 0;
            while (tries++ < 5)
            {
                // in CodeBuild container sometimes the directory is not accessible right away
                try
                {
                    File.AppendAllLines(Path.Combine(_testDir, "test2.log"), new string[] { "test2" });
                    await Task.Delay(500);
                    File.AppendAllLines(Path.Combine(_testDir, "test3.log"), new string[] { "test3" });
                    await Task.Delay(500);
                    Assert.Equal(3, sink.Count);
                    Assert.Single(sink.Select(r => r as LogEnvelope<string>).Where(r => r.ToString() == "test1"));
                    Assert.Single(sink.Select(r => r as LogEnvelope<string>).Where(r => r.ToString() == "test2"));
                    Assert.Single(sink.Select(r => r as LogEnvelope<string>).Where(r => r.ToString() == "test3"));
                    return;
                }
                catch (UnauthorizedAccessException)
                {
                    await Task.Delay(500);
                    continue;
                }
            }

            _cts.Cancel();
            await source.StopAsync(default);
        }

        [Theory]
        [InlineData(InitialPositionEnum.Bookmark)]
        [InlineData(InitialPositionEnum.BOS)]
        [InlineData(InitialPositionEnum.EOS)]
        public async Task DirectoryCreatedAfterSourceStart_ShouldBeDetected(InitialPositionEnum initialPosition)
        {
            if (Directory.Exists(_testDir))
            {
                Directory.Delete(_testDir);
            }

            var ctx = new PluginContext(null, NullLogger.Instance, null, _bookmarkManager, null, null);
            using var source = new AsyncDirectorySource<string, LogContext>(_sourceId, _testDir, CreateParser(NullLogger.Instance), _bookmarkManager, new DirectorySourceOptions
            {
                NameFilters = new string[] { "*.log" },
                QueryPeriodMs = 100,
                InitialPosition = initialPosition
            }, ctx)
            {
                DelayBetweenDependencyPoll = TimeSpan.FromMilliseconds(100)
            };

            var sink = new ListEventSink();
            source.Subscribe(sink);
            await source.StartAsync(_cts.Token);
            await Task.Delay(500);

            //create the directory
            Directory.CreateDirectory(_testDir);
            File.AppendAllLines(Path.Combine(_testDir, "test.log"), new string[] { "test1" });
            await Task.Delay(500);
            Assert.Equal("test1", (sink.Single() as LogEnvelope<string>).Data);

            _cts.Cancel();
            await source.StopAsync(default);
        }

        private static SingleLineLogParser CreateParser(ILogger logger) => new(logger, 0, null, 1024);
    }
}
