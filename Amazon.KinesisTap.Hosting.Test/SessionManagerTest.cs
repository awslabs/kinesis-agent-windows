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
using Xunit;
using Moq;
using System;
using System.IO;
using Amazon.KinesisTap.Core;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Amazon.KinesisTap.Core.Test;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using System.Threading;
using Newtonsoft.Json;
using Xunit.Abstractions;
using Amazon.KinesisTap.Core.Metrics;
using Amazon.KinesisTap.Test.Common;

namespace Amazon.KinesisTap.Hosting.Test
{
    /// <summary>
    /// Manages multiple configuration files and kick off multiple sessions.
    /// It persists Session IDs and supports dynamically-adding new config files.
    /// </summary>
    [Collection(nameof(SessionManagerTest))]
    public class SessionManagerTest : IDisposable
    {
        private readonly IMetrics _mockMetrics = new Mock<IMetrics>().Object;
        private readonly ITypeLoader _typeLoader = new Mock<ITypeLoader>().Object;
        private readonly IParameterStore _parameterStore = new DictionaryParameterStore();
        private readonly FactoryCatalogs _factoryCatalogs = new FactoryCatalogs
        {
            SourceFactoryCatalog = new FactoryCatalog<ISource>(),
            SinkFactoryCatalog = new FactoryCatalog<IEventSink>(),
            CredentialProviderFactoryCatalog = new FactoryCatalog<ICredentialProvider>(),
            GenericPluginFactoryCatalog = new FactoryCatalog<IGenericPlugin>(),
            PipeFactoryCatalog = new FactoryCatalog<IPipe>(),
            RecordParserCatalog = new FactoryCatalog<IRecordParser>()
        };
        private readonly string _testConfigDir;
        private readonly string _testMultiConfigDir;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly ITestOutputHelper _output;

        public SessionManagerTest(ITestOutputHelper output)
        {
            _testConfigDir = Path.Combine(Path.GetTempPath(), "SessionManagerTest-" + Guid.NewGuid().ToString());
            _testMultiConfigDir = Path.Combine(_testConfigDir, "configs");

            // creating _testMultiConfigDir will create _testConfigDir also
            Directory.CreateDirectory(_testMultiConfigDir);

            File.Copy("NLog.xml", Path.Combine(_testConfigDir, "NLog.xml"));
            File.Copy("appsettings.json", Path.Combine(_testConfigDir, HostingUtility.DefaultConfigFileName));
            _parameterStore.SetParameter(HostingUtility.ConfigDirPathKey, _testConfigDir);
            _output = output;
            NLog.LogManager.LoadConfiguration("NLog.xml");
        }

        public void Dispose()
        {
            // Deleting _testConfigDir will also delete _testMultiConfigDir
            if (Directory.Exists(_testConfigDir))
            {
                Directory.Delete(_testConfigDir, true);
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        public async Task SessionExitingInParallel(int noSessions)
        {
            // setup
            var mockSessionFactory = MockSessionFactory((id, val) => new MockSession(id, true, val, 500));

            // Create test json files in test dir
            for (var i = 0; i < noSessions; i++)
            {
                WriteConfigFile($"test_{i}.json");
            }

            using var sessionManager = new SessionManager(_factoryCatalogs, new LoggerFactory(), new NoopAppDataFileProvider(),
                _parameterStore, _typeLoader, mockSessionFactory.Object, _mockMetrics)
            {
                ConfigChangePollingIntervalMs = 100
            };

            // execute: start session manager
            await sessionManager.StartAsync(_cts.Token);
            await Task.Delay(1000);

            // verify number of running sessions
            Assert.Equal(noSessions + 1, sessionManager.ConfigPathToSessionMap.Count);

            // make sure sessionManager stops in less than 1 seconds
            _cts.Cancel();
            Assert.True(sessionManager.StopAsync(default).Wait(1000));
        }

        /// <summary>
        /// Test the SessionManager behavior for loading config files
        /// </summary>
        [Theory]
        [InlineData(0, 0)]
        [InlineData(0, 2)]
        [InlineData(1, 2)]
        [InlineData(10, 5)]
        public async Task TestConfigFilesLoading(int noConfigFiles, int addedFiles)
        {
            // setup
            var mockSessionFactory = MockSessionFactory((id, val) => new MockSession(id, true, val));

            // Create test json files in test dir
            for (var i = 0; i < noConfigFiles; i++)
            {
                WriteConfigFile($"test_{i}.json");
            }

            using (var sessionManager = new SessionManager(_factoryCatalogs, new LoggerFactory(), new NoopAppDataFileProvider(),
                 _parameterStore, _typeLoader, mockSessionFactory.Object, _mockMetrics)
            {
                ConfigChangePollingIntervalMs = 100
            })
            {
                // execute: start session manager
                await sessionManager.StartAsync(_cts.Token);

                await Task.Delay(500);

                // verify number of running sessions
                Assert.Equal(noConfigFiles + 1, sessionManager.ConfigPathToSessionMap.Count);

                //dynamically add config files
                for (var i = noConfigFiles; i < noConfigFiles + addedFiles; i++)
                {
                    WriteConfigFile($"test_{i}.json");
                }

                var allLoaded = false;
                for (var i = 0; i < 20; i++)
                {
                    // wait for new files to be notified and loaded
                    await Task.Delay(200);
                    allLoaded = noConfigFiles + addedFiles + 1 == sessionManager.ConfigPathToSessionMap.Count;
                    if (allLoaded)
                    {
                        break;
                    }
                }
                Assert.True(allLoaded, $"Only {sessionManager.ConfigPathToSessionMap.Count} sessions started out of {noConfigFiles + addedFiles + 1}");

                var configFiles = Directory.EnumerateFiles(_testMultiConfigDir, "*.json", SearchOption.TopDirectoryOnly).ToList();
                foreach (var path in configFiles)
                {
                    Assert.True(sessionManager.ConfigPathToSessionMap.ContainsKey(path));
                }

                _cts.Cancel();
                await sessionManager.StopAsync(CancellationToken.None);
            }
        }

        /// <summary>
        /// Test the SessionManager behavior for loading config files
        /// </summary>
        [Fact]
        public async Task TestConfigFileUpdate()
        {
            // setup
            const string extraConfigName = "test_extra.json";
            var mockSessionFactory = MockSessionFactory((id, val) => new MockSession(id, true, val));

            WriteConfigFile(extraConfigName);

            var defaultConfigPath = Path.Combine(_testConfigDir, HostingUtility.DefaultConfigFileName);
            var extraConfigPath = Path.Combine(_testMultiConfigDir, extraConfigName);

            using (var sessionManager = new SessionManager(_factoryCatalogs, new LoggerFactory(), new NoopAppDataFileProvider(),
                 _parameterStore, _typeLoader, mockSessionFactory.Object, _mockMetrics)
            {
                ConfigChangePollingIntervalMs = 100
            })
            {
                await sessionManager.StartAsync(_cts.Token);

                await Task.Delay(500);

                // verify number of running LogManager instances
                Assert.Equal(2, sessionManager.ConfigPathToSessionMap.Count);
                var defaultStartTime = sessionManager.ConfigPathToSessionMap[defaultConfigPath].StartTime;
                var extraStartTime = sessionManager.ConfigPathToSessionMap[extraConfigPath].StartTime;

                // update the default config file
                WriteConfigFile(defaultConfigPath);
                WriteConfigFile(extraConfigPath);

                await Task.Delay(1000);
                Assert.True(sessionManager.ConfigPathToSessionMap[defaultConfigPath].StartTime > defaultStartTime);
                Assert.True(sessionManager.ConfigPathToSessionMap[extraConfigPath].StartTime > extraStartTime);
                _cts.Cancel();
                await sessionManager.StopAsync(CancellationToken.None);
            }
        }

        [Theory]
        [InlineData(2, 2)]
        [InlineData(1, 1)]
        [InlineData(10, 2)]
        public async Task TestConfigFilesRemoved(int noConfigFiles, int removedFiles)
        {
            // setup
            var mockSessionFactory = MockSessionFactory((id, val) => new MockSession(id, true, val));

            // Create test json files in test dir
            for (var i = 0; i < noConfigFiles; i++)
            {
                WriteConfigFile($"test_{i}.json");
            }

            using (var sessionManager = new SessionManager(_factoryCatalogs, new LoggerFactory(), new NoopAppDataFileProvider(),
                 _parameterStore, _typeLoader, mockSessionFactory.Object, _mockMetrics)
            {
                ConfigChangePollingIntervalMs = 100
            })
            {
                await sessionManager.StartAsync(_cts.Token);

                await Task.Delay(500);

                // verify number of running LogManager instances
                Assert.Equal(noConfigFiles + 1, sessionManager.ConfigPathToSessionMap.Count);

                //dynamically add config files
                for (var i = 0; i < removedFiles; i++)
                {
                    RemoveConfigFile($"test_{i}.json");
                }

                // wait for new files to be notified and loaded
                await Task.Delay(500);

                // verify config files
                Assert.Equal(noConfigFiles - removedFiles + 1, sessionManager.ConfigPathToSessionMap.Count);
                var configFiles = Directory.EnumerateFiles(_testMultiConfigDir, "*.json", SearchOption.TopDirectoryOnly).ToList();
                foreach (var path in configFiles)
                {
                    Assert.True(sessionManager.ConfigPathToSessionMap.ContainsKey(path));
                }
                _cts.Cancel();
                await sessionManager.StopAsync(CancellationToken.None);
            }
        }

        [Theory]
        [InlineData("default.json")]
        [InlineData("extra config.json")]
        [InlineData("extra>config.json")]
        [InlineData("extra<config.json")]
        [InlineData("extra\"config.json")]
        [InlineData("extra\'config.json")]
        [InlineData("extra:config.json")]
        [InlineData("extra|config.json")]
        [InlineData("extra?config.json")]
        [InlineData("extra*config.json")]
        [InlineData("extra_config.xml")]
        public async Task TestInvalidConfigFileName_DoNotLoad(string fileName)
        {
            // setup
            var mockSessionFactory = MockSessionFactory((id, val) => new MockSession(id, true, val));

            using var sessionManager = new SessionManager(_factoryCatalogs, new LoggerFactory(), new NoopAppDataFileProvider(),
                 _parameterStore, _typeLoader, mockSessionFactory.Object, _mockMetrics)
            {
                ConfigChangePollingIntervalMs = 100
            };

            await sessionManager.StartAsync(_cts.Token);

            await Task.Delay(500);

            // assert only default session running
            Assert.Single(sessionManager.ConfigPathToSessionMap);

            try
            {
                // create a config file
                WriteConfigFile(fileName);
            }
            catch (Exception ex) when (ex is IOException || ex is NotSupportedException || ex is ArgumentException)
            {
                // on Windows, some characters in this test are not allowed to be in file names, so we ignore those
            }

            await Task.Delay(1000);

            // assert still only default session running
            Assert.Single(sessionManager.ConfigPathToSessionMap);
            _cts.Cancel();
            await sessionManager.StopAsync(CancellationToken.None);
        }

        /// <summary>
        /// Ids of LogManagers corrensponding to config files should be preservered between restarts of SessionManager
        /// </summary>
        /// <param name="noConfigFiles">Number of configuration files.</param>
        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(10)]
        public async Task TestIdsArePreserved(int noConfigFiles)
        {
            // setup
            var mockSessionFactory = MockSessionFactory((id, val) => new MockSession(id, true, val));

            var sessionManager = new SessionManager(_factoryCatalogs, new LoggerFactory(), new NoopAppDataFileProvider(),
                 _parameterStore, _typeLoader, mockSessionFactory.Object, _mockMetrics)
            {
                ConfigChangePollingIntervalMs = 100
            };

            // Create test json files in test dir
            for (var i = 0; i < noConfigFiles; i++)
            {
                WriteConfigFile($"test_{i}.json");
            }

            await sessionManager.StartAsync(_cts.Token);

            await Task.Delay(500);

            // save the mapping
            var configNameMapping = new Dictionary<string, string>();
            foreach (var item in sessionManager.ConfigPathToSessionMap.ToArray())
            {
                configNameMapping[item.Key] = item.Value.Name;
            }

            _cts.Cancel();
            await sessionManager.StopAsync(CancellationToken.None);
            sessionManager.Dispose();

            // create another Session Manager instance
            sessionManager = new SessionManager(_factoryCatalogs, new LoggerFactory(), new NoopAppDataFileProvider(),
                 _parameterStore, _typeLoader, mockSessionFactory.Object, _mockMetrics)
            {
                ConfigChangePollingIntervalMs = 100
            };

            using var cts = new CancellationTokenSource();
            await sessionManager.StartAsync(cts.Token);

            await Task.Delay(500);

            // verify that path-id mapping remains the same
            Assert.Equal(configNameMapping.Count, sessionManager.ConfigPathToSessionMap.Count);
            foreach (var item in sessionManager.ConfigPathToSessionMap.ToArray())
            {
                Assert.Equal(configNameMapping[item.Key], item.Value.Name);
            }

            cts.Cancel();
            await sessionManager.StopAsync(CancellationToken.None);
            sessionManager.Dispose();
        }

        /// <summary>
        /// A child (non-default) LogManager's failure should not affect others
        /// </summary>
        [Fact]
        public async Task TestChildLogManagerFailureNotAffectingOthers()
        {
            // setup
            const int noConfigFiles = 10;
            int configNumber = 0;
            var mockSessionFactory = MockSessionFactory((id, val) =>
                Interlocked.Increment(ref configNumber) == 2
                ? new MockSession(id, false, val)
                : new MockSession(id, true, val)
            );

            var sessionManager = new SessionManager(_factoryCatalogs, new LoggerFactory(), new NoopAppDataFileProvider(),
                 _parameterStore, _typeLoader, mockSessionFactory.Object, _mockMetrics)
            {
                ConfigChangePollingIntervalMs = 100
            };

            // Create test json files in test dir
            for (var i = 0; i < noConfigFiles; i++)
            {
                WriteConfigFile($"test_{i}.json");
            }

            await sessionManager.StartAsync(_cts.Token);

            await Task.Delay(500);

            Assert.Equal(noConfigFiles, sessionManager.ConfigPathToSessionMap.Values.Count);

            _cts.Cancel();
            await sessionManager.StopAsync(CancellationToken.None);
            sessionManager.Dispose();
        }

        /// <summary>
        /// Test case where the default Session throws an error on startup
        /// </summary>
        [Fact]
        public async Task TestDefaultLogManagerError()
        {
            // setup
            const int noConfigFiles = 10;
            var mockSessionFactory = MockSessionFactory((name, val) => name is null
                     ? new MockSession(name, false, val)
                     : new MockSession(name, true, val));

            using var sessionManager = new SessionManager(_factoryCatalogs, new LoggerFactory(), new NoopAppDataFileProvider(),
                    _parameterStore, _typeLoader, mockSessionFactory.Object, _mockMetrics)
            {
                ConfigChangePollingIntervalMs = 100
            };

            // Create test json files in test dir
            for (var i = 0; i < noConfigFiles; i++)
            {
                WriteConfigFile($"test_{i}.json");
            }
            await sessionManager.StartAsync(_cts.Token);
            await Task.Delay(2000);

            // Assert that the extra configuration files DO get loaded
            Assert.Equal(noConfigFiles, sessionManager.ConfigPathToSessionMap.Count);

            _cts.Cancel();
            await sessionManager.StopAsync(CancellationToken.None);
        }

        [Fact]
        public async Task TestDuplicateSessionName()
        {
            var mockSessionFactory = MockSessionFactory((id, val) => new MockSession(id, true, val));

            using var sessionManager = new SessionManager(_factoryCatalogs, new LoggerFactory(), new NoopAppDataFileProvider(),
                 _parameterStore, _typeLoader, mockSessionFactory.Object, _mockMetrics)
            {
                ConfigChangePollingIntervalMs = 100
            };
            await sessionManager.StartAsync(_cts.Token);

            // launch 'session1'
            WriteConfigFile($"session1.json");
            await Task.Delay(500);

            Assert.Equal(2, sessionManager.ConfigPathToSessionMap.Count);

            // launch another session with the same name
            WriteConfigFile($"session2.json", new
            {
                Name = "session1"
            });
            await Task.Delay(1000);
            Assert.Equal(2, sessionManager.ConfigPathToSessionMap.Count);
            _cts.Cancel();

            await sessionManager.StopAsync(default);
        }

        [Fact]
        public async Task TestCreateValidatedSession()
        {
            var mockSessionFactory = MockSessionFactory((id, val) => new MockSession(id, true, val));
            var fileFullPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("n") + ".json");
            using (var sessionManager = new SessionManager(_factoryCatalogs, new LoggerFactory(), new NoopAppDataFileProvider(),
                 _parameterStore, _typeLoader, mockSessionFactory.Object, _mockMetrics))
            {
                await sessionManager.StartAsync(_cts.Token);

                File.WriteAllText(fileFullPath, "{}");
                await Task.Delay(1000);
                Assert.False(sessionManager.ConfigPathToSessionMap.Single().Value.IsValidated);

                var sess = await sessionManager.LaunchValidatedSession(fileFullPath, default);
                Assert.True(sess.IsValidated);

                _cts.Cancel();
                await sessionManager.StopAsync(CancellationToken.None);
            }

            File.Delete(fileFullPath);
        }

        private Mock<ISessionFactory> MockSessionFactory(Func<string, bool, ISession> createSession)
        {
            // setup
            var mockSessionFactory = new Mock<ISessionFactory>(MockBehavior.Loose);
            mockSessionFactory
                .Setup(m => m.CreateSession(It.IsAny<string>(), It.IsAny<IConfiguration>()))
                .Returns((string name, IConfiguration config)
                    => createSession(name, false));

            mockSessionFactory
               .Setup(m => m.CreateValidatedSession(It.IsAny<string>(), It.IsAny<IConfiguration>()))
               .Returns((string name, IConfiguration config)
                   => createSession(name, true));

            return mockSessionFactory;
        }

        private void WriteConfigFile(string fileName, object configs = null)
        {
            string fileFullPath = Path.Combine(_testMultiConfigDir, fileName);
            var content = configs is null ? "{}" : JsonConvert.SerializeObject(configs);
            File.WriteAllText(fileFullPath, content);
        }

        private void RemoveConfigFile(string fileName)
        {
            string fileFullPath = Path.Combine(_testMultiConfigDir, fileName);
            File.Delete(fileFullPath);
        }
    }
}
