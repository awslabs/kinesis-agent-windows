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
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Hosting
{
    /// <summary>
    /// Manages the initialization and termination of sessions based on configuration files.
    /// </summary>
    public class SessionManager : IDisposable
    {
        private static SessionManager instance;
        private readonly HashSet<char> _forbiddenChars = new HashSet<char> { '>', '<', ':', '\'', '"', '/', '\\', '|', '?', '*' };
        public const int DefaultSessionId = 0;

        private readonly FileSystemWatcher _extraconfigWatcher;
        private readonly FileSystemWatcher _defaultConfigWatcher;
        private readonly ITypeLoader _typeLoader;
        private readonly IParameterStore _parameterStore;
        private readonly ISessionFactory _sessionFactory;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger _logger;
        private readonly INetworkStatusProvider _defaultNetworkProvider;
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly PersistentConfigFileIdMap _configIdMap;

        private int _configChanged = 1;
        private Task _configPoller = null;
        private bool _disposed = false;
        private volatile IConfigurationSection _defaultCredentialConfig = null;

        internal SessionManager(ISessionFactory sessionFactory, ITypeLoader typeLoader,
            IParameterStore parameterStore, INetworkStatusProvider defaultNetworkProvider, ILoggerFactory loggerFactory)
        {
            StackTraceMinimizerExceptionExtensions.DoCompressStackTrace = true;

            _typeLoader = typeLoader;
            _parameterStore = parameterStore;
            _sessionFactory = sessionFactory;
            _loggerFactory = loggerFactory;
            _defaultNetworkProvider = defaultNetworkProvider;
            _logger = _loggerFactory.CreateLogger<SessionManager>();
            _configIdMap = new PersistentConfigFileIdMap(_parameterStore);

            Directory.CreateDirectory(ExtraConfigDirPath);

            _logger.LogInformation($"Default configuration file is '{DefaultConfigPath}'");
            _logger.LogInformation($"Extra configuration directory is '{ExtraConfigDirPath}'");

            _defaultConfigWatcher = new FileSystemWatcher(Path.GetDirectoryName(DefaultConfigPath))
            {
                Filter = Path.GetFileName(DefaultConfigPath)
            };
            _extraconfigWatcher = new FileSystemWatcher(ExtraConfigDirPath)
            {
                Filter = "*.json"
            };
            HookFileWatcherEvents(_defaultConfigWatcher);
            HookFileWatcherEvents(_extraconfigWatcher);
            instance = this;
        }

        // made internal for testing
        internal readonly ConcurrentDictionary<string, ISession> ConfigPathToSessionMap = new ConcurrentDictionary<string, ISession>();

        internal int ConfigChangePollingIntervalMs { get; set; } = 10 * 1000;

        public static ISession LaunchValidatedSession(string configPath)
        {
            if (instance == null)
                throw new Exception("SessionManager singleton instance has not been initialized.");

            if (instance._cts.IsCancellationRequested)
                throw new Exception("SessionManager is shutting down.");

            if (instance._disposed)
                throw new Exception("SessionManager has been disposed.");

            var session = instance.LaunchSession(configPath, false, instance._configIdMap);
            instance.ConfigPathToSessionMap[configPath] = session;
            return session;
        }

        private void HookFileWatcherEvents(FileSystemWatcher watcher)
        {
            watcher.IncludeSubdirectories = false;
            watcher.NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.CreationTime | NotifyFilters.FileName;

            watcher.Created += OnExtraConfigChange;
            watcher.Renamed += OnExtraConfigChange;
            watcher.Changed += OnExtraConfigChange;
            watcher.Deleted += OnExtraConfigChange;
        }

        private void UnhookFileWatcherEvents(FileSystemWatcher watcher)
        {
            watcher.Created -= OnExtraConfigChange;
            watcher.Renamed -= OnExtraConfigChange;
            watcher.Changed -= OnExtraConfigChange;
            watcher.Deleted -= OnExtraConfigChange;
        }

        private void OnExtraConfigChange(object sender, FileSystemEventArgs e)
        {
            _logger.LogDebug("Change type '{0}' detected in file '{1}'", e.ChangeType, e.FullPath);
            Interlocked.Exchange(ref _configChanged, 1);
        }

        private async Task ConfigChangePoller()
        {
            var extraConfigs = new HashSet<string>();

            while (!_cts.Token.IsCancellationRequested)
            {
                try
                {
                    var shouldPoll = Interlocked.Exchange(ref _configChanged, 0) > 0;
                    if (shouldPoll)
                    {
                        // poll the default session first
                        PollConfig(DefaultConfigPath, true, _configIdMap);

                        // get the set of extra config files
                        extraConfigs.Clear();
                        foreach (var f in Directory.EnumerateFiles(ExtraConfigDirPath, "*.json", SearchOption.TopDirectoryOnly))
                        {
                            if (ShouldIgnoreConfigFile(f)) continue;
                            extraConfigs.Add(f);
                        }

                        // List the sessions that are currently running but are no longer present in the list of configuration files.
                        // Exclude configurations that have been loaded from another location, since these are managed by an external component.
                        var sessionsToStop = ConfigPathToSessionMap
                            .Where(kv => kv.Value.Id != DefaultSessionId
                                && !extraConfigs.Contains(kv.Key)
                                && kv.Key.StartsWith(ExtraConfigDirPath))
                            .Select(kv => kv.Key)
                            .ToList(); // Force the enumerable into a List.

                        foreach (var configPath in sessionsToStop)
                        {
                            if (ConfigPathToSessionMap.TryRemove(configPath, out var removedSession))
                            {
                                TerminateSession(removedSession, false);
                            }
                        }

                        // poll and start sessions as neccessary
                        foreach (var configFile in extraConfigs)
                        {
                            if (_cts.Token.IsCancellationRequested) break;
                            PollConfig(configFile, false, _configIdMap);
                        }
                    }

                    await Task.Delay(ConfigChangePollingIntervalMs, _cts.Token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(0, ex, "Error watching config file changes.");
                }
            }

            _logger.LogInformation($"Stopped watching for config file changes.");
        }

        /// <summary>
        /// Check the config file, if it has changed, start a new session and stop the old one.
        /// </summary>
        /// <returns>True iff a new session has been successfully started.</returns>
        private void PollConfig(string configPath, bool isDefault, PersistentConfigFileIdMap configIdMap)
        {
            // Don't process any files outside of the expected directories.
            // This allows us to support validated sessions by placing them in different directories.
            if (!configPath.StartsWith(DefaultConfigPath) && !configPath.StartsWith(ExtraConfigDirPath)) return;

            try
            {
                var lastWriteTime = File.GetLastWriteTime(configPath);
                if (ConfigPathToSessionMap.TryGetValue(configPath, out var existingSession))
                {
                    // the session is running
                    if (lastWriteTime <= existingSession.StartTime)
                    {
                        // the config file hasn't changed, nothing to do here
                        return;
                    }

                    _logger.LogInformation($"Config file '{configPath}' has changed.");

                    // stop the existing session
                    TerminateSession(existingSession, false);
                    ConfigPathToSessionMap.TryRemove(configPath, out existingSession);
                }
                else
                {
                    _logger.LogInformation($"Config file '{configPath}' is added.");
                }

                // start new session
                var newSession = LaunchSession(configPath, isDefault, configIdMap);
                ConfigPathToSessionMap[configPath] = newSession;
                PublishCounters(string.Empty, MetricsConstants.CATEGORY_PROGRAM, CounterTypeEnum.CurrentValue, new Dictionary<string, MetricValue>()
                {
                    { MetricsConstants.CONFIGS_LOADED, new MetricValue(1) },
                    { MetricsConstants.CONFIGS_FAILED_TO_LOAD, new MetricValue(0) },
                });
            }
            catch (SessionLaunchedException sessionLaunchEx)
            {
                _logger.LogError(0, sessionLaunchEx.InnerException, $"Error lauching session '{sessionLaunchEx.ConfigPath}'");
                PublishCounters(string.Empty, MetricsConstants.CATEGORY_PROGRAM, CounterTypeEnum.CurrentValue, new Dictionary<string, MetricValue>()
                {
                    { MetricsConstants.CONFIGS_LOADED, new MetricValue(0) },
                    { MetricsConstants.CONFIGS_FAILED_TO_LOAD, new MetricValue(1) },
                });
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(0, ex, $"Error while monitoring config file '{configPath}'");
                return;
            }
        }

        private string ExtraConfigDirPath => _parameterStore.GetParameter(HostingUtility.ExtraConfigurationDirectoryPathKey);

        private string DefaultConfigPath => _parameterStore.GetParameter(HostingUtility.DefaultConfigurationPathKey);

        internal Task StartAsync()
        {
            _defaultConfigWatcher.EnableRaisingEvents = true;
            _extraconfigWatcher.EnableRaisingEvents = true;

            var defaultSession = LaunchSession(DefaultConfigPath, true, null);
            ConfigPathToSessionMap[DefaultConfigPath] = defaultSession;

            _configPoller = ConfigChangePoller();

            return Task.CompletedTask;
        }

        internal async Task StopAsync()
        {
            _extraconfigWatcher.EnableRaisingEvents = false;
            _defaultConfigWatcher.EnableRaisingEvents = false;

            if (_configPoller == null)
            {
                // the poller never started, so nothing to do
                return;
            }

            // cancel the cts to signal the config handler to stop
            _cts.Cancel();
            await _configPoller;

            // Terminate all sessions, including ones that are managed externally.
            foreach (var item in ConfigPathToSessionMap)
            {
                TerminateSession(item.Value, true);
            }
        }

        private ISession LaunchSession(string configPath, bool isDefault, PersistentConfigFileIdMap configFileIdMap)
        {
            bool isValidated = !configPath.StartsWith(DefaultConfigPath) && !configPath.StartsWith(ExtraConfigDirPath);
            try
            {
                Guard.ArgumentNotNullOrEmpty(configPath, nameof(configPath));

                var startTime = File.GetLastWriteTime(configPath);

                if (isDefault)
                {
                    Debug.Assert(configPath == DefaultConfigPath,
                        $"Default config path should be {DefaultConfigPath}, but {configPath} is initialized");
                }

                var id = isDefault
                    ? DefaultSessionId
                    : GetIdOfConfigFile(configPath, configFileIdMap);

                var config = new ConfigurationBuilder()
                  .AddJsonFile(configPath, optional: false, reloadOnChange: false)
                  .Build();

                if (string.IsNullOrWhiteSpace(config[ConfigConstants.CONFIG_DESCRIPTIVE_NAME]))
                {
                    config[ConfigConstants.CONFIG_DESCRIPTIVE_NAME] = isDefault
                        ? "default"
                        : Path.GetFileNameWithoutExtension(configPath);
                }

                // If the configuration has it's own credentials, use them.
                // Otherwise, use the creds from the default config file.
                // If this is the default config, set the private field so subsequent configs can use it.
                var credSection = config.GetSection("Credentials");
                var configHasCredentials = credSection.GetChildren().Any();
                if (configHasCredentials)
                {
                    _logger.LogDebug("Configuration {0} has credentials, these will be used for sources.", id);
                    if (isDefault)
                        _defaultCredentialConfig = credSection;
                }
                else
                {
                    _logger.LogDebug("Configuration {0} has no credentials, using default credential section for sources.", id);
                    credSection = _defaultCredentialConfig;
                }

                var session = _sessionFactory.Create(id, config, startTime,
                    _typeLoader, _parameterStore,
                    _loggerFactory, _defaultNetworkProvider, credSection, isValidated);

                // start the session
                _logger.LogDebug("Starting session {0}", id);
                session.Start();

                if (!isDefault)
                {
                    configFileIdMap[configPath] = id;
                }

                return session;
            }
            catch (Exception ex)
            {
                throw new SessionLaunchedException(configPath, ex);
            }
        }

        private void TerminateSession(ISession session, bool serviceStopping)
        {
            _logger.LogDebug("Stopping session {0}", session.Id);
            try
            {
                session.Stop(serviceStopping);
                session.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogError(0, ex, $"Error while stopping session {session.Id}");
            }
        }

        /// <summary>
        /// Determine the ID of a Session based on the config file path.
        /// </summary>
        /// <param name="configPath">Path of the config file</param>
        /// <returns>ID of the <see cref="ISession"/></returns>
        private int GetIdOfConfigFile(string configPath, PersistentConfigFileIdMap configFileIdMap)
        {
            if (configFileIdMap.TryGetValue(configPath, out int id))
                return id;

            return configFileIdMap.MaxUsedId + 1;
        }

        private void PublishCounters(string id, string category, CounterTypeEnum counterType, Dictionary<string, MetricValue> counters)
        {
            ConfigPathToSessionMap[DefaultConfigPath]
                .PublishServiceLevelCounter(id, category, counterType, counters);
        }

        private bool ShouldIgnoreConfigFile(string file)
        {
            // Config files must have a .json file extension.
            if (Path.GetExtension(file) != ".json") return true;

            // Config files cannot contain whitespaces or characters in the forbiddenChars list.
            return Path.GetFileNameWithoutExtension(file)
                .Count(c => char.IsWhiteSpace(c) || _forbiddenChars.Contains(c)) > 0;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _cts.Dispose();

                    UnhookFileWatcherEvents(_defaultConfigWatcher);
                    UnhookFileWatcherEvents(_extraconfigWatcher);
                    _defaultConfigWatcher.Dispose();
                    _extraconfigWatcher.Dispose();
                }

                _disposed = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
