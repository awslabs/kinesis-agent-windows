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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Hosting
{
    /// <summary>
    /// Global manager for agent sessions
    /// </summary>
    public class SessionManager : ISessionManager
    {
        private static readonly HashSet<char> _forbiddenChars = new() { '>', '<', ':', '\'', '"', '/', '\\', '|', '?', '*' };
        private static readonly string[] _reservedConfigFileNames = new[]
        {
            "default",
            "aem"
        };

        private readonly FactoryCatalogs _factoryCatalogs;
        private readonly ILogger _logger;
        private readonly IParameterStore _parameterStore;
        private readonly FileSystemWatcher _configWatcher;
        private readonly ITypeLoader _typeLoader;
        private readonly ISessionFactory _sessionFactory;
        private readonly IMetrics _metrics;
        private readonly Dictionary<string, MetricValue> _typeLoaderMetrics = new();

        internal int ConfigChangePollingIntervalMs { get; set; } = 5 * 1000;
        private int _configChanged = 1;
        private bool _disposedValue;
        private readonly string _defaultConfigFilePath;
        private readonly string _extraConfigDirPath;

        private Task _configChangePoller;

        public SessionManager(FactoryCatalogs factoryCatalogs, ILoggerFactory loggerFactory,
            IParameterStore parameterStore, ITypeLoader typeLoader, ISessionFactory sessionFactory, IMetrics metrics)
        {
            _parameterStore = parameterStore;
            _factoryCatalogs = factoryCatalogs;
            _logger = loggerFactory.CreateLogger("KinesisTap");

            _defaultConfigFilePath = _parameterStore.GetDefaultConfigFilePath();
            _extraConfigDirPath = _parameterStore.GetExtraConfigDirPath();

            _configWatcher = new FileSystemWatcher(parameterStore.GetConfigDirPath())
            {
                IncludeSubdirectories = true,
                Filter = "*.json",
                NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.CreationTime | NotifyFilters.FileName
            };
            HookFileWatcherEvents(_configWatcher);
            _typeLoader = typeLoader;
            _sessionFactory = sessionFactory;
            LoadFactories();
            _metrics = metrics;
        }

        /// <summary>
        /// Mapping between configuration path to a session. Made internal for testing.
        /// </summary>
        internal readonly ConcurrentDictionary<string, ISession> ConfigPathToSessionMap = new();

        /// <summary>
        /// Maximum time to wait for a session to stop. Made internal for testing.
        /// </summary>
        internal readonly int SessionStopTimeoutMs = 30 * 1000;

        /// <inheritdoc />
        public async Task StartAsync(CancellationToken stopToken)
        {
            await _metrics.StartAsync(stopToken);
            _configWatcher.EnableRaisingEvents = true;
            _configChangePoller = ConfigChangePoller(stopToken);
        }

        /// <inheritdoc />
        public async Task StopAsync(CancellationToken stoppingToken)
        {
            _configWatcher.EnableRaisingEvents = false;
            if (_configChangePoller is not null)
            {
                await _configChangePoller;
            }

            _logger.LogInformation("Stopped watching for config file changes.");

            // Terminate all sessions, including ones that are managed externally.
            var stopTasks = ConfigPathToSessionMap
                .Select(i => TerminateSession(i.Value, stoppingToken))
                .ToArray();

            await Task.WhenAll(stopTasks);

            _logger.LogInformation("All sessions stopped");
        }

        public async Task<ISession> LaunchValidatedSession(string configPath, CancellationToken cancellationToken)
        {
            var session = await LaunchSession(configPath, false, cancellationToken);
            return session;
        }

        private void HookFileWatcherEvents(FileSystemWatcher watcher)
        {
            watcher.Created += OnConfigChange;
            watcher.Renamed += OnConfigChange;
            watcher.Changed += OnConfigChange;
            watcher.Deleted += OnConfigChange;
        }

        private void UnhookFileWatcherEvents(FileSystemWatcher watcher)
        {
            watcher.Created -= OnConfigChange;
            watcher.Renamed -= OnConfigChange;
            watcher.Changed -= OnConfigChange;
            watcher.Deleted -= OnConfigChange;
        }

        private void OnConfigChange(object sender, FileSystemEventArgs e)
        {
            _logger.LogInformation("Change type '{0}' detected in file '{1}'", e.ChangeType, e.FullPath);
            Interlocked.Exchange(ref _configChanged, 1);
        }

        private async Task ConfigChangePoller(CancellationToken stopToken)
        {
            var extraConfigs = new HashSet<string>();

            while (!stopToken.IsCancellationRequested)
            {
                try
                {
                    var shouldPoll = Interlocked.Exchange(ref _configChanged, 0) > 0;
                    if (shouldPoll)
                    {
                        // poll the default session first
                        await PollConfig(_defaultConfigFilePath, true, stopToken);

                        // get the set of extra config files
                        extraConfigs.Clear();
                        if (!Directory.Exists(_extraConfigDirPath))
                        {
                            continue;
                        }

                        var searchExtraConfigs = Directory.EnumerateFiles(_extraConfigDirPath, "*.json", SearchOption.TopDirectoryOnly);
                        foreach (var f in searchExtraConfigs)
                        {
                            if (ShouldIgnoreConfigFile(f))
                            {
                                continue;
                            }
                            extraConfigs.Add(f);
                        }

                        // List the sessions that are currently running but are no longer present in the list of configuration files.
                        // Exclude configurations that have been loaded from another location, since these are managed by an external component.
                        var sessionsToStop = ConfigPathToSessionMap
                            .Where(kv => kv.Key != _defaultConfigFilePath
                                && !extraConfigs.Contains(kv.Key)
                                && kv.Key.StartsWith(_extraConfigDirPath))
                            .Select(kv => kv.Key)
                            .ToList(); // Force the enumerable into a List.

                        foreach (var configPath in sessionsToStop)
                        {
                            stopToken.ThrowIfCancellationRequested();
                            if (ConfigPathToSessionMap.TryRemove(configPath, out var removedSession))
                            {
                                // always stop the session AFTER it's been removed from the mapping
                                await TerminateSession(removedSession, default);
                            }
                        }

                        // poll and start sessions as neccessary
                        foreach (var configFile in extraConfigs)
                        {
                            stopToken.ThrowIfCancellationRequested();
                            await PollConfig(configFile, false, stopToken);
                        }
                    }

                    await Task.Delay(ConfigChangePollingIntervalMs, stopToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error watching config file changes.");
                }
            }
        }

        /// <summary>
        /// Check the config file, if it has changed, start a new session and stop the old one.
        /// </summary>
        /// <returns>True iff a new session has been successfully started.</returns>
        private async Task PollConfig(string configPath, bool isDefault, CancellationToken stopToken)
        {
            // Don't process any files outside of the expected directories.
            // This allows us to support validated sessions by placing them in different directories.
            if (!isDefault && Path.GetDirectoryName(configPath) != _extraConfigDirPath)
            {
                return;
            }

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
                    ConfigPathToSessionMap.TryRemove(configPath, out existingSession);
                    await TerminateSession(existingSession, default);
                }
                else
                {
                    _logger.LogInformation($"Config file '{configPath}' is added.");
                }

                // start new session
                var newSession = await LaunchSession(configPath, isDefault, stopToken);
                ConfigPathToSessionMap[configPath] = newSession;

                _metrics.PublishCounters(string.Empty, MetricsConstants.CATEGORY_PROGRAM, CounterTypeEnum.CurrentValue, new Dictionary<string, MetricValue>()
                {
                    { MetricsConstants.CONFIGS_LOADED, new MetricValue(1) },
                    { MetricsConstants.CONFIGS_FAILED_TO_LOAD, new MetricValue(0) },
                });
            }
            catch (SessionLaunchedException sessionLaunchEx)
            {
                _logger.LogError(0, sessionLaunchEx.InnerException, $"Error lauching session '{sessionLaunchEx.ConfigPath}'");
                _metrics.PublishCounters(string.Empty, MetricsConstants.CATEGORY_PROGRAM, CounterTypeEnum.CurrentValue, new Dictionary<string, MetricValue>()
                {
                    { MetricsConstants.CONFIGS_LOADED, new MetricValue(0) },
                    { MetricsConstants.CONFIGS_FAILED_TO_LOAD, new MetricValue(1) },
                });
                return;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error while monitoring config file '{configPath}'");
                return;
            }
        }

        private async Task<ISession> LaunchSession(string configPath, bool isDefault, CancellationToken stopToken)
        {
            var isValidated = !isDefault && Path.GetDirectoryName(configPath) != _extraConfigDirPath;
            try
            {
                Guard.ArgumentNotNullOrEmpty(configPath, nameof(configPath));

                var startTime = File.GetLastWriteTime(configPath);

                if (isDefault)
                {
                    Debug.Assert(configPath == _defaultConfigFilePath,
                        $"Default config path should be {_defaultConfigFilePath}, but {configPath} is initialized instead");
                }

                var config = new ConfigurationBuilder()
                  .AddJsonFile(configPath, optional: false, reloadOnChange: false)
                  .Build();

                var name = GetSessionName(config, configPath, isDefault);

                var duplicateSession = ConfigPathToSessionMap.FirstOrDefault(p => p.Value.Name == name);
                if (duplicateSession.Key is not null)
                {
                    throw new Exception($"Session with name '{name}' already exists at '{duplicateSession.Key}'");
                }

                var sess = isValidated
                    ? _sessionFactory.CreateValidatedSession(name, config)
                    : _sessionFactory.CreateSession(name, config);

                // start the session
                _logger.LogInformation("Starting session '{0}' with configuration '{1}'", sess.DisplayName, configPath);
                await sess.StartAsync(stopToken);
                _metrics.PublishCounters(string.Empty, MetricsConstants.CATEGORY_PROGRAM, CounterTypeEnum.CurrentValue, _typeLoaderMetrics);

                return sess;
            }
            catch (Exception ex)
            {
                throw new SessionLaunchedException(configPath, ex);
            }
        }

        private static string GetSessionName(IConfiguration config, string configFilePath, bool isDefault)
        {
            if (isDefault)
            {
                return null;
            }
            var name = config[ConfigConstants.NAME];
            if (name is not null)
            {
                return name;
            }

            return Path.GetFileNameWithoutExtension(configFilePath);
        }

        private async Task TerminateSession(ISession session, CancellationToken cancellationToken)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(SessionStopTimeoutMs);

            _logger.LogDebug("Terminating session '{0}'", session.DisplayName);

            await session.StopAsync(cts.Token);
            session.Dispose();
        }

        private static bool ShouldIgnoreConfigFile(string file)
        {
            // Config files must have a .json file extension.
            if (!".json".Equals(Path.GetExtension(file), StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            // Config files must not have the reservered names
            var fileName = Path.GetFileNameWithoutExtension(file);
            foreach (var reservedName in _reservedConfigFileNames)
            {
                if (reservedName.Equals(fileName, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            // Config files must not contain whitespaces or characters in the forbiddenChars list.
            return Path.GetFileNameWithoutExtension(file)
                .Any(c => char.IsWhiteSpace(c) || _forbiddenChars.Contains(c));
        }

        private void LoadFactories()
        {
            _logger.LogInformation("Loading factories");
            LoadFactories(_factoryCatalogs.SinkFactoryCatalog,
                MetricsConstants.SINK_FACTORIES_LOADED, MetricsConstants.SINK_FACTORIES_FAILED_TO_LOAD);

            LoadFactories(_factoryCatalogs.RecordParserCatalog,
                MetricsConstants.PARSER_FACTORIES_LOADED, MetricsConstants.PARSER_FACTORIES_FAILED_TO_LOAD);

            LoadFactories(_factoryCatalogs.SourceFactoryCatalog,
                MetricsConstants.SOURCE_FACTORIES_LOADED, MetricsConstants.SOURCE_FACTORIES_FAILED_TO_LOAD);

            LoadFactories(_factoryCatalogs.PipeFactoryCatalog,
                MetricsConstants.PIPE_FACTORIES_LOADED, MetricsConstants.PIPE_FACTORIES_FAILED_TO_LOAD);

            LoadFactories(_factoryCatalogs.CredentialProviderFactoryCatalog,
                MetricsConstants.CREDENTIAL_PROVIDER_FACTORIES_LOADED, MetricsConstants.CREDENTIAL_PROVIDER_FACTORIES_FAILED_TO_LOAD);

            LoadFactories(_factoryCatalogs.GenericPluginFactoryCatalog,
                MetricsConstants.GENERIC_PLUGIN_FACTORIES_LOADED, MetricsConstants.GENERIC_PLUGIN_FACTORIES_FAILED_TO_LOAD);
        }

        private void LoadFactories<T>(IFactoryCatalog<T> catalog, string loadedMetricKey, string failedMetricKey)
        {
            var loaded = 0;
            var failed = 0;
            try
            {
                var factories = _typeLoader.LoadTypes<IFactory<T>>();
                foreach (var factory in factories)
                {
                    try
                    {
                        factory.RegisterFactory(catalog);
                        loaded++;
                        _logger.LogInformation("Registered factory {0}.", factory);
                    }
                    catch (Exception ex)
                    {
                        failed++;
                        _logger.LogError("Failed to register factory {0}: {1}.", factory, ex.ToMinimized());
                    }
                }
            }
            catch (Exception ex)
            {
                failed++;
                _logger.LogError(ex, "Error discovering IFactory<{0}>.", typeof(T));
                // If the problem discovering the factory is a missing type then provide more details to make debugging easier.
                if (ex is ReflectionTypeLoadException loaderEx)
                {
                    foreach (var innerEx in loaderEx.LoaderExceptions)
                    {
                        _logger.LogError(innerEx, "Loader exception");
                    }
                }
            }

            _typeLoaderMetrics[loadedMetricKey] = new MetricValue(loaded);
            _typeLoaderMetrics[failedMetricKey] = new MetricValue(failed);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    UnhookFileWatcherEvents(_configWatcher);
                    _configWatcher.Dispose();
                }

                _disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
