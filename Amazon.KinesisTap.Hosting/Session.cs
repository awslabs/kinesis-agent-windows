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
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;

namespace Amazon.KinesisTap.Hosting
{
    /// <summary>
    /// Default implementation of a session
    /// </summary>
    internal class Session : ISession
    {
        private const string KINESISTAP_METRICS_SOURCE = "_KinesisTapMetricsSource";
        private const string TELEMETRICS = "Telemetrics";

        private readonly IFactoryCatalog<ISource> _sourceFactoryCatalog;
        private readonly IFactoryCatalog<IEventSink> _sinkFactoryCatalog;
        private readonly IFactoryCatalog<ICredentialProvider> _credentialProviderFactoryCatalog;
        private readonly IFactoryCatalog<IGenericPlugin> _genericPluginFactoryCatalog;
        private readonly IFactoryCatalog<IPipe> _pipeFactoryCatalog;
        private readonly IFactoryCatalog<IRecordParser> _recordParserCatalog;

        private readonly IParameterStore _parameterStore;
        private readonly IDictionary<string, ISource> _sources = new Dictionary<string, ISource>();
        private readonly IDictionary<string, ISink> _sinks = new Dictionary<string, ISink>();
        private readonly IDictionary<string, ICredentialProvider> _credentialProviders = new Dictionary<string, ICredentialProvider>();
        private readonly IList<IDisposable> _subscriptions = new List<IDisposable>();
        private readonly IList<IGenericPlugin> _plugins = new List<IGenericPlugin>();
        private readonly NetworkStatus _networkStatus;
        private readonly IConfiguration _config;
        private readonly IServiceProvider _services;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger _logger;
        private readonly IBookmarkManager _bookmarkManager;
        private readonly IMetrics _metrics;

        private CancellationTokenSource _stopTokenSource;

        public Session(
            string name,
            IConfiguration config,
            IMetrics metrics,
            IServiceProvider services,
            bool validated)
        {
            Name = name;
            StartTime = DateTime.Now;
            IsValidated = validated;

            if (IsDefault && validated)
            {
                throw new ArgumentException("The default session cannot be validated");
            }

            _config = config;
            _metrics = metrics;
            _services = services;
            _loggerFactory = services.GetService<ILoggerFactory>();
            _parameterStore = services.GetService<IParameterStore>();
            _networkStatus = new NetworkStatus(services.GetService<INetworkStatusProvider>());
            _logger = _loggerFactory.CreateLogger(DisplayName);
            _logger.LogDebug("Configuration is validated: {0}", IsValidated);
            _bookmarkManager = new FileBookmarkManager(Utility.GetBookmarkDirectory(name),
                _loggerFactory.CreateLogger($"{DisplayName}:{nameof(IBookmarkManager)}"));

            var factoryCatalogs = services.GetService<FactoryCatalogs>();
            _sourceFactoryCatalog = factoryCatalogs.SourceFactoryCatalog;
            _sinkFactoryCatalog = factoryCatalogs.SinkFactoryCatalog;
            _credentialProviderFactoryCatalog = factoryCatalogs.CredentialProviderFactoryCatalog;
            _genericPluginFactoryCatalog = factoryCatalogs.GenericPluginFactoryCatalog;
            _pipeFactoryCatalog = factoryCatalogs.PipeFactoryCatalog;
            _recordParserCatalog = factoryCatalogs.RecordParserCatalog;
        }

        /// <inheritdoc/>
        public DateTime StartTime { get; }

        /// <inheritdoc/>
        public string Name { get; }

        /// <inheritdoc/>
        public string DisplayName => IsDefault ? "default" : Name;

        /// <inheritdoc/>
        public bool IsValidated { get; }

        /// <inheritdoc/>
        public bool Disposed { get; private set; }

        /// <inheritdoc/>
        public bool IsDefault => Name is null;

        /// <inheritdoc/>
        public async Task StartAsync(CancellationToken stopToken)
        {
            if (Disposed)
            {
                throw new ObjectDisposedException(DisplayName);
            }

            // create a token that is cancelled when either _stopTokenSource or stopToken is cancelled
            _stopTokenSource = CancellationTokenSource.CreateLinkedTokenSource(stopToken);

            await _bookmarkManager.StartAsync(_stopTokenSource.Token);

            LoadCredentialProviders();

            await LoadGenericPlugins(_stopTokenSource.Token);

            await LoadBuiltInSinks(_stopTokenSource.Token);

            await LoadEventSinks(_stopTokenSource.Token);

            (var sourcesLoaded, var sourcesFailed) = LoadEventSources();

            await LoadPipes(_stopTokenSource.Token);

            await StartEventSources(sourcesLoaded, sourcesFailed, _stopTokenSource.Token);

            _logger.LogInformation("Started");
        }

        /// <inheritdoc/>
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            _stopTokenSource?.Cancel();

            await StopPluginsAsync(_sources.Values.Where(s => s != _metrics), cancellationToken);

            await StopPluginsAsync(_sinks.Values, cancellationToken);

            await StopPluginsAsync(_plugins, cancellationToken);

            await _bookmarkManager.StopAsync(cancellationToken);

            _sinks.Clear();
            _networkStatus.ResetNetworkStatusProviders();
            _logger.LogInformation("Stopped");
        }

        private async Task StopPluginsAsync(IEnumerable<IPlugIn> plugins, CancellationToken cancellationToken)
        {
            var tasksAndPlugins = plugins.Select(p => (p, p.StopAsync(cancellationToken).AsTask())).ToArray();
            await Task.WhenAll(tasksAndPlugins.Select(t => t.Item2));

            // all the tasks might have finished because cancel token threw
            cancellationToken.ThrowIfCancellationRequested();

            foreach (var item in tasksAndPlugins)
            {
                if (item.Item2.IsFaulted && item.Item2.Exception?.InnerException is not null)
                {
                    _logger.LogError(item.Item2.Exception.InnerException, "Error stopping {0}", item.Item1.Id);
                }
            }
        }

        private void LoadCredentialProviders()
        {
            var credentialsSection = _config.GetSection("Credentials");
            var credentialStarted = 0;
            var credentialFailed = 0;

            if (credentialsSection is null || !credentialsSection.GetChildren().Any())
            {
                //this config file contains no credentials section
                return;
            }

            var credentialSections = credentialsSection.GetChildren();
            foreach (var credentialSection in credentialSections)
            {
                var id = credentialSection[ConfigConstants.ID];
                if (_credentialProviders.ContainsKey(id))
                {
                    credentialFailed++;
                    continue;
                }

                var credentialType = credentialSection[ConfigConstants.CREDENTIAL_TYPE];
                var factory = _credentialProviderFactoryCatalog.GetFactory(credentialType);
                if (factory != null)
                {
                    try
                    {
                        var credentialProvider = factory.CreateInstance(credentialType, CreatePlugInContext(credentialSection));
                        credentialProvider.Id = id;
                        _credentialProviders[id] = credentialProvider;
                        credentialStarted++;
                        _logger.LogDebug($"Created cred provider {credentialType} Id {id}");
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Unable to load credential {0}", id);
                        credentialFailed++;
                    }
                }
                else
                {
                    _logger?.LogError("Credential Type {0} is not recognized.", credentialType);
                    credentialFailed++;
                }

                _metrics.PublishCounters(string.Empty, MetricsConstants.CATEGORY_PROGRAM, CounterTypeEnum.CurrentValue, new Dictionary<string, MetricValue>()
                {
                    { MetricsConstants.SINKS_STARTED, new MetricValue(credentialStarted) },
                    { MetricsConstants.SINKS_FAILED_TO_START, new MetricValue(credentialFailed) }
                });
            }
        }

        private (int sourceLoaded, int sourcesFailed) LoadEventSources()
        {
            //metrics source
            _sources[KINESISTAP_METRICS_SOURCE] = _metrics;

            var sourcesSection = _config.GetSection("Sources");
            var sourceSections = sourcesSection.GetChildren();
            var sourcesLoaded = 0;
            var sourcesFailed = 0;

            foreach (var sourceSection in sourceSections)
            {
                var id = _config.GetChildConfig(sourceSection.Path, ConfigConstants.ID);
                var sourceType = _config.GetChildConfig(sourceSection.Path, ConfigConstants.SOURCE_TYPE);
                var factory = _sourceFactoryCatalog.GetFactory(sourceType);
                if (factory != null)
                {
                    try
                    {
                        var source = factory.CreateInstance(sourceType, CreatePlugInContext(sourceSection));
                        source.Id = id;
                        _sources[id] = source;
                        sourcesLoaded++;
                    }
                    catch (Exception ex)
                    {
                        sourcesFailed++;
                        _logger.LogError(ex, "Unable to load event source {0}", id);
                    }
                }
                else
                {
                    _logger.LogError("Source Type {0} is not recognized.", sourceType);
                }
            }
            return (sourcesLoaded, sourcesFailed);
        }

        private async ValueTask StartEventSources(int sourceLoaded, int sourcesFailed, CancellationToken stopToken)
        {
            var sourceStarted = 0;
            foreach (var source in _sources.Values)
            {
                try
                {
                    await source.StartAsync(stopToken);
                    sourceStarted++;
                }
                catch (Exception ex)
                {
                    sourceLoaded--;
                    sourcesFailed++;
                    _logger.LogError(ex, "Unable start event source {0}", source.Id);
                }
            }
            _metrics.PublishCounters(string.Empty, MetricsConstants.CATEGORY_PROGRAM, CounterTypeEnum.CurrentValue, new Dictionary<string, MetricValue>()
            {
                { MetricsConstants.SOURCES_STARTED, new MetricValue(sourceStarted) },
                { MetricsConstants.SOURCES_FAILED_TO_START, new MetricValue(sourcesFailed) }
            });
        }

        private async Task LoadEventSinks(CancellationToken stopToken)
        {
            var sinksSection = _config.GetSection("Sinks");
            var sinkSections = sinksSection.GetChildren();
            var sinksStarted = 0;
            var sinksFailed = 0;
            foreach (var sinkSection in sinkSections)
            {
                var id = _config.GetChildConfig(sinkSection.Path, ConfigConstants.ID);
                var sinkType = _config.GetChildConfig(sinkSection.Path, ConfigConstants.SINK_TYPE);
                var factory = _sinkFactoryCatalog.GetFactory(sinkType);
                if (factory != null)
                {
                    IEventSink sink = null;
                    try
                    {
                        if (string.IsNullOrWhiteSpace(id))
                        {
                            throw new Exception("Sink id is required.");
                        }

                        sink = factory.CreateInstance(sinkType, CreatePlugInContext(sinkSection));
                        await sink.StartAsync(stopToken);
                        _sinks[sink.Id] = sink;
                        sinksStarted++;
                    }
                    catch (Exception ex)
                    {
                        sinksFailed++;
                        _logger.LogError(ex, $"Unable to load event sink {id}");
                    }
                }
                else
                {
                    sinksFailed++;
                    _logger.LogError("Sink Type {0} is not recognized.", sinkType);
                }
            }
            _metrics.PublishCounters(string.Empty, MetricsConstants.CATEGORY_PROGRAM, CounterTypeEnum.CurrentValue, new Dictionary<string, MetricValue>()
            {
                { MetricsConstants.SINKS_STARTED, new MetricValue(sinksStarted) },
                { MetricsConstants.SINKS_FAILED_TO_START, new MetricValue(sinksFailed) }
            });
        }

        private async ValueTask LoadPipes(CancellationToken stopToken)
        {
            var pipesSection = _config.GetSection("Pipes");
            var pipeSections = pipesSection.GetChildren();
            var pipesConnected = 0;
            var pipesFailedToConnect = 0;
            foreach (var pipeSection in pipeSections)
            {
                if (await LoadPipe(pipeSection, stopToken))
                {
                    pipesConnected++;
                }
                else
                {
                    pipesFailedToConnect++;
                }
            }

            //If telemetry is redirected, connect it to sink
            IConfiguration telemetricsSection = _config.GetSection(TELEMETRICS);
            var redirectToSinkId = telemetricsSection[ConfigConstants.REDIRECT_TO_SINK_ID];
            if (!string.IsNullOrWhiteSpace(redirectToSinkId))
            {
                if (ConnectTelemetry(redirectToSinkId))
                {
                    pipesConnected++;
                }
                else
                {
                    pipesFailedToConnect++;
                }
            }

            _metrics.PublishCounters(string.Empty, MetricsConstants.CATEGORY_PROGRAM, CounterTypeEnum.CurrentValue, new Dictionary<string, MetricValue>()
            {
                { MetricsConstants.PIPES_CONNECTED, new MetricValue(pipesConnected) },
                { MetricsConstants.PIPES_FAILED_TO_CONNECT, new MetricValue(pipesFailedToConnect) }
            });
        }

        private bool ConnectTelemetry(string redirectToSinkId)
        {
            try
            {
                if (!_sinks.TryGetValue(redirectToSinkId, out var sink))
                {
                    _logger.LogError($"Sink {redirectToSinkId} not found for telemetry.");
                    return false;
                }
                else
                {
                    ((IEventSource)_sources[ConfigConstants.TELEMETRY_CONNECTOR]).Subscribe((IEventSink)sink);
                    return true;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unable to connect telemetry to Sink {0}", redirectToSinkId);
                return true;
            }
        }

        private async ValueTask<bool> LoadPipe(IConfigurationSection config, CancellationToken stopToken)
        {
            var id = config[ConfigConstants.ID];
            var sourceRef = config["SourceRef"];
            var sinkRef = config["SinkRef"];
            var pipeType = config[ConfigConstants.TYPE];
            try
            {
                if (string.IsNullOrEmpty(sinkRef))
                {
                    _logger.LogError($"SinkRef is required for pipe id {id}");
                    return false;
                }

                if (!_sinks.TryGetValue(sinkRef, out var sink))
                {
                    _logger.LogError($"SinkRef {sinkRef} not found for pipe id {id}");
                    return false;
                }

                if (!string.IsNullOrEmpty(sourceRef))
                {
                    if (!_sources.TryGetValue(sourceRef, out var source))
                    {
                        _logger.LogError($"Unable to connect source {sourceRef} to sink {sinkRef}.");
                        _logger.LogError($"SourceRef {sourceRef} not found for pipe id {id}");
                        return false;
                    }

                    if (source is IEventSource eventSource && sink is IEventSink eventSink)
                    {
                        if (string.IsNullOrWhiteSpace(pipeType)) //No type specified, just connect sink to source directly
                        {
                            _subscriptions.Add(eventSource.Subscribe(eventSink));
                        }
                        else
                        {
                            return await LoadPipe(config, id, pipeType, eventSource, eventSink, stopToken);
                        }
                    }
                    else if (source is IDataSource<object> dataSource && sink is IDataSink<object> dataSink)
                    {
                        dataSink.RegisterDataSource(dataSource);
                    }
                    else
                    {
                        _logger.LogError($"Unable to connect SourceRef {sourceRef} to SinkRef {sinkRef} for pipe id {id}");
                        return false;
                    }
                }

                _logger.LogInformation($"Connected source {sourceRef} to sink {sinkRef}");
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unable to connect source {0} to sink {1}", sourceRef, sinkRef);
                return false;
            }
        }

        private async ValueTask<bool> LoadPipe(IConfigurationSection config, string id, string pipeType,
            IEventSource eventSource, IEventSink eventSink, CancellationToken stopToken)
        {
            var factory = _pipeFactoryCatalog.GetFactory(pipeType);
            if (factory != null)
            {
                try
                {
                    var plugInContext = CreatePlugInContext(config);
                    plugInContext.ContextData[PluginContext.SOURCE_TYPE] = eventSource.GetType();
                    plugInContext.ContextData[PluginContext.SOURCE_OUTPUT_TYPE] = eventSource.GetOutputType();
                    plugInContext.ContextData[PluginContext.SINK_TYPE] = eventSink.GetType();
                    var pipe = factory.CreateInstance(pipeType, plugInContext);
                    await pipe.StartAsync(stopToken);
                    _subscriptions.Add(eventSource.Subscribe(pipe));
                    _subscriptions.Add(pipe.Subscribe(eventSink));
                    return true;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Unable to load event pipe {0}", id);
                    return false;
                }
            }
            else
            {
                _logger.LogError("Pipe Type {0} is not recognized.", pipeType);
                return false;
            }
        }

        private async ValueTask LoadBuiltInSinks(CancellationToken stopToken)
        {
            if (OperatingSystem.IsWindows() && IsDefault)
            {
                await CreatePerformanceCounterSink(stopToken);
            }

            await CreateTelemetricsSink(stopToken);
        }

        private async ValueTask CreateTelemetricsSink(CancellationToken stopToken)
        {
            try
            {
                var telemetricsSection = _config.GetSection(TELEMETRICS);

                // check if telemetric is turned off
                if (bool.TryParse(telemetricsSection["off"], out var telemetricsOff) && telemetricsOff)
                {
                    return;
                }

                // if this is a non-default config and there's no telemetry redirection, just skip
                // because we don't want the same metrics to be sent to our metric endpoint twice
                if (telemetricsSection[ConfigConstants.REDIRECT_TO_SINK_ID] is null && !IsDefault)
                {
                    return;
                }

                var factory = _sinkFactoryCatalog.GetFactory(TELEMETRICS);
                if (factory != null)
                {
                    var sinkId = telemetricsSection["Id"] ?? $"_{TELEMETRICS}";
                    telemetricsSection["Id"] = sinkId;
                    var context = CreatePlugInContext(telemetricsSection);

                    var sink = factory.CreateInstance(TELEMETRICS, context);
                    await sink.StartAsync(stopToken);
                    _sinks[sinkId] = sink;
                    _subscriptions.Add(_metrics.Subscribe(sink));
                    if (context.ContextData.TryGetValue(ConfigConstants.TELEMETRY_CONNECTOR, out var telemetryConnector))
                    {
                        _sources[ConfigConstants.TELEMETRY_CONNECTOR] = (ISource)telemetryConnector;
                    }

                    _logger.LogInformation("Created telemetrics sink");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"Unable to load telemetrics. Error: {ex.Message}"); //Only send a brief error message at Error level
                _logger.LogDebug($"{ex.ToMinimized()}"); //Send the detailed message if the user has Debug level on.
            }
        }

        private async ValueTask CreatePerformanceCounterSink(CancellationToken stopToken)
        {
            const string PERFORMANCE_COUNTER = "PerformanceCounter";
            try
            {
                var factory = _sinkFactoryCatalog.GetFactory(PERFORMANCE_COUNTER);
                if (factory != null)
                {
                    IConfiguration perfCounterSection = _config.GetSection("PerformanceCounter");
                    var sink = factory.CreateInstance(PERFORMANCE_COUNTER, CreatePlugInContext(perfCounterSection));
                    await sink.StartAsync(stopToken);
                    _sinks["_" + PERFORMANCE_COUNTER] = sink;
                    _subscriptions.Add(_metrics.Subscribe(sink));
                }

                _logger.LogInformation("Created performance counter sink");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unable to load PerformanceCounter sink");
            }
        }

        private IPlugInContext CreatePlugInContext(IConfiguration config)
        {
            // Previously, all sources, sinks, pipes and plugins all used the same instance of a logger (the LogManager logger).
            // This is confusing when reading the logs because it means that all log entries appear to come from the same class.
            // This new logic will check the config for an "Id" property and if found, will create a new logger with that Id as the name.
            // If there is no "Id" property specified, it will use the default logger (i.e. the LogManager logger instance).
            // This means that every line in the log will contain the Id of the source/sink/pipe/plugin that it originated from.
            var plugInLogger = string.IsNullOrWhiteSpace(config["Id"])
                ? _logger
                : _loggerFactory.CreateLogger($"{DisplayName}:{config["Id"]}");
            var plugInContext = new PluginContext(config, plugInLogger, _metrics, _bookmarkManager, _credentialProviders, _parameterStore)
            {
                NetworkStatus = _networkStatus,
                SessionName = Name,
                Validated = IsValidated,
                Services = _services
            };
            plugInContext.ContextData[PluginContext.PARSER_FACTORIES] = new ReadOnlyFactoryCatalog<IRecordParser>(_recordParserCatalog); //allow plug-ins access a list of parsers
            return plugInContext;
        }

        private async ValueTask LoadGenericPlugins(CancellationToken stopToken)
        {
            var pluginsSection = _config.GetSection("Plugins");
            var pluginSections = pluginsSection.GetChildren();
            var pluginsStarted = 0;
            var pluginsFailedToStart = 0;
            foreach (var pluginSection in pluginSections)
            {
                if (await LoadPlugin(pluginSection, stopToken))
                {
                    pluginsStarted++;
                }
                else
                {
                    pluginsFailedToStart++;
                }
            }
            _metrics.PublishCounters(string.Empty, MetricsConstants.CATEGORY_PROGRAM, CounterTypeEnum.CurrentValue, new Dictionary<string, MetricValue>()
            {
                { MetricsConstants.PLUGINS_STARTED, new MetricValue(pluginsStarted) },
                { MetricsConstants.PLUGINS_FAILED_TO_START, new MetricValue(pluginsFailedToStart) }
            });
        }

        private async ValueTask<bool> LoadPlugin(IConfigurationSection pluginSection, CancellationToken stopToken)
        {
            var pluginType = _config.GetChildConfig(pluginSection.Path, ConfigConstants.TYPE);
            var factory = _genericPluginFactoryCatalog.GetFactory(pluginType);
            if (factory != null)
            {
                try
                {
                    var plugin = factory.CreateInstance(pluginType, CreatePlugInContext(pluginSection));
                    await plugin.StartAsync(stopToken);
                    _plugins.Add(plugin);
                    _logger.LogInformation($"Plugin type {pluginType} started.");
                    if (plugin is INetworkStatusProvider networkStatusProvider)
                    {
                        _networkStatus.RegisterNetworkStatusProvider(networkStatusProvider);
                        _logger.LogInformation($"Registered network status provider {plugin.Id}");
                    }

                    return true;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Unable to load plugin type {0}", pluginType);
                    return false;
                }
            }
            else
            {
                _logger.LogError("Plugin Type {0} is not recognized.", pluginType);
                return false;
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (Disposed)
            {
                return;
            }

            if (disposing)
            {
                foreach (var subscription in _subscriptions)
                {
                    try
                    {
                        subscription?.Dispose();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"Error stopping subscription");
                    }
                }

                foreach (var source in _sources.Values)
                {
                    if (source != _metrics && source is IDisposable disposableSource)
                    {
                        disposableSource.Dispose();
                    }
                }

                foreach (var sink in _sinks.Values)
                {
                    if (sink is IDisposable disposableSink)
                    {
                        disposableSink.Dispose();
                    }
                }

                foreach (var plugin in _plugins)
                {
                    if (plugin is IDisposable disposablePlugin)
                    {
                        disposablePlugin.Dispose();
                    }
                }

                _stopTokenSource?.Dispose();
            }

            Disposed = true;
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
