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
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NLog.Extensions.Logging;

namespace Amazon.KinesisTap.Hosting
{
    /// <summary>
    /// Host builder for KinesisTap service
    /// </summary>
    public static class KinesisTapHostBuilder
    {
        /// <summary>
        /// Create an instance of <see cref="IHostBuilder"/>
        /// </summary>
        public static IHostBuilder Create(string[] args)
        {
            // set 'ComputerName' environment variable if not on Windows
            if (!OperatingSystem.IsWindows())
            {
                SetComputerNameEnvironmentVariable();
            }

            var builder = new HostBuilder()
                .ConfigureServices((hostContext, services) =>
                {
                    services.AddSingleton<ITypeLoader>(_ => new PluginLoader());
                    services.AddSingleton(_ =>
                    {
                        IParameterStore store = OperatingSystem.IsWindows()
                            ? new RegistryParameterStore()
                            : new SimpleParameterStore(Path.Combine(Utility.GetProfileDirectory(), "config"));

                        store.StoreConventionalValues();
                        return store;
                    });
                    services.AddSingleton<INetworkStatusProvider>(services =>
                        new DefaultNetworkStatusProvider(services.GetService<ILogger<DefaultNetworkStatusProvider>>()));
                    services.AddSingleton<ISessionManager, SessionManager>();
                    services.AddSingleton(_ => new FactoryCatalogs
                    {
                        SourceFactoryCatalog = new FactoryCatalog<ISource>(),
                        SinkFactoryCatalog = new FactoryCatalog<IEventSink>(),
                        CredentialProviderFactoryCatalog = new FactoryCatalog<ICredentialProvider>(),
                        GenericPluginFactoryCatalog = new FactoryCatalog<IGenericPlugin>(),
                        PipeFactoryCatalog = new FactoryCatalog<IPipe>(),
                        RecordParserCatalog = new FactoryCatalog<IRecordParser>()
                    });
                    services.AddSingleton<IMetrics>(services
                        => new KinesisTapMetricsSource("_KinesisTapMetricsSource", services.GetService<ILogger<IMetrics>>()));
                    services.AddSingleton<ISessionFactory, DefaultSessionFactory>();
                    services.AddHostedService<Worker>();
                })
                .ConfigureLogging(logging =>
                {
                    // this is important so that NLog does not stop logging when SIGTERM is catched
                    NLog.LogManager.AutoShutdown = false;

                    var nlogPath = Path.Combine(Utility.GetNLogConfigDirectory(), HostingUtility.NLogConfigFileName);
                    logging.ClearProviders();

                    // let NLog dictate the minimum level
                    logging.SetMinimumLevel(LogLevel.Trace);
                    logging
                        .AddFilter("Microsoft.Hosting.Lifetime", LogLevel.Warning)
                        .AddFilter("Microsoft", LogLevel.Information)
#if DEBUG
                        .AddConsole()
                        .AddFilter(level => level >= LogLevel.Trace)
#endif
                        .AddNLog(nlogPath);
                });

            return builder;
        }

        private static void SetComputerNameEnvironmentVariable()
        {
            Environment.SetEnvironmentVariable(ConfigConstants.COMPUTER_NAME, Utility.ComputerName);
            Environment.SetEnvironmentVariable("ComputerName", Utility.ComputerName);
            Environment.SetEnvironmentVariable("COMPUTERNAME", Utility.ComputerName);
        }
    }
}
