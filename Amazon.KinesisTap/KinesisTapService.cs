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
namespace Amazon.KinesisTap
{
    using Amazon.KinesisTap.Core;
    using Amazon.KinesisTap.Hosting;
    using Amazon.KinesisTap.Shared;
    using Amazon.KinesisTap.Windows;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.EventLog;
    using NLog.Extensions.Logging;
    using System;
    using System.Diagnostics;
    using System.Reflection;
    using System.ServiceProcess;

    public partial class KinesisTapService : ServiceBase
    {
        const int SERVICE_ACCEPT_PRESHUTDOWN = 0x100;
        const int SERVICE_CONTROL_PRESHUTDOWN = 0xf;
        const int SERVICE_CONTROL_STOP = 0x00000001;

        private readonly KinesisTapServiceManager serviceManager;
        private readonly object shutdownLock = new object();
        private bool isShuttingDown;
        private readonly ILoggerFactory serviceLoggerFactory;
        private readonly ITypeLoader typeLoader = new PluginLoader();
        private readonly IParameterStore parameterStore = new RegistryParameterStore();
        private readonly ILogger logger;

        public KinesisTapService()
        {
            this.parameterStore.StoreConventionalValues();

            // configure logging
            var nlogConfigPath = parameterStore.GetParameter(HostingUtility.NLogConfigPathKey);
            NLog.LogManager.LoadConfiguration(nlogConfigPath);

            this.serviceLoggerFactory = new LoggerFactory()
                .AddEventLog(new EventLogSettings
                {
                    SourceName = KinesisTapServiceManager.ServiceName,
                    LogName = "Application",
                    Filter = (msg, level) => level >= LogLevel.Information
                })
                .AddNLog();

            logger = this.serviceLoggerFactory.CreateLogger<KinesisTapService>();

            this.serviceManager = new KinesisTapServiceManager(this.typeLoader, this.parameterStore,
                this.serviceLoggerFactory.CreateLogger<KinesisTapServiceManager>(),
                new DefaultNetworkStatusProvider());

            // Try to enable pre-shutdown notifications to give KT an early start on shutdown.
            // By enabling this, as soon as a shutdown is triggered in the OS, KT will be notified.
            // https://docs.microsoft.com/en-us/windows/win32/services/service-control-handler-function
            try
            {
                // Unfortunately there's no convenience property for enabling pre-shutdown notifications, so we have to do it via reflection.
                // http://www.sivachandran.in/2012/03/handling-pre-shutdown-notification-in-c.html
                var acceptedCommandsFieldInfo = typeof(ServiceBase).GetField("acceptedCommands", BindingFlags.Instance | BindingFlags.NonPublic);
                if (acceptedCommandsFieldInfo != null)
                {
                    int value = (int)acceptedCommandsFieldInfo.GetValue(this);
                    acceptedCommandsFieldInfo.SetValue(this, value | SERVICE_ACCEPT_PRESHUTDOWN);
                }
                // when 'preshutdown' is enabled, we need to disable 'shutdown' command so that shutdown event is not handled twice
                this.CanShutdown = false;
            }
            catch (Exception ex)
            {
                // If this fails, we'll just log an error and ignore.
                // Since we're setting internal field properties, Windows may change this at any time.
                this.EventLog.WriteEntry($"An error occurred trying to configure the Service to accept pre-shutdown notifications: {ex}", EventLogEntryType.Information);
                // Set this to 'true' so we can handle OS's SHUTDOWN command
                this.CanShutdown = true;
            }
        }

        /// <inheritdoc />
        protected override void OnStart(string[] args)
        {
            this.serviceManager.Start();
        }

        /// <inheritdoc />
        protected override void OnStop()
        {
            this.logger.LogInformation("Received a STOP command from the OS, service will stop.");
            lock (this.shutdownLock)
            {
                if (this.isShuttingDown) return;
                this.isShuttingDown = true;
            }

            this.serviceManager.Stop();
            base.OnStop();
        }

        /// <inheritdoc />
        protected override void OnShutdown()
        {
            this.logger.LogInformation("Received a SHUTDOWN command from the OS, service will stop.");
            lock (this.shutdownLock)
            {
                if (this.isShuttingDown) return;
                this.isShuttingDown = true;
            }

            this.serviceManager.Stop();
            base.OnShutdown();
        }

        /// <inheritdoc />
        protected override void OnCustomCommand(int command)
        {
            if (command == SERVICE_CONTROL_PRESHUTDOWN)
            {
                this.logger.LogInformation("Received a pre-shutdown notification from the OS, service will stop.");

                // call the class's default command handler with 'Stop' command, this will properly handle the shutdown of service
                var baseCallback = typeof(ServiceBase).GetMethod("ServiceCommandCallback", BindingFlags.Instance | BindingFlags.NonPublic);
                if (baseCallback == null)
                {
                    throw new InvalidOperationException($"Private method 'ServiceCommandCallback' not found on {nameof(ServiceBase)}");
                }

                baseCallback.Invoke(this, new object[] { SERVICE_CONTROL_STOP });
            }
            else
            {
                base.OnCustomCommand(command);
            }
        }
    }
}
