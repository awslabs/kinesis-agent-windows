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
namespace Amazon.KinesisTap.Hosting
{
    using System;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.KinesisTap.Core;
    using Microsoft.Extensions.Logging;
    using NLog.Extensions.Logging;

    /// <summary>
    /// A class that provides an abstraction layer for the Service Control logic in Kinesis Tap.
    /// Previously this logic lived inside the Windows Service code (<see cref="KinesisTapService"/>),
    /// which made it exceptionally difficult to test, which of course meant that there were no tests.
    /// By moving it to a separate class, we can make it consumable from a standard test project.
    /// </summary>
    public class KinesisTapServiceManager
    {
        public const string ServiceName = "AWSKinesisTap";
        public static readonly TimeSpan MaximumServiceOperationDuration = TimeSpan.FromSeconds(25);
        private readonly ITypeLoader typeLoader;
        private readonly IParameterStore parameterStore;
        private readonly ILoggerFactory loggerFactory;
        private readonly ILogger logger;
        private SessionManager sessionManager;
        private readonly INetworkStatusProvider networkStatusProvider;

        /// <summary>
        /// Initializes a new instance of the <see cref="KinesisTapServiceManager"/> class, using custom parameters.
        /// </summary>
        /// <param name="typeLoader">An implementation of the <see cref="ITypeLoader"/> interface.</param>
        /// <param name="parameterStore">An implementation of the <see cref="IParameterStore"/> interface.</param>
        /// <param name="logger">An implementation of the <see cref="ILogger"/> interface.</param>
        public KinesisTapServiceManager(ITypeLoader typeLoader, IParameterStore parameterStore, ILogger logger, INetworkStatusProvider networkStatusProvider)
        {
            this.typeLoader = typeLoader;
            this.parameterStore = parameterStore;
            this.logger = logger;
            this.networkStatusProvider = networkStatusProvider;
            this.loggerFactory = CreateSessionLoggerFactory(parameterStore);
            PluginContext.ServiceLogger = logger;
        }

        /// <summary>
        /// Path to the default config file (appsettings.json)
        /// </summary>
        public string DefaultConfigPath => this.parameterStore.GetParameter(HostingUtility.DefaultConfigurationPathKey);

        /// <summary>
        /// Gets a <see cref="ManualResetEventSlim"/> object that is set when the singleton SessionManager class has finished starting.
        /// </summary>
        public ManualResetEventSlim StartCompleted { get; private set; } = new ManualResetEventSlim();

        /// <summary>
        /// Gets a <see cref="ManualResetEventSlim"/> object that is set when the singleton SessionManager class has finished stopping.
        /// </summary>
        public ManualResetEventSlim StopCompleted { get; private set; } = new ManualResetEventSlim();

        /// <summary>
        /// Starts a singleton SessionManager instance and waits for up to the <see cref="MaximumServiceOperationDuration"/> interval before returning.
        /// </summary>
        public void Start()
        {
            try
            {
                this.sessionManager = new SessionManager(new DefaultSessionFactory(),
                    this.typeLoader, this.parameterStore, this.networkStatusProvider, this.loggerFactory);

                // Create a separate thread so that the service can start without the risk of timing out.
                // This will NOT make the service start faster (the previous authors comment was inaccurate),
                // but it will ensure that the Service Control Manager does not time out while starting the service.
                // This method previously did not capture any startup errors that were encountered in the
                // SessionManager.Start method, since the task wasn't being awaited (nor was Wait() being called).
                var task = Task.Run(this.sessionManager.StartAsync)
                    .ContinueWith(startTask =>
                    {
                        if (startTask.Exception != null)
                        {
                            // If one or more errors was encountered during startup, write an Error event
                            // containing the aggregated stack trace to the Application Event Log.
                            var sb = new StringBuilder();
                            sb.AppendFormat("One or more errors occurred during startup of the {0} Service", ServiceName).AppendLine();
                            foreach (var e in startTask.Exception.Flatten().InnerExceptions)
                            {
                                sb.AppendLine("---------------------------------------------")
                                    .AppendLine(e.ToString());
                            }

                            this.logger.LogError(sb.ToString());
                        }
                        else
                        {
                            // If it started successfully, write an informational log event.
                            this.logger.LogInformation($"{ServiceName} has started.");
                        }

                        this.StartCompleted.Set();
                    });

                // Wait for SessionManager.Start to complete. If it takes longer than the interval defined in the,
                // MaximumServiceOperationDuration variable, a warning message will be written to the Event Log.
                if (!task.Wait(MaximumServiceOperationDuration))
                    this.logger.LogWarning($"{ServiceName} took longer than {MaximumServiceOperationDuration} to start.");
            }
            catch (Exception ex)
            {
                this.logger.LogError(0, ex, $"Error starting {nameof(SessionManager)}");
                throw;
            }
        }

        /// <summary>
        /// Stops the singleton SessionManager instance and waits for up to the <see cref="MaximumServiceOperationDuration"/> interval before returning.
        /// </summary>
        public void Stop()
        {
            // Call the Stop method in SessionManager and wait for [MaximumServiceOperationDuration] seconds.
            // This will *hopefully* give the sources/sinks enough time to cleanly shut down (but that's out of our control).
            // We can't wait longer than 30 seconds, as that would cause the Service Control Manager
            // (the Windows component that manages the lifetime of Services) to throw an error relating
            // to the service taking too long to stop. The default for this is 30 seconds, so we'll use a little less than that.
            if (!Task.Run(this.sessionManager.StopAsync)
                .ContinueWith(t => this.sessionManager.Dispose())
                .Wait(MaximumServiceOperationDuration))
            {
                this.logger.LogWarning($"{ServiceName} could not shut down all components within the maximum service stop interval.");
            }
            else
            {
                this.logger.LogInformation($"{ServiceName} has shut down all components successfully.");
            }

            this.StopCompleted.Set();
        }

        private static ILoggerFactory CreateSessionLoggerFactory(IParameterStore parameterStore)
        {
            ILoggerFactory loggerFactory = new LoggerFactory();
            loggerFactory.AddNLog();
#if DEBUG
            loggerFactory.AddConsole(LogLevel.Debug);
#endif
            return loggerFactory;
        }
    }
}
