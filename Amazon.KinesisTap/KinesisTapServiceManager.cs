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
    using System;
    using System.Diagnostics;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.KinesisTap.Core;
    using Amazon.KinesisTap.Hosting;
    using Amazon.KinesisTap.Windows;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// A class that provides an abstraction layer for the Service Control logic in Kinesis Tap.
    /// Previously this logic lived inside the Windows Service code (<see cref="KinesisTapService"/>),
    /// which made it exceptionally difficult to test, which of course meant that there were no tests.
    /// By moving it to a separate class, we can make it consumable from a standard test project.
    /// </summary>
    public class KinesisTapServiceManager
    {
        public const string ServiceName = "KinesisTap";
        public static readonly TimeSpan MaximumServiceOperationDuration = TimeSpan.FromSeconds(25);
        private readonly ITypeLoader typeLoader;
        private readonly IParameterStore parameterStore;
        private readonly ILogger logger;
        private LogManager logManager;

        /// <summary>
        /// Initializes a new instance of the <see cref="KinesisTapServiceManager"/> class.
        /// </summary>
        public KinesisTapServiceManager()
        {
            // Create the LogSource if it doesn't already exist.
            if (!EventLog.SourceExists(ServiceName))
                EventLog.CreateEventSource(ServiceName, "Application");
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KinesisTapServiceManager"/> class, using custom parameters.
        /// </summary>
        /// <param name="typeLoader">An implementation of the <see cref="ITypeLoader"/> interface.</param>
        /// <param name="parameterStore">An implementation of the <see cref="IParameterStore"/> interface.</param>
        /// <param name="logger">An implementation of the <see cref="ILogger"/> interface.</param>
        public KinesisTapServiceManager(ITypeLoader typeLoader, IParameterStore parameterStore, ILogger logger)
            : this()
        {
            this.typeLoader = typeLoader;
            this.parameterStore = parameterStore;
            this.logger = logger;
        }

        /// <summary>
        /// Gets a <see cref="ManualResetEventSlim"/> object that is set when the singleton LogManager class has finished starting.
        /// </summary>
        public ManualResetEventSlim StartCompleted { get; private set; } = new ManualResetEventSlim();

        /// <summary>
        /// Gets a <see cref="ManualResetEventSlim"/> object that is set when the singleton LogManager class has finished stopping.
        /// </summary>
        public ManualResetEventSlim StopCompleted { get; private set; } = new ManualResetEventSlim();

        /// <summary>
        /// Starts a singleton LogManager instance and waits for up to the <see cref="MaximumServiceOperationDuration"/> interval before returning.
        /// </summary>
        public void Start()
        {
            try
            {
                this.logManager = new LogManager(this.typeLoader ?? new NetTypeLoader(), this.parameterStore ?? new RegistryParameterStore());

                // Create a separate thread so that the service can start without the risk of timing out.
                // This will NOT make the service start faster (the previous authors comment was inaccurate),
                // but it will ensure that the Service Control Manager does not time out while starting the service.
                // This method previously did not capture any startup errors that were encountered in the
                // LogManager.Start method, since the task wasn't being awaited (nor was Wait() being called).
                var task = new TaskFactory()
                    .StartNew(this.logManager.Start, TaskCreationOptions.LongRunning)
                    .ContinueWith(_ =>
                    {
                        if (_.Exception != null)
                        {
                            // If one or more errors was encountered during startup, write an Error event
                            // containing the aggregated stack trace to the Application Event Log.
                            var sb = new StringBuilder();
                            sb.AppendFormat("One or more errors occurred during startup of the {0} Service", ServiceName).AppendLine();
                            foreach (var e in _.Exception.Flatten().InnerExceptions)
                            {
                                sb.AppendLine("---------------------------------------------")
                                    .AppendLine(e.ToString());
                            }

                            this.LogMessage(sb.ToString(), EventLogEntryType.Error);
                        }
                        else
                        {
                            // If it started successfully, write an informational log event.
                            this.LogMessage($"{ServiceName} has started.", EventLogEntryType.Information);
                        }

                        this.StartCompleted.Set();
                    });

                // Wait for LogManager.Start to complete. If it takes longer than the interval defined in the,
                // MaximumServiceOperationDuration variable, a warning message will be written to the Event Log.
                if (!task.Wait(MaximumServiceOperationDuration))
                    this.LogMessage($"{ServiceName} took longer than {MaximumServiceOperationDuration} to start.", EventLogEntryType.Warning);
            }
            catch (Exception ex)
            {
                this.LogMessage(ex.ToString(), EventLogEntryType.Error);
                throw;
            }
        }

        /// <summary>
        /// Stops the singleton LogManager instance and waits for up to the <see cref="MaximumServiceOperationDuration"/> interval before returning.
        /// </summary>
        public void Stop()
        {
            // Call the Stop method in LogManager and wait for [MaximumServiceOperationDuration] seconds.
            // This will *hopefully* give the sources/sinks enough time to cleanly shut down (but that's out of our control).
            // We can't wait longer than 30 seconds, as that would cause the Service Control Manager
            // (the Windows component that manages the lifetime of Services) to throw an error relating
            // to the service taking too long to stop. The default for this is 30 seconds, so we'll use a little less than that.
            if (!Task.Run(() => this.logManager.Stop(true)).Wait(MaximumServiceOperationDuration))
                this.LogMessage($"{ServiceName} could not shut down all components within the maximum service stop interval.", EventLogEntryType.Warning);
            else
                this.LogMessage($"{ServiceName} has shut down all components successfully.", EventLogEntryType.Information);

            this.StopCompleted.Set();
        }

        private void LogMessage(string message, EventLogEntryType type)
        {
            EventLog.WriteEntry(ServiceName, message, type);
            if (this.logger != null)
            {
                switch (type)
                {
                    case EventLogEntryType.Error:
                        this.logger.LogError(message);
                        break;
                    case EventLogEntryType.Warning:
                        this.logger.LogWarning(message);
                        break;
                    case EventLogEntryType.Information:
                        this.logger.LogInformation(message);
                        break;
                    default:
                        break;
                }
            }
        }
    }
}
