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
    using System.Reflection;
    using System.ServiceProcess;

    public partial class KinesisTapService : ServiceBase
    {
        const int SERVICE_ACCEPT_PRESHUTDOWN = 0x100;
        const int SERVICE_CONTROL_PRESHUTDOWN = 0xf;

        private readonly KinesisTapServiceManager serviceManager;
        private readonly object shutdownLock = new object();
        private bool isShuttingDown;

        public KinesisTapService()
        {
            this.serviceManager = new KinesisTapServiceManager();

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
            }
            catch (Exception ex)
            {
                // If this fails, we'll just log an error and ignore.
                // Since we're setting internal field properties, Windows may change this at any time.
                this.EventLog.WriteEntry($"An error occurred trying to configure the Service to accept pre-shutdown notifications: {ex}", EventLogEntryType.Information);
            }

            // Enable the ability to subscribe to shutdown events.
            // This instructs the Service Control Manager to call the "OnShutdown" method when the OS is shutting down.
            // When this is called, it only has 5 seconds to stop.
            // This is enabled as a backup in case the pre-shutdown notification is missed.
            this.CanShutdown = true;
        }

        /// <inheritdoc />
        protected override void OnStart(string[] args)
        {
            this.serviceManager.Start();
        }

        /// <inheritdoc />
        protected override void OnStop()
        {
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
            lock (this.shutdownLock)
            {
                if (this.isShuttingDown) return;
                this.isShuttingDown = true;
            }

            this.EventLog.WriteEntry("Received a shutdown command from the OS, service will stop.", EventLogEntryType.Information);
            this.serviceManager.Stop();
        }

        /// <inheritdoc />
        protected override void OnCustomCommand(int command)
        {
            if (command == SERVICE_CONTROL_PRESHUTDOWN)
            {
                lock (this.shutdownLock)
                {
                    if (this.isShuttingDown) return;
                    this.isShuttingDown = true;
                }

                this.EventLog.WriteEntry("Received a pre-shutdown notification from the OS, service will stop.", EventLogEntryType.Information);
                this.serviceManager.Stop();
            }
            else
            {
                base.OnCustomCommand(command);
            }
        }
    }
}
