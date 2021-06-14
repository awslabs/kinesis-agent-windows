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
using System.Diagnostics;
using System.Diagnostics.Eventing.Reader;
using System.Linq;
using System.Runtime.Versioning;

namespace Amazon.KinesisTap.Windows
{
    /// <summary>
    /// <see cref="ServiceDependency"/> representing the Windows Event Log service
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal class EventLogDependency : ServiceDependency
    {
        private readonly string _logName;

        /// <summary>
        /// Initialize an <see cref="EventLogDependency"/> object from the name of the event log.
        /// </summary>
        /// <param name="logName">Name of the event log.</param>
        public EventLogDependency(string logName) : base("EventLog")
        {
            _logName = logName;
        }

        /// <inheritdoc/>
        public override bool IsDependencyAvailable() => base.IsDependencyAvailable() && EventLogExists(_logName);

        /// <inheritdoc/>
        public override string Name => $"Windows Event log '{_logName}'";

        /// <summary>
        /// EventLog.Exists only supports 'classic' logs, so we need workaround
        /// </summary>
        private static bool EventLogExists(string logName)
        {
            if (EventLog.Exists(logName))
            {
                // fast path
                return true;
            }

            return EventLogSession.GlobalSession.GetLogNames().Any(n => n.Equals(logName, StringComparison.Ordinal));
        }
    }
}
