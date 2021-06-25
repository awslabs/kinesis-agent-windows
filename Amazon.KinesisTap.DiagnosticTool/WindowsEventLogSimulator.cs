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
using Amazon.KinesisTap.DiagnosticTool.Core;
using System;
using System.Diagnostics;
using System.Runtime.Versioning;

namespace Amazon.KinesisTap.DiagnosticTool
{
    /// <summary>
    /// Simulator for Windows Event log
    /// </summary>
    [SupportedOSPlatform("windows")]
    public class WindowsEventLogSimulator : LogSimulator, IDisposable
    {
        const string EVENT_SOURCE = "KTDiag.exe";
        private readonly EventLog _log;

        public WindowsEventLogSimulator(string[] args) : base(1000, 1000, 1)
        {
            ParseOptionValues(args);

            var logName = args[1];
            var source = logName + "_" + EVENT_SOURCE;

            if (!EventLog.SourceExists(source))
            {
                EventLog.CreateEventSource(source, logName);
            }
            _log = new EventLog(logName, ".", source);
        }

        /// <summary>
        /// Generate the logs
        /// </summary>
        /// <param name="v"></param>
        protected override void WriteLog(string v)
        {
            var eventId = (int)(DateTime.Now.Ticks % ushort.MaxValue);
            _log.WriteEntry(v, EventLogEntryType.Information, eventId);
        }

        #region IDisposable Support
        private bool _disposedValue = false; // To detect redundant calls

        protected override void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                base.Dispose(disposing);
                if (disposing)
                {
                    _log.Dispose();
                }
                _disposedValue = true;
            }
        }
        #endregion
    }
}
