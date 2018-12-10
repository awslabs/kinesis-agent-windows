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
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

using Amazon.KinesisTap.Hosting;
using Amazon.KinesisTap.Windows;

namespace Amazon.KinesisTap
{
    public partial class KinesisTapService : ServiceBase
    {
        private LogManager _logManger;

        public KinesisTapService()
        {
        }

        protected override void OnStart(string[] args)
        {
            try
            {
                _logManger = new LogManager(new NetTypeLoader(), new RegistryParameterStore());
                //Create a separate thread so that the service can start faster
                //logManager.Start will catch all none fatal errors and log them
                //Fatal errors will be captured by CurrentDomain_UnhandledException in Program.cs 
                //and logged to Windows Event Log
                Task.Run(() => _logManger.Start());
            }
            catch(Exception ex)
            {
                LogError(ex);
                throw;
            }
        }

        protected override void OnStop()
        {
            _logManger?.Stop();
        }

        private void LogError(Exception ex)
        {
            string logSource = this.ServiceName;
            if (!EventLog.SourceExists(logSource))
            {
                EventLog.CreateEventSource(logSource, "Application");
            }
            EventLog.WriteEntry(logSource, ex.ToString(), EventLogEntryType.Error);
        }

        private void InitializeComponent()
        {
            // 
            // Amazon.KinesisTapService
            // 
            this.ServiceName = "AWSKinesisTap";
        }
    }
}
