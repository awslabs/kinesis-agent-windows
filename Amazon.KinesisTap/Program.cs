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
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

using Amazon.KinesisTap.Windows;

namespace Amazon.KinesisTap
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        static void Main()
        {
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);

            WindowsStartup.Start();
#if DEBUG
            Application.Run(new frmMain());
#else
            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[]
            {
                new KinesisTapService()
            };
            ServiceBase.Run(ServicesToRun);
#endif
        }

        static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            var eventLogName = "KinesisTap";

            if (!EventLog.SourceExists(eventLogName))
            {
                EventLog.CreateEventSource(eventLogName, "Application");
            }
            string entry = $"Unhandled exception {e.ExceptionObject}";
            EventLog.WriteEntry(eventLogName,
                entry,
                EventLogEntryType.Error);
        }
    }
}
