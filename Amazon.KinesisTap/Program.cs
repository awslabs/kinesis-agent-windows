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
