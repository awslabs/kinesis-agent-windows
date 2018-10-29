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
