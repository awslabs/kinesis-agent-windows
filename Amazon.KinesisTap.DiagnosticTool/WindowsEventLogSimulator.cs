using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.DiagnosticTool
{
    public class WindowsEventLogSimulator : LogSimulator, IDisposable
    {
        const string EVENT_SOURCE = "KTDiag.exe";
        private EventLog _log;

        public WindowsEventLogSimulator(string[] args) : base(1000, 1000, 1)
        {
            ParseOptionValues(args);

            string logName = args[1];
            string source = logName + "_" + EVENT_SOURCE;

            if (!EventLog.SourceExists(source))
            {
                EventLog.CreateEventSource(source, logName);
            }
            _log = new EventLog(logName, ".", source);
        }

        protected override void WriteLog(string v)
        {
            int eventId = (int)(DateTime.Now.Ticks % ushort.MaxValue);
            _log.WriteEntry(v, EventLogEntryType.Information, eventId);
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected override void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                base.Dispose(disposing);
                if (disposing)
                {
                    _log.Dispose();
                }
                disposedValue = true;
            }
        }
        #endregion
    }
}
