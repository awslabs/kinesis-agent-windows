using System;
using System.Diagnostics.Eventing.Reader;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.DiagnosticTool
{
    class EventLogValidator
    {

        public bool ValidateLogName(string logName, IList<String> messages)
        {
            EventLogQuery eventLogQuery = new EventLogQuery(logName, PathType.LogName);
            EventLogWatcher watcher = new EventLogWatcher(eventLogQuery, null, true);

            try
            {
                watcher.Enabled = true;
                return true;
            }
            catch (EventLogNotFoundException ex)
            {
                messages.Add(ex.Message);
                messages.Add($"Event Log Name: {logName} is not a valid log name!");
                return false;
            }
            catch (Exception ex)
            {
                messages.Add(ex.ToString());
                messages.Add($"Unexpected exceptions. Event Log Name: {logName}.");
                return false;
            }
            finally {
                watcher.Enabled = false;
                watcher.Dispose();
            }
        }
    }
}
