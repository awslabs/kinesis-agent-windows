using log4net;
using log4net.Appender;
using log4net.Core;
using log4net.Layout;
using log4net.Repository.Hierarchy;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.DiagnosticTool
{
    public class Log4NetSimulator : LogSimulator
    {
        private ILog _log;

        public Log4NetSimulator(string[] args) : base(1000, 1000, 1)
        {
            ParseOptionValues(args);
            ParseLog4NetOptionValues(args);
            _log = LogManager.GetLogger(this.GetType());
        }

        protected void ParseLog4NetOptionValues(string[] args)
        {
            Hierarchy hierarchy = (Hierarchy)LogManager.GetRepository();
            RollingFileAppender appender = new RollingFileAppender();
            appender.File = args[1];
            var options = args.Where(s => s.StartsWith("-"));
            foreach (string option in options)
            {
                if (option.StartsWith("-l"))
                {
                    switch (option)
                    {
                        case "-le":
                            appender.LockingModel = new FileAppender.ExclusiveLock();
                            break;
                        case "-lm":
                            appender.LockingModel = new FileAppender.MinimalLock();
                            break;
                        case "-li":
                            appender.LockingModel = new FileAppender.InterProcessLock();
                            break;
                    }
                }
            }

            PatternLayout layout = new PatternLayout();
            layout.ConversionPattern = "%date [%thread] %-5level %logger - %message%newline";
            layout.ActivateOptions();
            appender.Layout = layout;

            appender.ActivateOptions();
            hierarchy.Root.AddAppender(appender);

            hierarchy.Root.Level = Level.Info;
            hierarchy.Configured = true;
        }

        protected override void WriteLog(string v)
        {
            _log.Info(v);
        }
    }
}
