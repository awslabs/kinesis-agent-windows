using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public class LogContext
    {
        public string FilePath { get; set; }

        public long Position { get; set; }

        public long LineNumber { get; set; }

        public int ConsecutiveIOExceptionCount { get; set; }
    }
}
