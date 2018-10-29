using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public class DelimitedLogContext : LogContext
    {
        public IDictionary<string, int> Mapping { get; set; }
        public string TimeStampField { get; set; }
    }
}
