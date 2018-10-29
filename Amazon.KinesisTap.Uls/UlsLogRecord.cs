using Amazon.KinesisTap.Core;
using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Uls
{
    /// <summary>
    /// UlsLogRecord.
    /// </summary>
    public class UlsLogRecord : DelimitedLogRecordBase
    {
        public UlsLogRecord(string[] data, DelimitedLogContext context) : base(data, context)
        {
        }

        public override DateTime TimeStamp
        { 
            get
            {
                return DateTime.Parse(this["Timestamp"], null, System.Globalization.DateTimeStyles.RoundtripKind);
            }
        }
    }
}
