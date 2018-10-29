using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public class DelimitedLogRecord : DelimitedLogRecordBase
    {
        private readonly Func<DelimitedLogRecordBase, DateTime> _getDateTime;

        public DelimitedLogRecord(string[] data, DelimitedLogContext context, Func<DelimitedLogRecordBase, DateTime> getDateTime) : base(data, context)
        {
            _getDateTime = getDateTime;
        }

        public override DateTime TimeStamp => _getDateTime(this);
    }
}
