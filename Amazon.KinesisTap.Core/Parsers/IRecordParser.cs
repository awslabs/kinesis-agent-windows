using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public interface IRecordParser<out TData, TContext> where TContext : LogContext
    {
        IEnumerable<IEnvelope<TData>> ParseRecords(StreamReader sr, TContext context);
    }
}
