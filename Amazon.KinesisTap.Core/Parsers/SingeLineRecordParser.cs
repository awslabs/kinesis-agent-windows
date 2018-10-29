using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Eg: each line is a single record
    /// </summary>
    public class SingeLineRecordParser : IRecordParser<string, LogContext>
    {
        public IEnumerable<IEnvelope<string>> ParseRecords(StreamReader sr, LogContext context)
        {
            if (context.Position > sr.BaseStream.Position)
            {
                sr.BaseStream.Position = context.Position;
            }

            while (!sr.EndOfStream)
            {
                string record = sr.ReadLine();
                context.LineNumber++;
                if (!string.IsNullOrWhiteSpace(record))
                {
                    yield return new LogEnvelope<string>(record, 
                        DateTime.UtcNow, 
                        record, 
                        context.FilePath, 
                        context.Position,
                        context.LineNumber);
                }
            }
        }
    }
}
