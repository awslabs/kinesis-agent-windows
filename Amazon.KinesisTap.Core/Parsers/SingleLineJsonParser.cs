using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

using Newtonsoft.Json.Linq;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Each line is a Json object
    /// </summary>
    public class SingleLineJsonParser : IRecordParser<JObject, LogContext>
    {
        private readonly Func<JObject, DateTime> _getTimestamp;

        public SingleLineJsonParser(string timestampField, string timestampFormat)
        {
            if (!string.IsNullOrEmpty(timestampField) || !string.IsNullOrEmpty(timestampFormat))
            {
                //If one is provided, then timestampField is required
                Guard.ArgumentNotNullOrEmpty(timestampField, "TimestampField is required for SingleLineJsonParser");
                TimestampExtrator timestampExtractor = new TimestampExtrator(timestampField, timestampFormat);
                _getTimestamp = timestampExtractor.GetTimestamp;
            }
            else
            {
                _getTimestamp = jobject => DateTime.UtcNow;
            }
        }

        public IEnumerable<IEnvelope<JObject>> ParseRecords(StreamReader sr, LogContext context)
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
                    JObject jObject = JObject.Parse(record);
                    yield return new LogEnvelope<JObject>(jObject,
                        _getTimestamp(jObject),
                        record,
                        context.FilePath,
                        context.Position,
                        context.LineNumber);
                }
            }
        }
    }
}
