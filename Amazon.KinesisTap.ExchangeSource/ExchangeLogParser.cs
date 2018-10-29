using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public class ExchangeLogParser : DelimitedLogParserBase<ExchangeLogRecord>
    {
        protected const string FIELDS = "#Fields: ";

        public ExchangeLogParser() : base(",", (data, context) => new ExchangeLogRecord(data, context))
        {
        }

        protected override bool IsComment(string line)
        {
            return line.StartsWith("#") || line.StartsWith("Date");
        }

        protected override bool IsHeader(string line)
        {
            return line.StartsWith(FIELDS);
        }

        protected override string[] GetFields(string fieldsLine)
        {
            return base.GetFields(fieldsLine.Substring(FIELDS.Length));
        }

        protected override void AnalyzeMapping(DelimitedLogContext context)
        {
            base.AnalyzeMapping(context);
            if (!string.IsNullOrWhiteSpace(this.TimeStampField))
            {
                context.TimeStampField = TimeStampField;
            }
            else if (context.Mapping.ContainsKey("date-time"))
            {
                context.TimeStampField = "date-time";
            }
            else if (context.Mapping.ContainsKey("DateTime"))
            {
                context.TimeStampField = "DateTime";
            }
            else
            {
                throw new Exception("Exchange log parser cannot determine date-time field");
            }
        }
    }
}
