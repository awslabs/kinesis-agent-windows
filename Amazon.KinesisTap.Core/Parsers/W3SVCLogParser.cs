using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public class W3SVCLogParser : DelimitedLogParserBase<W3SVCLogRecord>
    {
        protected const string FIELDS = "#Fields: ";

        public W3SVCLogParser() : base(" ", (data, context) => new W3SVCLogRecord(data, context))
        {
        }

        protected override bool IsComment(string line)
        {
            return line.StartsWith("#");
        }

        protected override bool IsHeader(string line)
        {
            return line.StartsWith(FIELDS);
        }

        protected override string[] GetFields(string fieldsLine)
        {
            return base.GetFields(fieldsLine.Substring(FIELDS.Length));
        }
    }
}
