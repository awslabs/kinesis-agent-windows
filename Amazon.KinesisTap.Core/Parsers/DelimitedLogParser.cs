using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace Amazon.KinesisTap.Core
{
    public class DelimitedLogParser : DelimitedLogParserBase<DelimitedLogRecord>
    {
        protected Regex _headerRegex;
        protected Regex _recordRegex;
        protected Regex _commentRegex;
        protected string _headers;

        public DelimitedLogParser(
            string delimiter,
            Func<string[], DelimitedLogContext, DelimitedLogRecord> recordFactoryMethod,
            string headerPattern,
            string recordPattern,
            string commentPattern,
            string headers,
            DateTimeKind timeZoneKind
        ) : base (delimiter, recordFactoryMethod, timeZoneKind)
        {
            if (!string.IsNullOrWhiteSpace(headerPattern)) _headerRegex = new Regex(headerPattern);
            if (!string.IsNullOrWhiteSpace(recordPattern)) _recordRegex = new Regex(recordPattern);
            if (!string.IsNullOrWhiteSpace(commentPattern)) _commentRegex = new Regex(commentPattern);
            if (!string.IsNullOrWhiteSpace(headers))  _headers = headers.Trim();
        }

        public override IEnumerable<IEnvelope<DelimitedLogRecord>> ParseRecords(StreamReader sr, DelimitedLogContext context)
        {
            if (_headers != null && context.Mapping == null)
            {
                context.Mapping = GetFieldIndexMap(_headers);
            }
            return base.ParseRecords(sr, context);
        }

        protected override bool IsComment(string line)
        {
            if (_commentRegex != null)
            {
                return _commentRegex.IsMatch(line);
            }
            else if (_recordRegex != null)
            {
                return !_recordRegex.IsMatch(line); //No need to check header here because the caller always calls IsHeader first
            }
            return false;
        }

        protected override bool IsHeader(string line)
        {
            if (_headerRegex != null)
            {
                return _headerRegex.IsMatch(line);
            }
            return false;
        }
    }
}
