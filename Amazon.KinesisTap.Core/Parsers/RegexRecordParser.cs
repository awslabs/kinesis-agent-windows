using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace Amazon.KinesisTap.Core
{
    public class RegexRecordParser : IRecordParser<IDictionary<string, string>, LogContext>
    {
        protected Regex _regex;
        protected Regex _extractionRegex;
        protected string _timeStampFormat;
        protected string _peekLine;
        protected DateTime? _peekTimeStamp;
        protected long _id;
        protected ILogger _logger;
        protected DateTimeKind _timeZoneKind;
        protected RegexRecordParserOptions _parserOptions;

        public RegexRecordParser(string pattern,
          string timeStampFormat,
          ILogger logger,
          string extractionPattern,
          DateTimeKind timeZoneKind) : 
            this(pattern,
                timeStampFormat,
                logger, 
                extractionPattern,
                timeZoneKind,
                new RegexRecordParserOptions())
        {

        }

        public RegexRecordParser(string pattern, 
            string timeStampFormat, 
            ILogger logger, 
            string extractionPattern,
            DateTimeKind timeZoneKind,
            RegexRecordParserOptions parserOptions)
        {
            _regex = new Regex(pattern);
            if (!string.IsNullOrEmpty(extractionPattern))
            {
                _extractionRegex = new Regex(extractionPattern);
            }
            _timeStampFormat = timeStampFormat;
            _logger = logger;
            _timeZoneKind = timeZoneKind;
            _parserOptions = parserOptions;
        }

        public IEnumerable<IEnvelope<IDictionary<string, string>>> ParseRecords(StreamReader sr, LogContext context)
        {
            if (context.Position > sr.BaseStream.Position)
            {
                sr.BaseStream.Position = context.Position;
            }

            while (!sr.EndOfStream || this.BufferNotEmpty)
            {
                var envelope = this.ParseNextRecord(sr, context);
                if (envelope != null)
                {
                    yield return envelope;
                }
            }
        }

        protected virtual IEnvelope<IDictionary<string, string>> ParseNextRecord(StreamReader sr, LogContext context)
        {
            DateTime? timestamp = null;
            StringBuilder sb = new StringBuilder();
            bool startedRecord = false;
            long lineNumber = context.LineNumber; //for multiline record, capture the previous line number
            if (!string.IsNullOrEmpty(_peekLine))
            {
                sb.Append(_peekLine);
                timestamp = _peekTimeStamp;
                _peekLine = null;
                startedRecord = true;
            }
            else
            {
                lineNumber++; //If peeked, the line number is already advanced. Otherwise, need to advance here
            }
            while (!sr.EndOfStream)
            {
                string line = sr.ReadLine();
                context.LineNumber++;
                Match match = _regex.Match(line);
                if (match.Success)
                {
                    DateTime? timestampTemp = GetTimeStamp(match);
                    if (startedRecord)
                    {
                        //Save the line
                        _peekLine = line;
                        _peekTimeStamp = timestampTemp;
                        break;
                    }
                    else
                    {
                        timestamp = timestampTemp;
                        sb.Append(line);
                        startedRecord = true;
                    }
                }
                else
                {
                    if (startedRecord)
                    {
                        sb.AppendLine();
                        sb.Append(line);
                    }
                    else
                    {
                        if (_parserOptions.RemoveUnmatchedRecord)
                        {
                            _logger.LogWarning($"Line discarded: {line}");
                        }
                        else
                        {
                            sb.Append(line);
                            startedRecord = true;
                        }
                    }
                }
            }
            string rawRecord = sb.ToString();
            if (string.IsNullOrWhiteSpace(rawRecord))
            {
                return null;
            }

            IDictionary<string, string> fields = null;
            if (_extractionRegex != null)
            {
                fields = Utility.ExtractFields(_extractionRegex, rawRecord);
            }

            return new LogEnvelope<IDictionary<string, string>>(
                fields,
                ToUniversalTime(timestamp),
                rawRecord,
                context.FilePath,
                context.Position,
                lineNumber);
        }

        protected bool BufferNotEmpty
        {
            get { return _peekLine != null; }
        }

        private DateTime? GetTimeStamp(Match match)
        {
            for(int i = 1; i < match.Groups.Count; i++)
            {
                string groupName = _regex.GroupNameFromNumber(i);
                if ("TimeStamp".Equals(groupName, StringComparison.CurrentCultureIgnoreCase))
                {
                    string value = match.Groups[i].Value;
                    if (DateTime.TryParseExact(match.Groups[i].Value, _timeStampFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime timeStamp))
                    {
                        return timeStamp;
                    }
                    else
                    {
                        _logger?.LogError($"Unable to parse string {value} with DateTime format {_timeStampFormat}");
                    }
                }
            }
            return null;
        }

        protected DateTime ToUniversalTime(DateTime? dateTime)
        {
            if (!dateTime.HasValue)
                return DateTime.UtcNow;

            if (_timeZoneKind == DateTimeKind.Local)
            {
                return dateTime.Value.ToUniversalTime();
            }
            else
            {
                return dateTime.Value;
            }
        }
    }
}
