/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
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
        protected readonly Regex _regex;
        protected readonly Regex _extractionRegex;
        protected readonly string _timeStampFormat;
        protected readonly string _alternateTimeStampFormat;
        protected string _peekLine;
        protected DateTime? _peekTimeStamp;
        protected readonly long _id;
        protected readonly ILogger _logger;
        protected readonly DateTimeKind _timeZoneKind;
        protected readonly RegexRecordParserOptions _parserOptions;

        public RegexRecordParser(string pattern,
          string timeStampFormat,
          ILogger logger,
          string extractionPattern,
          string extractionRegexOptions,
          DateTimeKind timeZoneKind) : 
            this(pattern,
                timeStampFormat,
                logger, 
                extractionPattern,
                extractionRegexOptions,
                timeZoneKind,
                new RegexRecordParserOptions(),
                null)
        {

        }

        public RegexRecordParser(string pattern,
            string timeStampFormat,
            ILogger logger,
            string extractionPattern,
            string extractionRegexOptions,
            DateTimeKind timeZoneKind,
            RegexRecordParserOptions parserOptions) :
            this(pattern,
                timeStampFormat,
                logger,
                extractionPattern,
                extractionRegexOptions,
                timeZoneKind,
                parserOptions,
                null)
        {

        }

        public RegexRecordParser(string pattern, 
            string timeStampFormat, 
            ILogger logger, 
            string extractionPattern,
            string extractionRegexOptions,
            DateTimeKind timeZoneKind,
            RegexRecordParserOptions parserOptions, 
            string alternateTimestampFormat
            )
        {
            _regex = new Regex(pattern);
            if (!string.IsNullOrEmpty(extractionPattern))
            {
                RegexOptions regexOptions = ParseRegexOptions(extractionRegexOptions);
                _extractionRegex = new Regex(extractionPattern, regexOptions);
            }
            _timeStampFormat = timeStampFormat;
            _logger = logger;
            _timeZoneKind = timeZoneKind;
            _parserOptions = parserOptions;
            _alternateTimeStampFormat = alternateTimestampFormat;
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
                _logger?.LogDebug($"Read line: {line}");
                context.LineNumber++;
                Match match = _regex.Match(line);
                if (match.Success)
                {
                    _logger?.LogDebug("Matched regex.");
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
                    _logger?.LogDebug("Does not Match regex.");
                    if (startedRecord)
                    {
                        _logger?.LogDebug("Added to existing record.");
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
                            _logger?.LogDebug("Starting new record.");
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

        protected RegexOptions ParseRegexOptions(string extractionRegexOptions)
        {
            RegexOptions regexOptions = RegexOptions.None;
            if (!string.IsNullOrWhiteSpace(extractionRegexOptions))
            {
                foreach (var option in extractionRegexOptions.Split(new char[] { '+' }, StringSplitOptions.RemoveEmptyEntries))
                {
                    regexOptions = regexOptions | Utility.ParseEnum<RegexOptions>(option.Trim());
                }
            }
            return regexOptions;
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
                    else if (_alternateTimeStampFormat != null 
                        && DateTime.TryParseExact(match.Groups[i].Value, _alternateTimeStampFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime alternateTimeStamp))
                    {
                        return alternateTimeStamp;
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
