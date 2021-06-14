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
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Filesystem
{
    /// <summary>
    /// Parses records that match with a regular expression.
    /// </summary>
    public class RegexLogParser : ILogParser<IDictionary<string, string>, RegexLogContext>
    {
        private readonly Regex _patternRegex;
        private readonly Regex _extractionRegex;
        private readonly string _timestampFormat;
        private readonly ILogger _logger;
        private readonly DateTimeKind _timeZoneKind;
        private readonly bool _removeUnmatchedRecord;
        private readonly Encoding _encoding;
        private readonly int _bufferSize;

        public RegexLogParser(ILogger logger,
            string pattern,
            RegexParserOptions options,
            Encoding encoding, int bufferSize)
        {
            _patternRegex = new Regex(pattern, RegexOptions.Compiled);
            if (!string.IsNullOrEmpty(options.ExtractionPattern))
            {
                var regexOptions = ParseRegexOptions(options.ExtractionRegexOptions);
                _extractionRegex = new Regex(options.ExtractionPattern, regexOptions | RegexOptions.Compiled);
            }
            _timestampFormat = options.TimestampFormat;
            _logger = logger;
            _timeZoneKind = options.TimeZoneKind;
            _removeUnmatchedRecord = options.RemoveUnmatchedRecord;
            _encoding = encoding;
            _bufferSize = bufferSize;
        }

        public async Task ParseRecordsAsync(RegexLogContext context, IList<IEnvelope<IDictionary<string, string>>> output,
            int recordCount, CancellationToken stopToken = default)
        {
            var count = 0;
            using (var stream = new FileStream(context.FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                stream.Position = context.Position;
                using (var reader = new LineReader(stream, _encoding, _bufferSize))
                {
                    while (count < recordCount)
                    {
                        stopToken.ThrowIfCancellationRequested();
                        var (line, consumed) = await reader.ReadAsync(stopToken);
                        _logger.LogTrace("File: '{0}', line: '{1}', bytes: {2}", context.FilePath, line, consumed);

                        if (line is null)
                        {
                            // end-of-file
                            var record = CreateRecord(context);
                            if (record is not null)
                            {
                                output.Add(record);
                            }

                            context.RecordBuilder.Clear();
                            context.MatchedLineNumber = -1;
                            context.MatchedLineTimestamp = null;
                            break;
                        }

                        context.LineNumber++;
                        context.Position += consumed;

                        var match = _patternRegex.Match(line);
                        if (match.Success)
                        {
                            _logger.LogDebug("Regex matched.");
                            // this is start of a new record, get the last record
                            var record = CreateRecord(context);
                            if (record is not null)
                            {
                                output.Add(record);
                                count++;
                            }
                            // remember the new record's first line
                            context.RecordBuilder.Clear();
                            context.RecordBuilder.Append(line);
                            context.MatchedLineNumber = context.LineNumber;
                            // if the matched line has a 'Timestamp' match group, use it as the record's timestamp
                            context.MatchedLineTimestamp = GetTimeStamp(match);
                        }
                        else
                        {
                            _logger.LogDebug("Regex NOT matched.");
                            // line is not the beginning of a new record
                            if (context.RecordBuilder.Length != 0)
                            {
                                // some lines have been added to the new record
                                context.RecordBuilder.AppendLine();
                                context.RecordBuilder.Append(line);
                            }
                            else if (_removeUnmatchedRecord)
                            {
                                _logger.LogWarning("Line discarded: {0}", line);
                            }
                            else
                            {
                                // start a new record
                                // TODO figure out what's the use case scenario for this
                                context.RecordBuilder.Append(line);
                                context.MatchedLineNumber = context.LineNumber;
                            }
                        }
                    }
                }
            }
        }

        private LogEnvelope<IDictionary<string, string>> CreateRecord(RegexLogContext context)
        {
            var rawRecord = context.RecordBuilder.Length == 0 ? null : context.RecordBuilder.ToString();
            if (rawRecord is null)
            {
                return null;
            }

            var fields = _extractionRegex is null
                    ? new Dictionary<string, string>()
                    : Utility.ExtractFields(_extractionRegex, rawRecord);

            var timestamp = GetTimestamp(fields, context.MatchedLineTimestamp);
            return new LogEnvelope<IDictionary<string, string>>(fields,
                timestamp,
                rawRecord,
                context.FilePath,
                context.Position,
                context.MatchedLineNumber);
        }

        private DateTime? GetTimeStamp(Match match)
        {
            var style = _timeZoneKind == DateTimeKind.Utc
                ? DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal
                : DateTimeStyles.AssumeLocal;
            for (var i = 1; i < match.Groups.Count; i++)
            {
                var groupName = _patternRegex.GroupNameFromNumber(i);
                if ("Timestamp".Equals(groupName, StringComparison.OrdinalIgnoreCase))
                {
                    var value = match.Groups[i].Value;
                    if (DateTime.TryParseExact(value, _timestampFormat, CultureInfo.InvariantCulture, style, out var timestamp))
                    {
                        return timestamp;
                    }
                }
            }
            return null;
        }

        private DateTime GetTimestamp(IDictionary<string, string> recordData, DateTime? defaultTimestamp)
        {
            if (recordData is null)
            {
                goto returnCurrentTimestamp;
            }
            var style = _timeZoneKind == DateTimeKind.Utc
                ? DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal
                : DateTimeStyles.AssumeLocal;
            foreach (var kvp in recordData)
            {
                if ("Timestamp".Equals(kvp.Key, StringComparison.OrdinalIgnoreCase))
                {
                    if (_timestampFormat is null && DateTime.TryParse(kvp.Value, CultureInfo.InvariantCulture, style, out var timestamp))
                    {
                        return timestamp;
                    }
                    if (_timestampFormat is not null && DateTime.TryParseExact(kvp.Value, _timestampFormat, CultureInfo.InvariantCulture, style, out timestamp))
                    {
                        return timestamp;
                    }
                    _logger.LogError($"Unable to parse timestamp '{kvp.Value}' with format '{_timestampFormat}'");
                }
            }

returnCurrentTimestamp:
            if (defaultTimestamp.HasValue)
            {
                return defaultTimestamp.Value;
            }
            return _timeZoneKind == DateTimeKind.Utc ? DateTime.UtcNow : DateTime.Now;
        }

        private static RegexOptions ParseRegexOptions(string extractionRegexOptions)
        {
            var regexOptions = RegexOptions.None;
            if (!string.IsNullOrWhiteSpace(extractionRegexOptions))
            {
                foreach (var option in extractionRegexOptions.Split('+', StringSplitOptions.RemoveEmptyEntries))
                {
                    regexOptions |= Utility.ParseEnum<RegexOptions>(option.Trim());
                }
            }
            return regexOptions;
        }
    }
}
