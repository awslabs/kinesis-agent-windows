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
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Filesystem
{
    /// <summary>
    /// Parse any log where records are separated by a delimiter.
    /// </summary>
    public class GenericDelimitedLogParser : AsyncDelimitedLogParserBase<KeyValueLogRecord>
    {
        private const string HeadersExtractionGroupName = "Headers";

        protected readonly TimestampExtrator _timestampExtractor;
        private readonly Regex _commentRegex;
        private readonly Regex _recordRegex;
        private readonly Regex _headerRegex;
        private readonly string _headers;
        private readonly bool _csvMode;
        private readonly StringBuilder _stringBuilder = new();

        public GenericDelimitedLogParser(ILogger logger, string delimiter, GenericDelimitedLogParserOptions options)
            : base(logger, delimiter, options)
        {
            _headers = options.Headers;
            _csvMode = options.CSVEscapeMode;
            if (options.TimestampField is not null)
            {
                _timestampExtractor = new TimestampExtrator(options.TimestampField, options.TimestampFormat);
            }

            if (options.CommentPattern is not null)
            {
                _commentRegex = new Regex(options.CommentPattern, RegexOptions.Compiled);
            }

            if (options.RecordPattern is not null)
            {
                _recordRegex = new Regex(options.RecordPattern, RegexOptions.Compiled);
            }

            if (options.HeadersPattern is not null)
            {
                _headerRegex = new Regex(options.HeadersPattern, RegexOptions.Compiled | RegexOptions.ExplicitCapture);
            }
            else if (options.Headers is null)
            {
                throw new ArgumentException("Either 'Headers' or 'HeadersPattern' must be provided");
            }
        }

        protected override KeyValueLogRecord CreateRecord(DelimitedTextLogContext context, Dictionary<string, string> data)
        {
            var timestamp = _timestampExtractor is null
                ? DateTime.Now
                : _timestampExtractor.GetTimestamp(data);

            return new KeyValueLogRecord(timestamp, data);
        }

        protected override bool IsComment(string line)
        {
            if (_commentRegex is not null)
            {
                return _commentRegex.IsMatch(line);
            }

            if (_recordRegex is not null)
            {
                return !_recordRegex.IsMatch(line);
            }
            return false;
        }

        protected override bool IsHeaders(string line, long lineNumber)
        {
            if (_delimiter == "," && _csvMode)
            {
                return lineNumber == 1;
            }

            return _headerRegex is not null && _headerRegex.IsMatch(line);
        }

        protected override Task<string[]> TryGetHeaderFields(DelimitedTextLogContext context, CancellationToken stopToken)
        {
            if (_headers is not null)
            {
                return Task.FromResult(SplitHeaders(_headers));
            }
            return base.TryGetHeaderFields(context, stopToken);
        }

        protected override string[] ParseDataFragments(string line)
        {
            if (_delimiter == "," && _csvMode)
            {
                _stringBuilder.Clear();
                return Utility.ParseCSVLine(line, _stringBuilder).ToArray();
            }
            return base.ParseDataFragments(line);
        }

        protected override string[] ParseHeadersLine(string headerLine)
        {
            // overwrite headers from the config if available
            if (_headers is not null)
            {
                headerLine = _headers;
            }

            var names = _headerRegex?.GetGroupNames();
            if (names is null || names.Length <= 1)
            {
                // there is no explicit capture group, so we assume the entire line is the headers
                return SplitHeaders(headerLine);
            }

            var match = _headerRegex.Match(headerLine);
            if (!match.Success)
            {
                return null;
            }

            var extractedHeader = (match.Groups as IEnumerable<Group>).FirstOrDefault(g => g.Name == HeadersExtractionGroupName);
            if (extractedHeader is null)
            {
                return null;
            }

            return SplitHeaders(extractedHeader.Value);
        }

        private string[] SplitHeaders(string headers)
        {
            if (_delimiter == "," && _csvMode)
            {
                _stringBuilder.Clear();
                return Utility.ParseCSVLine(headers, _stringBuilder).ToArray();
            }
            return headers.Split(_delimiter, StringSplitOptions.None);
        }
    }
}
