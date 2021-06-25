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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Filesystem
{
    /// <summary>
    /// Parser for W3SVC log format https://docs.microsoft.com/en-us/windows/win32/http/w3c-logging
    /// </summary>
    internal class AsyncW3SVCLogParser : AsyncDelimitedLogParserBase<W3SVCRecord>
    {
        protected const string FIELDS = "#Fields: ";
        private readonly string _defaultHeaders;

        public AsyncW3SVCLogParser(ILogger logger, string defaultHeaders, DelimitedLogParserOptions options)
            : base(logger, " ", options)
        {
            _defaultHeaders = defaultHeaders;
        }

        protected override W3SVCRecord CreateRecord(DelimitedTextLogContext context, Dictionary<string, string> data)
        {
            var timestamp = DateTime.UtcNow;
            if (data.TryGetValue("date", out var date) && data.TryGetValue("time", out var time))
            {
                // date and time field is UTC-based: https://docs.microsoft.com/en-us/windows/win32/http/w3c-logging
                timestamp = DateTime.Parse(date + "T" + time + "Z", null, DateTimeStyles.RoundtripKind);
            }

            return new W3SVCRecord(timestamp, data);
        }

        protected override async Task<string[]> TryGetHeaderFields(DelimitedTextLogContext context, CancellationToken stopToken)
        {
            if (_defaultHeaders is not null)
            {
                return ParseHeadersLine(_defaultHeaders);
            }

            return await base.TryGetHeaderFields(context, stopToken);
        }

        protected override bool IsHeaders(string line, long lineNumber) => line != null && line.StartsWith(FIELDS);

        protected override string[] ParseHeadersLine(string headerLine) => headerLine[FIELDS.Length..].Split(_delimiter, StringSplitOptions.None);

        protected override bool IsComment(string line) => line != null && line.StartsWith("#");

        protected override bool ShouldStopAndRollback(string line, DelimitedTextLogContext context)
        {
            //Sometimes the log writer may expand the log by writing a block of \x00
            //In this case, we rewind the position and retry from the position again
            if (line.StartsWith("\x00", StringComparison.Ordinal))
            {
                return true;
            }
            return base.ShouldStopAndRollback(line, context);
        }
    }
}
