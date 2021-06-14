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
using System.Text;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Filesystem
{
    internal class AsyncExchangeLogParser : GenericDelimitedLogParser
    {
        private const string FIELDS = "#Fields: ";

        public AsyncExchangeLogParser(ILogger logger, string timestampField, Encoding encoding, int bufferSize)
            : base(logger, ",", new GenericDelimitedLogParserOptions
            {
                CSVEscapeMode = true,
                TextEncoding = encoding,
                TimestampField = timestampField,
                BufferSize = bufferSize,
                HeadersPattern = "^#Fields:"
            })
        {
        }

        protected override bool IsComment(string line) => line.StartsWith("#") || line.StartsWith("Date");

        protected override bool IsHeaders(string line, long lineNumber) => line.StartsWith(FIELDS);

        protected override string[] ParseHeadersLine(string headersLine) => base.ParseHeadersLine(headersLine[FIELDS.Length..]);

        protected override KeyValueLogRecord CreateRecord(DelimitedTextLogContext context, Dictionary<string, string> data)
        {
            // we need to try to recoginize the timestamp
            if (_timestampExtractor is null)
            {
                // no 'TimestampField', try to figure out the timestamp
                if (data.TryGetValue("date-time", out var timestampText) && DateTime.TryParse(timestampText, out var timestamp))
                {
                    return new KeyValueLogRecord(timestamp, data);
                }

                if (data.TryGetValue("DateTime", out timestampText) && DateTime.TryParse(timestampText, out timestamp))
                {
                    return new KeyValueLogRecord(timestamp, data);
                }
            }

            // this means either the user have specified a timestamp field, or no timestamp is recognized,
            // use the parent method
            return base.CreateRecord(context, data);
        }
    }
}
