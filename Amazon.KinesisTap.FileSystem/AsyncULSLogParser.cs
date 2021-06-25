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
using System.Text;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Filesystem
{
    internal class AsyncULSLogParser : AsyncDelimitedLogParserBase<KeyValueLogRecord>
    {
        public AsyncULSLogParser(ILogger logger, Encoding encoding, int bufferSize)
            : base(logger, "\t", new DelimitedLogParserOptions
            {
                TextEncoding = encoding,
                BufferSize = bufferSize,
                TrimDataFields = true
            })
        {
        }

        protected override KeyValueLogRecord CreateRecord(DelimitedTextLogContext context, Dictionary<string, string> data)
        {
            var timestamp = DateTime.Now;
            if (data.TryGetValue("TimestampUtc", out var timestampText))
            {
                timestamp = DateTime.Parse(timestampText, null, DateTimeStyles.AssumeUniversal);
            }
            else if (data.TryGetValue("Timestamp", out timestampText))
            {
                timestamp = DateTime.Parse(timestampText, null, DateTimeStyles.AssumeLocal);
            }
            return new KeyValueLogRecord(timestamp, data);
        }

        protected override bool IsComment(string line) => false;

        protected override bool IsHeaders(string line, long lineNumber) => line.StartsWith("Timestamp", StringComparison.OrdinalIgnoreCase);

        protected override string[] ParseHeadersLine(string headerLine) => headerLine.Split(_delimiter, StringSplitOptions.None);

        protected override (string, string) KeyValueSelector(string key, string value)
        {
            var (newKey, newValue) = base.KeyValueSelector(key, value);
            //The field name contains spaces that need to be trimmed
            newKey = newKey.Trim();
            if (key.StartsWith("Timestamp", StringComparison.OrdinalIgnoreCase) && newValue.EndsWith("*"))
            {
                //Sometimes timestamp has an extra * at the end that we have to remove
                newValue = newValue[0..^1];
            }

            return (newKey, newValue);
        }
    }
}
