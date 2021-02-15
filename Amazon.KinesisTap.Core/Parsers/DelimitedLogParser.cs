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
using System.IO;
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
            IPlugInContext plugInContext,
            string delimiter,
            Func<string[], DelimitedLogContext, DelimitedLogRecord> recordFactoryMethod,
            string headerPattern,
            string recordPattern,
            string commentPattern,
            string headers,
            DateTimeKind timeZoneKind
        ) : base(plugInContext, delimiter, recordFactoryMethod, timeZoneKind, null)
        {
            if (!string.IsNullOrWhiteSpace(headerPattern)) _headerRegex = new Regex(headerPattern);
            if (!string.IsNullOrWhiteSpace(recordPattern)) _recordRegex = new Regex(recordPattern);
            if (!string.IsNullOrWhiteSpace(commentPattern)) _commentRegex = new Regex(commentPattern);
            if (!string.IsNullOrWhiteSpace(headers)) _headers = headers.Trim();
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
