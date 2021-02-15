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

namespace Amazon.KinesisTap.Core
{
    public class ExchangeLogParser : DelimitedLogParserBase<ExchangeLogRecord>
    {
        protected const string FIELDS = "#Fields: ";

        public ExchangeLogParser() : base(",", (data, context) => new ExchangeLogRecord(data, context), null)
        {
        }

        protected override bool IsComment(string line)
        {
            return line.StartsWith("#") || line.StartsWith("Date");
        }

        protected override bool IsHeader(string line)
        {
            return line.StartsWith(FIELDS);
        }

        protected override string[] GetFields(string fieldsLine)
        {
            return base.GetFields(fieldsLine.Substring(FIELDS.Length));
        }

        protected override void AnalyzeMapping(DelimitedLogContext context)
        {
            base.AnalyzeMapping(context);
            if (!string.IsNullOrWhiteSpace(this.TimeStampField))
            {
                context.TimeStampField = TimeStampField;
            }
            else if (context.Mapping.ContainsKey("date-time"))
            {
                context.TimeStampField = "date-time";
            }
            else if (context.Mapping.ContainsKey("DateTime"))
            {
                context.TimeStampField = "DateTime";
            }
            else
            {
                throw new Exception("Exchange log parser cannot determine date-time field");
            }
        }
    }
}
