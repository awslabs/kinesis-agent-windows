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
using System.IO;

namespace Amazon.KinesisTap.Core
{
    public class W3SVCLogParser : DelimitedLogParserBase<W3SVCLogRecord>
    {
        protected const string FIELDS = "#Fields: ";

        public W3SVCLogParser(IPlugInContext plugInContext, string defaultMapping)
            : base(plugInContext, " ", (data, context) => new W3SVCLogRecord(data, context), defaultMapping)
        {
        }

        protected override bool IsComment(string line)
        {
            return line.StartsWith("#");
        }

        protected override bool IsHeader(string line)
        {
            return line.StartsWith(FIELDS);
        }

        protected override string[] GetFields(string fieldsLine)
        {
            return base.GetFields(fieldsLine.Substring(FIELDS.Length));
        }

        protected override bool ShouldStopReading(string line, StreamReader sr, DelimitedLogContext context)
        {
            //Sometimes the log writer may expand the log by writing a block of \x00
            //In this case, we rewind the position and retry from the position again
            if (line.StartsWith("\x00", StringComparison.Ordinal))
            {
                //Need to rewind the position
                context.Position = sr.BaseStream.Position - line.Length;
                return true;
            }
            else
            {
                return base.ShouldStopReading(line, sr, context);
            }
        }
    }
}
