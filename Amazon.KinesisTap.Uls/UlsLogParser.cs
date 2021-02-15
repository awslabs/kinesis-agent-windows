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
using Amazon.KinesisTap.Core;
using System.Collections.Generic;
using System.Linq;

namespace Amazon.KinesisTap.Uls
{
    /// <summary>
    /// Parser for the Sharepoint Uls format
    /// </summary>
    public class UlsLogParser : DelimitedLogParserBase<UlsLogRecord>
    {
        /// <summary>
        /// Uls log is a tab delimited
        /// </summary>
        public UlsLogParser() : base("\t", (data, context) => new UlsLogRecord(data, context), null)
        {

        }

        protected override bool IsComment(string line)
        {
            return false;
        }

        protected override bool IsHeader(string line)
        {
            return line != null && line.StartsWith("Timestamp");
        }

        //Need to override the base method because the field name needs to be trimmed
        protected override IDictionary<string, int> GetFieldIndexMap(string fieldsLine)
        {
            string[] fields = GetFields(fieldsLine);
            IDictionary<string, int> fieldIndexMap = new Dictionary<string, int>();
            for (int i = 0; i < fields.Length; i++)
            {
                //The field name contains spaces that need to be trimmed
                fieldIndexMap[fields[i].Trim()] = i;
            }
            return fieldIndexMap;
        }

        //Need to override the base SplitData because data needs to be trimmed and sometimes timestamp added with '*' 
        protected override string[] SplitData(string line)
        {
            string[] data = base.SplitData(line)
                .Select(f => f.Trim())
                .ToArray();
            //Sometimes timestamp has an extra * at the end that we have to remove
            string timestamp = data[0];
            if (timestamp.EndsWith("*"))
            {
                data[0] = timestamp.Substring(0, timestamp.Length - 1);
            }
            return data;
        }
    }
}
