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
using System.Text;

using Newtonsoft.Json.Linq;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Each line is a Json object
    /// </summary>
    public class SingleLineJsonParser : IRecordParser<JObject, LogContext>
    {
        private readonly Func<JObject, DateTime> _getTimestamp;

        public SingleLineJsonParser(string timestampField, string timestampFormat)
        {
            if (!string.IsNullOrEmpty(timestampField) || !string.IsNullOrEmpty(timestampFormat))
            {
                //If one is provided, then timestampField is required
                Guard.ArgumentNotNullOrEmpty(timestampField, "TimestampField is required for SingleLineJsonParser");
                TimestampExtrator timestampExtractor = new TimestampExtrator(timestampField, timestampFormat);
                _getTimestamp = timestampExtractor.GetTimestamp;
            }
            else
            {
                _getTimestamp = jobject => DateTime.UtcNow;
            }
        }

        public IEnumerable<IEnvelope<JObject>> ParseRecords(StreamReader sr, LogContext context)
        {
            if (context.Position > sr.BaseStream.Position)
            {
                sr.BaseStream.Position = context.Position;
            }

            while (!sr.EndOfStream)
            {
                string record = sr.ReadLine();
                context.LineNumber++;
                if (!string.IsNullOrWhiteSpace(record))
                {
                    JObject jObject = JObject.Parse(record);
                    yield return new LogEnvelope<JObject>(jObject,
                        _getTimestamp(jObject),
                        record,
                        context.FilePath,
                        context.Position,
                        context.LineNumber);
                }
            }
        }
    }
}
