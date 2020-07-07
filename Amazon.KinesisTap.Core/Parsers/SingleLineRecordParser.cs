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

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Eg: each line is a single record
    /// </summary>
    public class SingleLineRecordParser : IRecordParser<string, LogContext>
    {
        public IEnumerable<IEnvelope<string>> ParseRecords(StreamReader sr, LogContext context)
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
                    yield return new LogEnvelope<string>(record, 
                        DateTime.UtcNow, 
                        record, 
                        context.FilePath, 
                        context.Position,
                        context.LineNumber);
                }
            }
        }
    }
}
