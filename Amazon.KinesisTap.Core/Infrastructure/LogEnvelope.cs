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
    public class LogEnvelope<TData> : Envelope<TData>, ILogEnvelope
    {
        protected string _rawRecord;

        public LogEnvelope(TData data, DateTime timestamp, string rawRecord, string filePath, long position, long lineNumber)
            : base(data, timestamp, null, position)
        {
            _rawRecord = rawRecord;
            LineNumber = lineNumber;
            FilePath = filePath;
        }

        public string FilePath { get; set; }

        public long LineNumber { get; set; }

        public string FileName => Path.GetFileName(FilePath);

        public override string ToString()
        {
            return _rawRecord;
        }

        public override object ResolveMetaVariable(string variable)
        {
            var lowerVariable = variable.ToLower();

            switch (lowerVariable)
            {
                case "_filepath":
                    return FilePath;
                case "_filename":
                    return FileName;
                case "_position":
                    return Position;
                case "_linenumber":
                    return LineNumber;
                default:
                    return base.ResolveMetaVariable(variable);
            }
        }
    }
}
