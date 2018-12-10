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

namespace Amazon.KinesisTap.Core
{
    public class LogEnvelope<TData> : Envelope<TData>, ILogEnvelope
    {
        protected string _rawRecord;
        protected string _filePath;
        protected long _position;
        protected long _lineNumber;

        public LogEnvelope(TData data, DateTime timestamp, string rawRecord, string filePath, long position, long lineNumber) : base(data, timestamp)
        {
            _rawRecord = rawRecord;
            _filePath = filePath;
            _position = position;
            _lineNumber = lineNumber;
        }

        public string FilePath => _filePath;

        public string FileName => Path.GetFileName(_filePath);

        public long Position => _position;

        public long LineNumber => _lineNumber;

        public override string ToString()
        {
            return _rawRecord;
        }
    }
}
