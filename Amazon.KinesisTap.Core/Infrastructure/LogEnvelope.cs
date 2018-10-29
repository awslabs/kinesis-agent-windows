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
