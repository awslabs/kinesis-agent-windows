using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public interface ILogEnvelope : IEnvelope
    {
        string FilePath { get; }
        string FileName { get; }
        long Position { get; }
        long LineNumber { get; }
    }
}
