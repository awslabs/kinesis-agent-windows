using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core.Test
{
    public class MemoryLoggerProvider : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName)
        {
            return new MemoryLogger(categoryName);
        }

        public void Dispose()
        {
        }
    }
}
