using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core.Test
{
    public static class MemoryLoggerExtensions
    {
        public static ILoggerFactory AddMemoryLogger(this ILoggerFactory loggerFactory)
        {
            loggerFactory.AddProvider(new MemoryLoggerProvider());

            return loggerFactory;
        }
    }
}
