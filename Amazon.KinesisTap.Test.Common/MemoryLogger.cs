using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Core.Test
{
    public class MemoryLogger : ILogger, IDisposable
    {
        private readonly string _categoryName;
        private readonly List<String> _entries;

        public MemoryLogger(string categoryName)
        {
            _entries = new List<string>();
            _categoryName = categoryName;
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, 
            Func<TState, Exception, string> formatter)
        {
            _entries.Add(formatter(state, exception));
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            return new NoopDisposable();
        }

        public void Dispose()
        {
            _entries?.Clear();
        }

        public IList<string> Entries => _entries;

        public string LastEntry => _entries[_entries.Count - 1];

        private class NoopDisposable : IDisposable
        {
            public void Dispose()
            {
            }
        }
    }
}
