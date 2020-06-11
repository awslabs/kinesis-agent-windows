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
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Core.Test
{
    public class MemoryLogger : ILogger, IDisposable
    {
        private readonly string _categoryName;
        private readonly List<string> _entries;
        private readonly List<LogLevel> _levels;

        public MemoryLogger(string categoryName)
        {
            _entries = new List<string>();
            _levels = new List<LogLevel>();
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
            _levels.Add(logLevel);
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

        public IList<LogLevel> LogLevels => _levels;

        public string LastEntry => _entries[_entries.Count - 1];

        private class NoopDisposable : IDisposable
        {
            public void Dispose()
            {
            }
        }
    }
}
