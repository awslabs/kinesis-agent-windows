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
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Filesystem
{
    /// <summary>
    /// Each line is log record
    /// </summary>
    public class SingleLineLogParser : ILogParser<string, LogContext>
    {
        private readonly int _skipLines;
        private readonly ILogger _logger;
        private readonly Encoding _encoding;
        private readonly int _bufferSize;

        public SingleLineLogParser(ILogger logger, int skipLines, Encoding encoding, int bufferSize)
        {
            _skipLines = skipLines;
            _logger = logger;
            _encoding = encoding;
            _bufferSize = bufferSize;
        }

        /// <inheritdoc/>
        public async Task ParseRecordsAsync(LogContext context, IList<IEnvelope<string>> output,
            int recordCount, CancellationToken stopToken)
        {
            using (var stream = new FileStream(context.FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                stream.Position = context.Position;

                using (var reader = new LineReader(stream, _encoding, _bufferSize))
                {
                    string line;
                    int consumed;
                    var linesCount = 0;
                    while (linesCount < recordCount)
                    {
                        stopToken.ThrowIfCancellationRequested();
                        (line, consumed) = await reader.ReadAsync(stopToken);
                        _logger.LogTrace("File: '{0}', line: '{1}', bytes: {2}", context.FilePath, line, consumed);
                        context.Position += consumed;
                        if (line is null)
                        {
                            break;
                        }
                        if (context.LineNumber++ < _skipLines)
                        {
                            continue;
                        }

                        var envelope = new LogEnvelope<string>(line,
                           DateTime.UtcNow,
                           line,
                           context.FilePath,
                           context.Position,
                           context.LineNumber);
                        output.Add(envelope);
                        linesCount++;
                    }
                }
            }
        }
    }
}
