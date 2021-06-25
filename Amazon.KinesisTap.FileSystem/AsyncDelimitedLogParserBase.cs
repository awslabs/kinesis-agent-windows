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
    /// Common base class for parsing logs with delimiters.
    /// </summary>
    public abstract class AsyncDelimitedLogParserBase<TData> : ILogParser<TData, DelimitedTextLogContext> where TData : KeyValueLogRecord
    {
        protected readonly ILogger _logger;
        protected readonly string _delimiter;
        protected readonly Encoding _encoding;
        private readonly int _bufferSize;
        private readonly bool _trimDataValues;

        public AsyncDelimitedLogParserBase(ILogger logger, string delimiter, DelimitedLogParserOptions options)
        {
            _logger = logger;
            _delimiter = delimiter;
            _encoding = options.TextEncoding;
            _bufferSize = options.BufferSize;
            _trimDataValues = options.TrimDataFields;
        }

        /// <inheritdoc/>
        public async Task ParseRecordsAsync(DelimitedTextLogContext context, IList<IEnvelope<TData>> output,
            int recordCount, CancellationToken stopToken = default)
        {
            if (context.Fields is null)
            {
                context.Fields = await TryGetHeaderFields(context, stopToken);
            }

            var count = 0;
            using (var stream = new FileStream(context.FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                stream.Position = context.Position;
                using (var reader = new LineReader(stream, _encoding, _bufferSize))
                {
                    while (count < recordCount)
                    {
                        stopToken.ThrowIfCancellationRequested();
                        var (line, consumed) = await reader.ReadAsync(stopToken);
                        _logger.LogTrace("File: '{0}', line: '{1}', bytes: {2}", context.FilePath, line, consumed);
                        context.Position += consumed;

                        if (line is null)
                        {
                            break;
                        }

                        if (ShouldStopAndRollback(line, context))
                        {
                            context.Position -= consumed;
                            return;
                        }

                        context.LineNumber++;

                        if (IsHeaders(line, context.LineNumber))
                        {
                            context.Fields = ParseHeadersLine(line);
                            continue;
                        }
                        else if (IsComment(line))
                        {
                            continue;
                        }

                        try
                        {
                            // 'ParseDataFragments' and 'CreateRecord' might throw error, so we need to catch it and skip the record
                            var fragments = ParseDataFragments(line);

                            if (context.Fields is null)
                            {
                                _logger.LogWarning("Unknown field mapping, skipping line {0}", context.LineNumber);
                                continue;
                            }
                            var dict = new Dictionary<string, string>();
                            for (var i = 0; i < context.Fields.Length; i++)
                            {
                                if (i >= fragments.Length)
                                {
                                    break;
                                }
                                var (key, val) = KeyValueSelector(context.Fields[i], fragments[i]);
                                dict[key] = val;
                            }

                            var record = CreateRecord(context, dict);

                            var envelope = new LogEnvelope<TData>(record, record.Timestamp, line, context.FilePath, context.Position, context.LineNumber);
                            output.Add(envelope);
                            count++;
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error processing record '{0}'", line);
                            continue;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// When overriden, returns the desired key-value pair from the parsed data.
        /// </summary>
        protected virtual (string, string) KeyValueSelector(string key, string value)
        {
            if (_trimDataValues)
            {
                value = value.Trim();
            }
            return (key, value);
        }

        /// <summary>
        /// When implemented, returns true iff the line indicates that the reader should stop and rewind.
        /// </summary>
        protected virtual bool ShouldStopAndRollback(string line, DelimitedTextLogContext context) => false;

        /// <summary>
        /// When implemented, returns true iff the line is a 'comment' and not a record.
        /// </summary>
        protected abstract bool IsComment(string line);

        /// <summary>
        /// When immplemented, create a record based on the parsed key-value pairs.
        /// </summary>
        protected abstract TData CreateRecord(DelimitedTextLogContext context, Dictionary<string, string> data);

        /// <summary>
        /// When overriden, return the data fragments in the text line separated by the delimiter.
        /// </summary>
        protected virtual string[] ParseDataFragments(string line) => line.Split(_delimiter, StringSplitOptions.None);

        /// <summary>
        /// When overriden, return the header fields for the log file with a given <paramref name="context"/>,
        /// or 'null' if none is found
        /// </summary>
        protected virtual async Task<string[]> TryGetHeaderFields(DelimitedTextLogContext context, CancellationToken stopToken)
        {
            var position = 0;
            long lineNumber = 0;
            using (var stream = new FileStream(context.FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var reader = new LineReader(stream, _encoding, _bufferSize))
            {
                while (position < context.Position)
                {
                    stopToken.ThrowIfCancellationRequested();
                    var (line, consumed) = await reader.ReadAsync(stopToken);
                    if (line is null)
                    {
                        break;
                    }
                    lineNumber++;
                    position += consumed;
                    if (IsHeaders(line, lineNumber))
                    {
                        return ParseHeadersLine(line);
                    }
                }
            }

            return null;
        }

        protected abstract string[] ParseHeadersLine(string headerLine);

        protected abstract bool IsHeaders(string line, long lineNumber);
    }
}
