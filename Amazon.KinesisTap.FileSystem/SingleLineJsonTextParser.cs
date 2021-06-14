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
using Newtonsoft.Json.Linq;

namespace Amazon.KinesisTap.Filesystem
{
    public class SingleLineJsonTextParser : ILogParser<JObject, LogContext>
    {
        private readonly ILogger _logger;
        private readonly Encoding _encoding;
        private readonly TimestampExtrator _timestampExtrator;

        public SingleLineJsonTextParser(ILogger logger, string timestampField, string timestampFormat, Encoding encoding)
        {
            _logger = logger;
            _encoding = encoding;
            if (timestampField != null)
            {
                _timestampExtrator = new TimestampExtrator(timestampField, timestampFormat);
            }
        }

        public async Task ParseRecordsAsync(LogContext context, IList<IEnvelope<JObject>> output,
            int recordCount, CancellationToken stopToken = default)
        {
            using (var stream = new FileStream(context.FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                stream.Position = context.Position;

                using (var reader = new LineReader(stream, _encoding))
                {
                    var linesCount = 0;
                    while (linesCount < recordCount)
                    {
                        stopToken.ThrowIfCancellationRequested();
                        var (line, consumed) = await reader.ReadAsync(stopToken);
                        _logger.LogTrace("File: '{0}', line: '{1}', bytes: {2}", context.FilePath, line, consumed);
                        context.Position += consumed;
                        if (line is null)
                        {
                            break;
                        }

                        try
                        {
                            var jObject = JObject.Parse(line);
                            var timestamp = _timestampExtrator is null
                                ? DateTime.Now
                                : _timestampExtrator.GetTimestamp(jObject);

                            var envelope = new LogEnvelope<JObject>(jObject,
                               timestamp,
                               line,
                               context.FilePath,
                               context.Position,
                               context.LineNumber);

                            output.Add(envelope);
                            linesCount++;
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
    }
}
