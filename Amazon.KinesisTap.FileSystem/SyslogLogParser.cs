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
using SyslogDotnet.Rfc3164;
using SyslogDotnet.Rfc5424;

namespace Amazon.KinesisTap.Filesystem
{
    /// <summary>
    /// Each line is a syslog-format record
    /// </summary>
    internal class SyslogLogParser : ILogParser<SyslogData, LogContext>
    {
        private readonly bool _isRfc5424;
        private readonly Encoding _encoding;
        private readonly ILogger _logger;
        private readonly int _bufferSize;

        public SyslogLogParser(ILogger logger, bool isRfc5424, Encoding encoding, int bufferSize)
        {
            _logger = logger;
            _isRfc5424 = isRfc5424;
            _encoding = encoding;
            _bufferSize = bufferSize;
        }

        public async Task ParseRecordsAsync(LogContext context, IList<IEnvelope<SyslogData>> output,
            int recordCount, CancellationToken stopToken)
        {
            using (var stream = new FileStream(context.FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                stream.Position = context.Position;

                using (var reader = new LineReader(stream, _encoding, _bufferSize))
                {
                    if (_isRfc5424)
                    {
                        await ParseRfc5424Log(reader, context, output, recordCount, stopToken);
                    }
                    else
                    {
                        await ParseRfc3164Log(reader, context, output, recordCount, stopToken);
                    }
                }
            }
        }

        private async Task ParseRfc3164Log(LineReader reader, LogContext context, IList<IEnvelope<SyslogData>> output,
            int recordCount, CancellationToken stopToken)
        {
            var packet = new Rfc3164Packet();
            var parser = new Rfc3164Parser(new Rfc3164ParserOptions
            {
                RequirePri = false,
                DefaultYear = DateTime.Now.Year
            });

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
                context.LineNumber++;

                var valid = parser.ParseString(line, ref packet);
                if (!valid)
                {
                    _logger.LogWarning($"Unable to parse record at line {context.LineNumber} in file {context.FilePath}. Record may be in invalid format");
                }
                var record = new SyslogData(
                    packet.TimeStamp ?? DateTimeOffset.Now,
                    packet.HostName,
                    packet.Tag,
                    packet.Content);

                var envelope = new LogEnvelope<SyslogData>(
                    record,
                    record.Timestamp.UtcDateTime,
                    line,
                    context.FilePath,
                    context.Position,
                    context.LineNumber);

                output.Add(envelope);
                linesCount++;
            }
        }

        private async Task ParseRfc5424Log(LineReader reader, LogContext context, IList<IEnvelope<SyslogData>> output,
            int recordCount, CancellationToken stopToken)
        {
            var packet = new Rfc5424Packet();
            var parser = new Rfc5424Parser();

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
                context.LineNumber++;

                var valid = parser.ParseString(line, ref packet);
                if (!valid)
                {
                    _logger.LogWarning($"Unable to parse record at line {context.LineNumber} in file {context.FilePath}. Record may be in invalid format");
                }

                var record = new SyslogData(
                    packet.TimeStamp ?? DateTimeOffset.Now,
                    packet.HostName,
                    packet.AppName,
                    packet.Message);

                var envelope = new LogEnvelope<SyslogData>(
                    record,
                    record.Timestamp.UtcDateTime,
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
