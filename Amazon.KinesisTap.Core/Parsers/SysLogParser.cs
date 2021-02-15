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
using Microsoft.Extensions.Logging;
using SyslogDotnet.Rfc3164;
using SyslogDotnet.Rfc5424;
using System;
using System.Collections.Generic;
using System.IO;

namespace Amazon.KinesisTap.Core
{
    public class SyslogParser : IRecordParser<SyslogData, LogContext>
    {
        private readonly bool _isRfc5424;
        private readonly ILogger _logger;

        public SyslogParser(ILogger logger, bool isRfc5424)
        {
            _logger = logger;
            _isRfc5424 = isRfc5424;
        }

        public IEnumerable<IEnvelope<SyslogData>> ParseRecords(StreamReader sr, LogContext context)
        {
            if (context.Position > sr.BaseStream.Position)
            {
                sr.BaseStream.Position = context.Position;
            }

            return _isRfc5424
                ? ParseRfc5424Log(sr, context)
                : ParseRfc3164Log(sr, context);
        }

        private IEnumerable<IEnvelope<SyslogData>> ParseRfc3164Log(StreamReader sr, LogContext context)
        {
            var packet = new Rfc3164Packet();
            var parser = new Rfc3164Parser(new Rfc3164ParserOptions
            {
                RequirePri = false,
                DefaultYear = DateTime.Now.Year
            });
            while (!sr.EndOfStream)
            {
                // TODO change the ReadLine() functionality to using the full-line reader
                var line = sr.ReadLine();
                context.LineNumber++;
                if (!string.IsNullOrWhiteSpace(line))
                {
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

                    yield return new LogEnvelope<SyslogData>(record,
                        record.Timestamp.UtcDateTime,
                        line,
                        context.FilePath,
                        context.Position,
                        context.LineNumber);
                }
            }
        }

        private IEnumerable<IEnvelope<SyslogData>> ParseRfc5424Log(StreamReader sr, LogContext context)
        {
            var packet = new Rfc5424Packet();
            var parser = new Rfc5424Parser();
            while (!sr.EndOfStream)
            {
                // TODO change the ReadLine() functionality to using the full-line reader
                var line = sr.ReadLine();
                context.LineNumber++;
                if (!string.IsNullOrWhiteSpace(line))
                {
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

                    yield return new LogEnvelope<SyslogData>(record,
                        record.Timestamp.DateTime.ToUniversalTime(),
                        line,
                        context.FilePath,
                        context.Position,
                        context.LineNumber);
                }
            }
        }
    }
}
