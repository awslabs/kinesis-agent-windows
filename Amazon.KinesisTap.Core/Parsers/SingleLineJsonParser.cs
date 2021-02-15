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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Each line is a Json object
    /// </summary>
    public class SingleLineJsonParser : IRecordParser<JObject, LogContext>
    {
        private readonly ConcurrentDictionary<string, FileLineReader> _readers = new ConcurrentDictionary<string, FileLineReader>();
        private readonly ILogger _logger;
        private readonly Func<JObject, DateTime> _getTimestamp;

        public SingleLineJsonParser(string timestampField, string timestampFormat, ILogger logger)
        {
            _logger = logger;
            if (!string.IsNullOrEmpty(timestampField) || !string.IsNullOrEmpty(timestampFormat))
            {
                //If one is provided, then timestampField is required
                Guard.ArgumentNotNullOrEmpty(timestampField, "TimestampField is required for SingleLineJsonParser");
                var timestampExtractor = new TimestampExtrator(timestampField, timestampFormat);
                _getTimestamp = timestampExtractor.GetTimestamp;
            }
            else
            {
                _getTimestamp = jobject => DateTime.UtcNow;
            }
        }

        public IEnumerable<IEnvelope<JObject>> ParseRecords(StreamReader sr, LogContext context)
        {
            var baseStream = sr.BaseStream;
            var filePath = context.FilePath;
            var lineReader = _readers.GetOrAdd(filePath, f => new FileLineReader());
            if (context.Position > baseStream.Position)
            {
                baseStream.Position = context.Position;
            }
            else if (context.Position == 0)
            {
                // this might happen due to the file being truncated
                // in that case, we need to reset the reader's state
                lineReader.Reset();
                context.LineNumber = 0;
            }

            string line;
            do
            {
                line = lineReader.ReadLine(baseStream, sr.CurrentEncoding ?? Encoding.UTF8);
                if (line is null)
                {
                    yield break;
                }

                context.LineNumber++;
                _logger.LogDebug("ReadLine '{0}'", line);

                if (line.Length == 0)
                {
                    // an 'empty' line, ignore
                    continue;
                }

                JObject jObject;
                try
                {
                    jObject = JObject.Parse(line);
                }
                catch (JsonReaderException jre)
                {
                    _logger.LogError(0, jre, "Error parsing log file '{0}' at line {1}", filePath, context.LineNumber);
                    jObject = null;
                }

                if (jObject is null)
                {
                    // this means that the line is not a valid JSON, skip and read next line
                    continue;
                }

                yield return new LogEnvelope<JObject>(jObject,
                       _getTimestamp(jObject),
                       line,
                       context.FilePath,
                       context.Position,
                       context.LineNumber);
            } while (line != null);
        }
    }
}
