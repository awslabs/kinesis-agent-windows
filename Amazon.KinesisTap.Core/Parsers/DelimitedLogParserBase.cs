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
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public abstract class DelimitedLogParserBase<TData> : IRecordParser<TData, DelimitedLogContext> where TData : DelimitedLogRecordBase
    {
        protected readonly IPlugInContext _plugInContext;
        protected readonly string _delimiter;
        protected readonly Func<string[], DelimitedLogContext, TData> _recordFactoryMethod;
        protected readonly DateTimeKind _timeZoneKind;
        protected readonly string _defaultMapping;

        protected DelimitedLogParserBase(
            string delimiter,
            Func<string[], DelimitedLogContext, TData> recordFactoryMethod,
            string defaultMapping
        ) : this(null, delimiter, recordFactoryMethod, DateTimeKind.Utc, defaultMapping)
        {
        }

        protected DelimitedLogParserBase(
            IPlugInContext plugInContext,
            string delimiter,
            Func<string[], DelimitedLogContext, TData> recordFactoryMethod,
            string defaultMapping
        ) : this(plugInContext, delimiter, recordFactoryMethod, DateTimeKind.Utc, defaultMapping)
        {
        }

        protected DelimitedLogParserBase(
            string delimiter,
            Func<string[], DelimitedLogContext, TData> recordFactoryMethod,
            DateTimeKind timeZoneKind,
            string defaultMapping
        ) : this(null, delimiter, recordFactoryMethod, timeZoneKind, defaultMapping)
        {
        }

        protected DelimitedLogParserBase(
            IPlugInContext plugInContext,
            string delimiter,
            Func<string[], DelimitedLogContext, TData> recordFactoryMethod,
            DateTimeKind timeZoneKind,
            string defaultMapping
        )
        {
            _plugInContext = plugInContext;
            _delimiter = delimiter;
            _recordFactoryMethod = recordFactoryMethod;
            _timeZoneKind = timeZoneKind;
            _defaultMapping = defaultMapping;
        }

        public virtual IEnumerable<IEnvelope<TData>> ParseRecords(StreamReader sr, DelimitedLogContext context)
        {
            if (context.Position > 0)
            {
                if (context.Mapping == null)
                {
                    //Need to get the fieldIndexMap
                    context.Mapping = GetFieldIndexMap(sr, context);
                    if (context.Mapping != null)
                    {
                        AnalyzeMapping(context);
                    }
                }
                else
                {
                    sr.BaseStream.Position = context.Position;
                }
            }

            if (context.Mapping == null && _defaultMapping != null)
            {
                context.Mapping = GetFieldIndexMap(_defaultMapping);
            }

            while (!sr.EndOfStream)
            {
                string line = sr.ReadLine();
                if (ShouldStopReading(line, sr, context)) break;

                context.LineNumber++;
                if (IsHeader(line))
                {
                    context.Mapping = GetFieldIndexMap(line);
                    AnalyzeMapping(context);
                }
                else if (IsComment(line))
                {
                    continue;
                }
                else
                {
                    string[] data;
                    if (_delimiter == ",")
                    {
                        data = Utility.ParseCSVLine(line, new StringBuilder()).ToArray();
                    }
                    else
                    {
                        data = SplitData(line);
                    }
                    TData record = _recordFactoryMethod(data, context);
                    //If we failed to get the timestamp, we log the error and continue reading the log
                    DateTime? timestamp = null;
                    try
                    {
                        timestamp = record.TimeStamp;
                    }
                    catch (Exception ex)
                    {
                        _plugInContext?.Logger?.LogError($"Failed to get time stamp in {context.FilePath}, {context.LineNumber}: {ex.ToMinimized()}");
                        _plugInContext?.Logger?.LogError(line);
                    }
                    if (timestamp.HasValue)
                    {
                        yield return new LogEnvelope<TData>(
                            record,
                            ToUniversalTime(timestamp.Value),
                            line,
                            context.FilePath,
                            context.Position,
                            context.LineNumber);
                    }
                }
            }
        }

        public string TimeStampField { get; set; }

        protected virtual string[] SplitData(string line)
        {
            return line.Split(new[] { _delimiter }, StringSplitOptions.None);
        }

        protected abstract bool IsComment(string line);

        protected abstract bool IsHeader(string line);

        protected virtual bool ShouldStopReading(string line, StreamReader sr, DelimitedLogContext context)
        {
            return false;
        }

        protected virtual void AnalyzeMapping(DelimitedLogContext context) { }

        /// <summary>
        /// Get field index map from the #Fields line
        /// </summary>
        /// <param name="fieldsLine"></param>
        /// <returns></returns>
        protected virtual IDictionary<string, int> GetFieldIndexMap(string fieldsLine)
        {
            string[] fields = GetFields(fieldsLine);
            IDictionary<string, int> fieldIndexMap = new Dictionary<string, int>();
            for (int i = 0; i < fields.Length; i++)
            {
                if (!string.IsNullOrWhiteSpace(fields[i]))
                {
                    fieldIndexMap[fields[i].Trim()] = i;
                }
            }
            return fieldIndexMap;
        }

        protected virtual string[] GetFields(string fieldsLine)
        {
            return fieldsLine.Split(new[] { _delimiter }, StringSplitOptions.None);
        }

        /// <summary>
        /// Get field index map from a stream up to the position
        /// </summary>
        /// <param name="fs"></param>
        /// <param name="position"></param>
        /// <returns></returns>
        protected IDictionary<string, int> GetFieldIndexMap(StreamReader sr, DelimitedLogContext context)
        {
            IDictionary<string, int> fieldIndexMap = null;
            while (!sr.EndOfStream && sr.BaseStream.Position < context.Position)
            {
                string line = sr.ReadLine();
                context.LineNumber++;
                if (IsHeader(line))
                {
                    fieldIndexMap = GetFieldIndexMap(line);
                }
            }
            return fieldIndexMap;
        }

        protected DateTime ToUniversalTime(DateTime? dateTime)
        {
            if (!dateTime.HasValue)
                return DateTime.UtcNow;

            if (_timeZoneKind == DateTimeKind.Local)
            {
                return dateTime.Value.ToUniversalTime();
            }
            else
            {
                return dateTime.Value;
            }
        }
    }
}
