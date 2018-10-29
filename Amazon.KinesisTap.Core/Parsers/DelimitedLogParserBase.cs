using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public abstract class DelimitedLogParserBase<TData> : IRecordParser<TData, DelimitedLogContext> where TData : DelimitedLogRecordBase
    {
        protected string _delimiter;
        protected Func<string[], DelimitedLogContext, TData> _recordFactoryMethod;
        protected DateTimeKind _timeZoneKind;

        protected DelimitedLogParserBase(
            string delimiter,
            Func<string[], DelimitedLogContext, TData> recordFactoryMethod
        ) : this(delimiter, recordFactoryMethod, DateTimeKind.Utc)
        {
        }

        protected DelimitedLogParserBase(
            string delimiter,
            Func<string[], DelimitedLogContext, TData> recordFactoryMethod,
            DateTimeKind timeZoneKind
        )
        {
            _delimiter = delimiter;
            _recordFactoryMethod = recordFactoryMethod;
            _timeZoneKind = timeZoneKind;
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

            while (!sr.EndOfStream)
            {
                string line = sr.ReadLine();
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
                    yield return new LogEnvelope<TData>(
                        record,
                        ToUniversalTime(record.TimeStamp),
                        line,
                        context.FilePath,
                        context.Position,
                        context.LineNumber);
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
