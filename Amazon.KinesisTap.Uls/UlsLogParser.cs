using Amazon.KinesisTap.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Amazon.KinesisTap.Uls
{
    /// <summary>
    /// Parser for the Sharepoint Uls format
    /// </summary>
    public class UlsLogParser : DelimitedLogParserBase<UlsLogRecord>
    {
        /// <summary>
        /// Uls log is a tab dilimited
        /// </summary>
        public UlsLogParser() : base("\t", (data, context) => new UlsLogRecord(data, context))
        {

        }

        protected override bool IsComment(string line)
        {
            return false;
        }

        protected override bool IsHeader(string line)
        {
            return line != null && line.StartsWith("Timestamp");
        }

        //Need to override the base method because the field name needs to be trimmed
        protected override IDictionary<string, int> GetFieldIndexMap(string fieldsLine)
        {
            string[] fields = GetFields(fieldsLine);
            IDictionary<string, int> fieldIndexMap = new Dictionary<string, int>();
            for (int i = 0; i < fields.Length; i++)
            {
                //The field name contains spaces that need to be trimed
                fieldIndexMap[fields[i].Trim()] = i;
            }
            return fieldIndexMap;
        }

        //Need to override the base SplitData because data needs to be trimmed and sometimes timestamp added with '*' 
        protected override string[] SplitData(string line)
        {
            string[] data = base.SplitData(line)
                .Select(f => f.Trim())
                .ToArray();
            //Sometimes timestamp has an extra * at the end that we have to remove
            string timestamp = data[0];
            if (timestamp.EndsWith("*"))
            {
                data[0] = timestamp.Substring(0, timestamp.Length - 1);
            }
            return data;
        }
    }
}
