using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public class DelimitedLogTimestampExtrator
    {
        private readonly string _formatSpec;
        private readonly string _parseSpec;
        private readonly List<string> _fields = new List<string>();

        /// <summary>
        /// Constructor for DelimitedLogTimestampExtrator
        /// </summary>
        /// <param name="timestampField">Describe the column(s) that form the timestamp, e.g., {Date} {Time}</param>
        /// <param name="timestampFormat">Describe the format to parse the timestamp, e.g., MM/dd/yy HH:mm:ss</param>
        public DelimitedLogTimestampExtrator(string timestampField, string timestampFormat)
        {
            Guard.ArgumentNotNullOrEmpty(timestampField, "timestampField");
            Guard.ArgumentNotNullOrEmpty(timestampFormat, "timestampFormat");
            _parseSpec = timestampFormat;
            if (timestampField.IndexOf("{") < 0) //The entire string is a single field
            {
                _formatSpec = "{0}";
                _fields.Add(timestampField);
            }
            else
            {
                int fieldCounter = 0;
                _formatSpec = Utility.ResolveVariables(timestampField, s =>
                {
                    _fields.Add(s.Substring(1, s.Length - 2));  //Remove {}
                    return "{" + (fieldCounter++) + "}";
                });
            }
        }

        /// <summary>
        /// Get the timestamp from the log record.
        /// </summary>
        /// <param name="record">The record to extract timestamp.</param>
        /// <returns></returns>
        public DateTime GetTimestamp(DelimitedLogRecordBase record)
        {
            string[] values = _fields.Select(f => record[f]).ToArray();
            string formatted = string.Format(_formatSpec, values);
            return DateTime.ParseExact(formatted, _parseSpec, CultureInfo.InvariantCulture);
        }
    }
}
