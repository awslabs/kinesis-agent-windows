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
using System.Globalization;
using System.Linq;
using System.Text;

using Newtonsoft.Json.Linq;

namespace Amazon.KinesisTap.Core
{
    public class TimestampExtrator
    {
        protected readonly string _formatSpec;
        protected readonly string _parseSpec;
        protected readonly List<string> _fields = new List<string>();

        /// <summary>
        /// Constructor for DelimitedLogTimestampExtrator
        /// </summary>
        /// <param name="timestampField">Describe the column(s) that form the timestamp, e.g., {Date} {Time}</param>
        /// <param name="timestampFormat">Describe the format to parse the timestamp, e.g., MM/dd/yy HH:mm:ss</param>
        public TimestampExtrator(string timestampField, string timestampFormat)
        {
            Guard.ArgumentNotNullOrEmpty(timestampField, "timestampField");
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
        public DateTime GetTimestamp(IReadOnlyDictionary<string, string> record)
        {
            string[] values = _fields.Select(f => record[f]).ToArray();
            string formatted = string.Format(_formatSpec, values);
            return Utility.ParseDatetime(formatted, _parseSpec);
        }

        /// <summary>
        /// Get the timestamp from the log record.
        /// </summary>
        /// <param name="record">The record to extract timestamp.</param>
        /// <returns></returns>
        public DateTime GetTimestamp(IDictionary<string, JToken> record)
        {
            JToken[] values = _fields.Select(f => record[f]).ToArray();

            if (_fields.Count == 1)
            {
                //Single field
                JToken token = values[0];
                if (token.Type == JTokenType.Date)
                {
                    return (DateTime)token;
                }
                else if (token.Type == JTokenType.Integer && ConfigConstants.EPOCH.Equals(_parseSpec, StringComparison.CurrentCultureIgnoreCase))
                {
                    return Utility.FromEpochTime((long)token);
                }
            }

            //Format it first and then parse
            string formatted = string.Format(_formatSpec, values);
            return Utility.ParseDatetime(formatted, _parseSpec);
        }
    }
}
