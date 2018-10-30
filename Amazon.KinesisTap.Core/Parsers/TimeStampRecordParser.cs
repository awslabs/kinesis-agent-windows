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
using System.Text;
using System.Text.RegularExpressions;

namespace Amazon.KinesisTap.Core
{
    public class TimeStampRecordParser : RegexRecordParser
    {
        protected string _timeStamp;

        public TimeStampRecordParser(string timeStamp, ILogger logger, DateTimeKind timeZoneKind) :
            this(timeStamp, logger, timeZoneKind, new RegexRecordParserOptions())
        {

        }

        public TimeStampRecordParser(string timeStamp, ILogger logger, DateTimeKind timeZoneKind, RegexRecordParserOptions parserOptions) : 
            base(ConvertTimeStampToRegex(timeStamp), timeStamp, logger, null, timeZoneKind, parserOptions)
        {
            _timeStamp = timeStamp; //e.g.: "MM/dd/yyyy HH:mm:ss"
        }

        private static string ConvertTimeStampToRegex(string timeStamp)
        {
            char[] timeStampCharacters = new[] { 'd', 'M', 'm', 'y', 'H', 'h', 's', 'f' };
            string regex = timeStamp;
            foreach(char c in timeStampCharacters)
            {
                regex = regex.Replace(c.ToString(), @"\d");
            }
            return $"^(?<TimeStamp>{regex})";
        }
    }
}
