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
using System.Text;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Filesystem
{
    public class TimestampLogParser : RegexLogParser
    {
        private static readonly char[] _timeStampCharacters = new[] { 'd', 'M', 'm', 'y', 'H', 'h', 's', 'f' };
        public TimestampLogParser(ILogger logger,
            RegexParserOptions options,
            Encoding encoding,
            int bufferSize)
            : base(logger, ConvertTimeStampToRegex(options.TimestampFormat), options, encoding, bufferSize)
        {
        }

        private static string ConvertTimeStampToRegex(string timestampFormat)
        {
            if (timestampFormat is null)
            {
                throw new ArgumentNullException("TimestampFormat");
            }

            var regex = timestampFormat;
            foreach (var c in _timeStampCharacters)
            {
                regex = regex.Replace(c.ToString(), @"\d");
            }
            return $"^(?<Timestamp>{regex})";
        }
    }
}
