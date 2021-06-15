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
using System.Globalization;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Represents extracted data of a Syslog record.
    /// </summary>
    public class SyslogData
    {
        public SyslogData(DateTimeOffset timestamp, string hostname, string program, string message)
        {
            Timestamp = timestamp;
            Hostname = hostname;
            Program = program;
            Message = message;
        }

        /// <summary>
        /// Timestamp in syslog format.
        /// </summary>
        public string SyslogTimestamp => Timestamp.ToString("MMM dd HH:mm:ss", CultureInfo.InvariantCulture);

        /// <summary>
        /// The 'Hostname' part of the syslog record.
        /// </summary>
        public string Hostname { get; }

        /// <summary>
        /// The 'Program' part of the syslog record.
        /// </summary>
        public string Program { get; }

        /// <summary>
        /// The 'Message' part of the syslog record.
        /// </summary>
        public string Message { get; }

        /// <summary>
        /// Timestamp when this record is generated.
        /// </summary>
        public DateTimeOffset Timestamp { get; }
    }
}
