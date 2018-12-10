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
using Microsoft.Extensions.Configuration;

namespace Amazon.KinesisTap.DiagnosticTool.Core
{
    /// <summary>
    /// The class for DirectorySource validator
    /// </summary>
    public class DirectorySourceValidator : ISourceValidator
    {
        // The list of record parsers for Directory source
        private readonly HashSet<String> recordParsers = new HashSet<String>() { "timestamp", "singleline", "regex", "syslog", "delimited", "singlelinejson" };

        /// <summary>
        /// Validate the source section
        /// </summary>
        /// <param name="sourceSection"></param>
        /// <param name="id"></param>
        /// <param name="messages"></param>
        /// <returns></returns>
        public bool ValidateSource(IConfigurationSection sourceSection, string id, IList<string> messages)
        {
            string recordParser = sourceSection["RecordParser"].ToLower();
            if (!recordParsers.Contains(recordParser))
            {
                messages.Add($"RecordParser {recordParser} is not valid in source ID: {id}.");
                return false;
            }

            if (recordParser.Equals("timestamp"))
            {
                // Required only if "RecordParser" = "Timestamp"
                string timestampFormat = sourceSection["TimestampFormat"];
                if (string.IsNullOrEmpty(timestampFormat))
                {
                    messages.Add($"Attribute 'TimestampFormat' is required in source ID: {id}.");
                    return false;
                }
            }
            else if (recordParser.Equals("regex"))
            {
                // Required only if "RecordParser" = "Regex"
                string pattern = sourceSection["Pattern"];
                if (string.IsNullOrEmpty(pattern))
                {
                    messages.Add($"Attribute 'Pattern' is required in source ID: {id}.");
                    return false;
                }

                // Required only if the field "Pattern" has a named group "TimeStamp"
                if (pattern.ToLower().Contains("timestamp"))
                {
                    if (string.IsNullOrEmpty(sourceSection["TimestampFormat"]))
                    {
                        messages.Add($"Attribute 'TimestampFormat' is required in source ID: {id} because the 'Pattern' has a TimeStamp named group.");
                        return false;
                    }
                }
            }
            else if (recordParser.Equals("delimited"))
            {
                string delimiter = sourceSection["Delimiter"];
                string timestampField = sourceSection["TimestampField"];
                string timestampFormat = sourceSection["TimestampFormat"];

                if (string.IsNullOrEmpty(delimiter))
                {
                    messages.Add($"Attribute 'Delimiter' is required in source ID: {id}.");
                    return false;
                }

                if (string.IsNullOrEmpty(timestampField))
                {
                    messages.Add($"Attribute 'TimestampField' is required in source ID: {id}.");
                    return false;
                }

                if (string.IsNullOrEmpty(timestampFormat))
                {
                    messages.Add($"Attribute 'TimestampFormat' is required in source ID: {id}.");
                    return false;
                }
            }

            return true;
        }
    }
}
