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
namespace Amazon.KinesisTap.Filesystem
{
    /// <summary>
    /// Options for parsing generic delimited logs.
    /// </summary>
    public class GenericDelimitedLogParserOptions : DelimitedLogParserOptions
    {
        /// <summary>
        /// Timestamp format. Skip if format should be detected automatically.
        /// </summary>
        public string TimestampFormat { get; set; }

        /// <summary>
        /// Field(s) that contain timestamp value. Should be none if records do not contain timestamps.
        /// </summary>
        public string TimestampField { get; set; }

        /// <summary>
        /// Pattern for recognizing 'headers' line.
        /// </summary>
        public string HeadersPattern { get; set; }

        /// <summary>
        /// Pattern for recognizing record lines.
        /// </summary>
        public string RecordPattern { get; set; }

        /// <summary>
        /// Pattern for recognizing comment lines.
        /// </summary>
        public string CommentPattern { get; set; }

        /// <summary>
        /// Specify explicit headers.
        /// </summary>
        public string Headers { get; set; }

        /// <summary>
        /// Use CSV-style character escape. True by default.
        /// </summary>
        public bool CSVEscapeMode { get; set; } = true;
    }
}
