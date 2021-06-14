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
using System.Text;

namespace Amazon.KinesisTap.Filesystem
{
    /// <summary>
    /// A set of options for parsing delimited logs
    /// </summary>
    public class DelimitedLogParserOptions
    {
        /// <summary>
        /// User-specified log file encoding.
        /// </summary>
        public Encoding TextEncoding { get; set; }

        /// <summary>
        /// Buffer size used to read data from log files. Default to 1024.
        /// </summary>
        public int BufferSize { get; set; } = 1024;

        /// <summary>
        /// Whether to trim the parsed data fields.
        /// </summary>
        public bool TrimDataFields { get; set; }
    }
}
