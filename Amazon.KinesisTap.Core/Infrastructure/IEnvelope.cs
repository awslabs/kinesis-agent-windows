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
namespace Amazon.KinesisTap.Core
{
    using System;
    using System.IO;

    //Wrap around the underlying data to provide additional meta data
    public interface IEnvelope
    {
        DateTime Timestamp { get; }

        string GetMessage(string format);

        /// <summary>
        /// Serializes the data into a new <see cref="MemoryStream"/>.
        /// </summary>
        /// <param name="format">The format to use when writing.</param>
        MemoryStream GetMessageStream(string format);

        /// <summary>
        /// Writes the data into an existing <see cref="MemoryStream"/>.
        /// </summary>
        /// <param name="format">The format to use when writing.</param>
        /// <param name="memoryStream">The stream to write to.</param>
        void WriteMessageToStream(string format, MemoryStream memoryStream);

        /// <summary>
        /// Resolve local variable. Local variable are defined as the variable only depends on the envelope. 
        /// They don't depend on the environment (environment variables, ec2 meta data)
        /// </summary>
        /// <param name="variable">Name of the variable to resolve. Assume {} are already stripped off and starts with $.</param>
        /// <returns>Return value from evaluating the variable.</returns>
        object ResolveLocalVariable(string variable);

        /// <summary>
        /// Resolve meta variable, such _timestamp, _record
        /// </summary>
        /// <param name="variable"></param>
        /// <returns></returns>
        object ResolveMetaVariable(string variable);

        /// <summary>
        /// Gets or sets the bookmark position of the record.
        /// </summary>
        long Position { get; set; }

        /// <summary>
        /// Gets or sets the Id of the bookmark object registered in the <see cref="BookmarkManager"/>.
        /// </summary>
        int? BookmarkId { get; set; }
    }

    public interface IEnvelope<out TData> : IEnvelope
    {
        TData Data { get; }
    }
}
