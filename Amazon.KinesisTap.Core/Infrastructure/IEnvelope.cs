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
using System.Text;

namespace Amazon.KinesisTap.Core
{
    //Wrap around the underlying data to provide additional meta data
    public interface IEnvelope
    {
        DateTime Timestamp { get; }

        string GetMessage(string format);

        /// <summary>
        /// Resolve local variable. Local variable are defined as the variable only depends on the envelope. 
        /// They don't depend on the environment (environment variables, ec2 meta data)
        /// </summary>
        /// <param name="variable">Name of the variable to resolve. Assume {} are already stripped off and starts with $.</param>
        /// <returns>Return value from evaludating the variable.</returns>
        object ResolveLocalVariable(string variable);

        /// <summary>
        /// Resolve meta variable, such _timestamp, _record
        /// </summary>
        /// <param name="variable"></param>
        /// <returns></returns>
        object ResolveMetaVariable(string variable);
    }

    public interface IEnvelope<out TData> : IEnvelope
    {
        TData Data { get; }
    }
}
