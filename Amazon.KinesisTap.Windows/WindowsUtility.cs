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

namespace Amazon.KinesisTap.Windows
{
    public class WindowsUtility
    {
        /// <summary>
        /// Provide a search order on how we are going to resolve environment variables on Windows. Mac and Linux only have Process variables.
        /// </summary>
        /// <param name="variable">The name of the environment variable</param>
        /// <returns></returns>
        public static string ResolveEnvironmentVariable(string variable)
        {
            return Environment.GetEnvironmentVariable(variable, EnvironmentVariableTarget.Machine)
                ?? Environment.GetEnvironmentVariable(variable, EnvironmentVariableTarget.User)
                ?? Environment.GetEnvironmentVariable(variable, EnvironmentVariableTarget.Process);
        }
    }
}
