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
    /// <summary>
    /// Help clients to keep track when a log was last written and whether should write the log
    /// </summary>
    public static class LogThrottler
    {
        //Key: logTypeId, value: LastWritten
        private static IDictionary<int, DateTime> _logTypes = new Dictionary<int, DateTime>();

        /// <summary>
        /// Tell a client whether it should write the log
        /// </summary>
        /// <param name="logTypeId">Identify the log type. Can be a hash and may include key parameters in hash.</param>
        /// <param name="minimumDelayBetweenWrite">Minimum delay between write</param>
        /// <returns></returns>
        public static bool ShouldWrite(int logTypeId, TimeSpan minimumDelayBetweenWrite)
        {
            lock(_logTypes)
            {
                DateTime now = DateTime.Now;
                if (_logTypes.TryGetValue(logTypeId, out DateTime lastWrite) && lastWrite + minimumDelayBetweenWrite > now )
                {
                    return false;
                }
                else
                {
                    _logTypes[logTypeId] = now;
                    return true;
                }
            }
        }

        /// <summary>
        /// Generate a unique LogTypeId from class name, method name, block name and some key arguments, if needed
        /// </summary>
        /// <param name="className">Class name</param>
        /// <param name="methodName">Method name</param>
        /// <param name="blockName">A name of developer's choice to identify the code block, if needed</param>
        /// <param name="keyArguments">Can include some arguments if needed</param>
        /// <returns></returns>
        public static int CreateLogTypeId(string className, string methodName, string blockName, params object[] keyArguments)
        {
            StringBuilder keyStringBuilder = new StringBuilder()
                .AppendFormat("{0}.{1}.{2}", className, methodName, blockName);

            foreach(object keyArgument in keyArguments)
            {
                keyStringBuilder.AppendFormat(".{0}", keyArgument);
            }
            return keyStringBuilder.ToString().GetHashCode();
        }
    }
}
