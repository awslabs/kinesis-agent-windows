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
using System.Collections.Generic;

namespace Amazon.KinesisTap.Core.Test
{
    /// <summary>
    /// A mock parameter store based on Dictionary
    /// </summary>
    public class DictionaryParameterStore : IParameterStore
    {
        private readonly Dictionary<string, string> _store = new Dictionary<string, string>();

        /// <summary>
        /// Get the parameter by name
        /// </summary>
        /// <param name="name">The name of the parameter to get</param>
        /// <returns>Retrieved parameter, or null if it does not exist.</returns>
        public string GetParameter(string name)
        {
            if (_store.TryGetValue(name, out string value))
            {
                return value;
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Set parameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="value">Parameter value</param>
        public void SetParameter(string name, string value)
        {
            _store[name] = value;
        }
    }
}
