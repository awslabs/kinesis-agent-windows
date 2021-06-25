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
using System.Collections;
using System.Collections.Generic;

namespace Amazon.KinesisTap.Filesystem
{
    /// <summary>
    /// Represent log record as a collection of key-value string mappings.
    /// </summary>
    public class KeyValueLogRecord : IReadOnlyDictionary<string, string>
    {
        protected readonly IReadOnlyDictionary<string, string> _data;

        public KeyValueLogRecord(DateTime timestamp, IReadOnlyDictionary<string, string> data)
        {
            Timestamp = timestamp;
            _data = data;
        }

        /// <summary>
        /// Timestamp at which this record is generated.
        /// </summary>
        public DateTime Timestamp { get; }

        #region IReadOnlyDictionary
        public string this[string key] => _data[key];

        public IEnumerable<string> Keys => _data.Keys;

        public IEnumerable<string> Values => _data.Values;

        public int Count => _data.Count;

        public bool ContainsKey(string key) => _data.ContainsKey(key);

        public IEnumerator<KeyValuePair<string, string>> GetEnumerator() => _data.GetEnumerator();

        public bool TryGetValue(string key, out string value) => _data.TryGetValue(key, out value);

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        #endregion
    }
}
