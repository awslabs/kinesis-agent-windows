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
using System.Linq;

namespace Amazon.KinesisTap.Core
{
    public abstract class DelimitedLogRecordBase : IReadOnlyDictionary<string, string>
    {
        protected string[] _data;
        protected DelimitedLogContext _context;
        protected DateTimeKind _timeZoneKind;

        protected DelimitedLogRecordBase(string[] data, DelimitedLogContext context)
        {
            if (context?.Mapping is null)
            {
                throw new ArgumentNullException($"Field mapping for this record is not determined. This might be due to field mapping line not present in the log. " +
                    $"Consider specifying '{ConfigConstants.DEFAULT_FIELD_MAPPING}' in the source configuration");
            }
            _data = data;
            _context = context;
        }

        public abstract DateTime TimeStamp { get; }

        #region IReadOnlyDictionary
        public string this[string key] => _data[_context.Mapping[key]];


        public IEnumerable<string> Keys => _context.Mapping.Keys;

        public IEnumerable<string> Values => _data;

        public int Count => _context.Mapping.Count;

        public bool ContainsKey(string key)
        {
            return _context.Mapping.ContainsKey(key);
        }

        public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
        {
            return _context.Mapping.Keys.Select(k => new KeyValuePair<string, string>(k, this[k])).GetEnumerator();
        }

        public bool TryGetValue(string key, out string value)
        {
            if (_context.Mapping.TryGetValue(key, out int index))
            {
                value = _data[index];
                return true;
            }
            else
            {
                value = null;
                return false;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
        #endregion
    }
}
