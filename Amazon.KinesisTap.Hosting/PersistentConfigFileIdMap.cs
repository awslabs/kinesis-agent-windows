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
using Amazon.KinesisTap.Core;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Amazon.KinesisTap.Hosting
{
    /// <summary>
    /// Maintains a storage-backed mapping between a configuration file path and its config ID.
    /// This class uses <see cref="IParameterStore"/> to save and retrieve the mapping as a json object.
    /// This class is NOT thread-safe.
    /// </summary>
    public class PersistentConfigFileIdMap : IDictionary<string, int>
    {
        private readonly Dictionary<string, int> _memoryMap;
        private readonly IParameterStore _store;

        /// <summary>
        /// The maximum ID that has been saved in the map
        /// </summary>
        public int MaxUsedId { get; private set; } = 0;

        /// <summary>
        /// Create a new PersistentConfigFileIdMap
        /// </summary>
        /// <param name="filePath">Path to the file that backs the map</param>
        public PersistentConfigFileIdMap(IParameterStore store)
        {
            _store = store;
            _memoryMap = LoadMapping();
        }

        public int this[string key]
        {
            get => _memoryMap[key];
            set
            {
                ValidateKeyValue(key, value);
                MemSet(key, value);
                SaveMapping();
            }
        }

        public ICollection<string> Keys => _memoryMap.Keys;

        public ICollection<int> Values => _memoryMap.Values;

        public int Count => _memoryMap.Count;

        public bool IsReadOnly => false;

        public void Add(string key, int value)
        {
            ValidateKeyValue(key, value);

            MemSet(key, value);
            SaveMapping();
        }

        public void Add(KeyValuePair<string, int> item) => Add(item.Key, item.Value);

        public void Clear()
        {
            _memoryMap.Clear();
            MaxUsedId = 0;
            SaveMapping();
        }

        public bool Contains(KeyValuePair<string, int> item) => (_memoryMap as IDictionary<string, int>).Contains(item);

        public bool ContainsKey(string key) => _memoryMap.ContainsKey(key);

        public void CopyTo(KeyValuePair<string, int>[] array, int arrayIndex)
            => (_memoryMap as IDictionary<string, int>).CopyTo(array, arrayIndex);

        public IEnumerator<KeyValuePair<string, int>> GetEnumerator() => _memoryMap.GetEnumerator();

        public bool Remove(string key)
        {
            var didRemove = _memoryMap.Remove(key);
            if (didRemove)
            {
                SaveMapping();
            }
            return didRemove;
        }

        public bool Remove(KeyValuePair<string, int> item)
        {
            var didRemove = (_memoryMap as IDictionary<string, int>).Remove(item);
            if (didRemove)
            {
                SaveMapping();
            }
            return didRemove;
        }

        public bool TryGetValue(string key, out int value) => _memoryMap.TryGetValue(key, out value);

        IEnumerator IEnumerable.GetEnumerator() => _memoryMap.GetEnumerator();

        private void MemSet(string key, int value)
        {
            _memoryMap[key] = value;

            //Assignment of new id is performed in an self-incrementing way
            //when reloading, `holes` in an id-range should also be avoided
            MaxUsedId = Math.Max(MaxUsedId, value);
        }

        private void ValidateKeyValue(string key, int value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key), "Key must not be null");
            }

            if (value <= 0)
            {
                throw new ArgumentOutOfRangeException("ID", $"ID must be greater than 0");
            }
        }

        private void SaveMapping()
        {
            var mapAsJson = JsonConvert.SerializeObject(_memoryMap);
            _store.SetParameter(HostingUtility.PersistentConfigFileIdMapStoreKey, mapAsJson);
        }

        private Dictionary<string, int> LoadMapping()
        {
            var mapAsJson = _store.GetParameter(HostingUtility.PersistentConfigFileIdMapStoreKey);
            if (mapAsJson == null)
            {
                return new Dictionary<string, int>();
            }

            return JsonConvert.DeserializeObject<Dictionary<string, int>>(mapAsJson);
        }
    }
}
