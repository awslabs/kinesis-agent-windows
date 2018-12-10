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
using System.IO;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public class FilePersistentQueue<T> : ISimpleQueue<T>
    {
        private int _head;
        private int _tail;
        private readonly int _capacity;
        private readonly string _directory;
        ISerializer<T> _serializer;

        const int MAX_CAPACITY = 1000000000; 

        public FilePersistentQueue(int capacity, 
            string directory, 
            ISerializer<T> serializer)
        {
            _capacity = capacity;
            if (_capacity > MAX_CAPACITY)
                throw new ArgumentException($"The maximum capacity is {MAX_CAPACITY}");

            _directory = directory;
            EnsureDirectory();
            _serializer = serializer;
            string indexFilepath = GetIndexFile();
            if (File.Exists(indexFilepath))
            {
                LoadIndex(indexFilepath);
            }
            else
            {
                _head = 0;
                _tail = 0;
            }
        }

#region public methods
        public int Count => _tail - _head;

        public int Capacity => _capacity;

        public string QueueDirectory => _directory;

        public T Dequeue()
        {
            if (Count == 0)
            {
                throw new InvalidOperationException("Queue empty.");
            }

            try
            {
                T item;
                string filepath = GetFilePath(_head);
                using (var instream = File.OpenRead(filepath))
                {
                    item = _serializer.Deserialize(instream);
                }
                File.Delete(filepath);
                return item;
            }
            finally
            {
                //If failed, don't get stuck
                _head++;
                UpdateIndex();
            }
        }

        public void Enqueue(T item)
        {
            if (Count >= _capacity)
            {
                throw new InvalidOperationException("Exceed capacity.");
            }

            using (var outstream = File.OpenWrite(GetFilePath(_tail)))
            {
                _serializer.Serialize(outstream, item);
            }
            _tail++;
            UpdateIndex();
        }
#endregion

        private string GetFilePath(int index)
        {
            return Path.Combine(_directory, index.ToString().PadLeft(9, '0'));
        }

        private void EnsureDirectory()
        {
            if (!Directory.Exists(_directory))
            {
                Directory.CreateDirectory(_directory);
            }
        }

        private string GetIndexFile()
        {
            return Path.Combine(_directory, "Index");
        }

        private void LoadIndex(string indexFilepath)
        {
            string line = File.ReadAllText(indexFilepath);
            string[] idxs = line.Split(' ');
            _head = int.Parse(idxs[0]);
            _tail = int.Parse(idxs[1]);
        }

        private void UpdateIndex()
        {
            using (var outstream = File.OpenWrite(GetIndexFile()))
            using (var sw = new StreamWriter(outstream))
            {
                sw.Write($"{_head} {_tail}");
            }
        }
    }
}
