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
using System.IO;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// A file-based implementation of the <see cref="ISimpleQueue{T}"/> interface.
    /// This class is built on files persisted in a "Queue" directory, with "Index" files that contain the current positions in the queue.
    /// </summary>
    /// <typeparam name="T">The type of item stored in the queue.</typeparam>
    public class FilePersistentQueue<T> : ISimpleQueue<T>
    {
        private readonly ISerializer<T> _serializer;
        private readonly IAppDataFileProvider _fileProvider;
        private readonly ILogger _logger;
        private readonly string _indexFilePath;
        private readonly object _fileLock = new object();

        const int MAX_CAPACITY = 1000000000;

        public FilePersistentQueue(int capacity, string directory,
            ISerializer<T> serializer,
            IAppDataFileProvider fileProvider,
            ILogger logger)
        {
            _logger = logger;
            _serializer = serializer;
            _fileProvider = fileProvider;
            Capacity = capacity;
            if (Capacity > MAX_CAPACITY)
            {
                throw new ArgumentException($"The maximum capacity is {MAX_CAPACITY}");
            }

            QueueDirectory = directory;
            fileProvider.CreateDirectory(QueueDirectory);

            _indexFilePath = Path.Combine(QueueDirectory, "Index");
            if (!LoadIndex())
            {
                DiscoverIndex();
            }
        }

        /// <summary>
        /// Gets the index of the first record in the queue.
        /// </summary>
        public int Head { get; internal set; }

        /// <summary>
        /// Gets the index of the last record in the queue.
        /// </summary>
        public int Tail { get; internal set; }

        /// <summary>
        /// Gets the directory containing the queued items. This is relative to the AppData directory.
        /// </summary>
        public string QueueDirectory { get; }

        /// <inheritdoc />
        public int Count
        {
            get
            {
                lock (_fileLock)
                {
                    return CountInternal;
                }
            }
        }

        private int CountInternal => Tail - Head;

        /// <inheritdoc />
        public int Capacity { get; }

        public void Dispose()
        {
        }

        /// <inheritdoc />
        public bool TryDequeue(out T item)
        {
            lock (_fileLock)
            {
                if (CountInternal > 0)
                {
                    var filepath = GetFilePath(Head);
                    try
                    {
                        // Open the file using the "DeleteOnClose" flag so that it is deleted after being read.
                        // This eliminates the need to delete the file in a separate operation.
                        using var fs = _fileProvider.OpenFile(filepath, FileMode.Open, FileAccess.Read, FileShare.None, 4096, FileOptions.DeleteOnClose);
                        item = _serializer.Deserialize(fs);

                        _logger?.LogTrace("[{0}] Successfully dequeued item from persistent queue. {1} items left in queue.", nameof(FilePersistentQueue<T>.TryDequeue), CountInternal);
                        return true;
                    }
                    catch (Exception ex)
                    {
                        // If this fails, we don't want it to get stuck.
                        // We'll log the error, with the bad file name, and increment the head counter in the finally.
                        _logger?.LogError("Error dequeuing object in file {0}: {1}", filepath, ex.ToMinimized());
                    }
                    finally
                    {
                        Head++;
                        UpdateIndex();
                    }
                }

                _logger?.LogDebug("[{0}] No records left in persistent queue.", nameof(FilePersistentQueue<T>.TryDequeue));

                item = default;
                return false;
            }
        }

        /// <inheritdoc />
        public bool TryEnqueue(T item)
        {
            lock (_fileLock)
            {
                if (CountInternal >= Capacity)
                {
                    _logger?.LogDebug("[{0}] Persistent queue full, cannot enqueue new item.", nameof(FilePersistentQueue<T>.TryEnqueue));
                    return false;
                }

                var path = GetFilePath(Tail);
                try
                {
                    using var outstream = _fileProvider.OpenFile(path, FileMode.Create, FileAccess.Write, FileShare.None);
                    _serializer.Serialize(outstream, item);
                }
                catch (Exception ex)
                {
                    _logger?.LogError("Failed to add new item to FilePersistentQueue: {0}", ex.ToMinimized());
                    if (_fileProvider.FileExists(path))
                    {
                        _fileProvider.DeleteFile(path);
                    }
                    return false;
                }

                Tail++;
                UpdateIndex();

                _logger?.LogTrace("[{0}] Successfully enqueued new item in persistent queue, {1} items now in queue.", nameof(FilePersistentQueue<T>.TryEnqueue), CountInternal);
                return true;
            }
        }

        internal void DiscoverIndex()
        {
            // Enumerate the directory for all files to get a list of file names.
            // The file names contain the incrementing index value, so we'll parse the
            // name portion into an int and sort the array based on that value.
            var files = _fileProvider.GetFilesInDirectory(QueueDirectory)
                .Select(f => new { Path = f, Index = int.TryParse(Path.GetFileName(f), out var ind) ? ind : -2 })
                .Where(i => i.Index > -1) // This is faster than >= 0.
                .OrderByDescending(i => i.Index)
                .ToList();

            // If there are no files in the persistent queue, return.
            if (files.Count == 0) return;

            // Temporarily use the last item in the list as the head. We'll verify this later.
            Head = files.First().Index;

            // The first element in the array will be the last item written, so that will be our tail.
            // We need to increment the value by 1 so that TryEnqueue doesn't overwrite the last record.
            Tail = Head + 1;

            // If head and tail are the same, return.
            if (files.Count == 1) return;

            // To find the actual head, we need to step back through each file to find the last item in a consecutive sequence.
            // Since we're not deleting bad files, there may be some orphaned files in the queue, so we need to make sure
            // that we don't use one of those as the head. As soon as we detect that the next file in the list isn't in
            // a consecutive sequence from the tail, return. Skip the first entry since it's the current head value.
            foreach (var f in files.Skip(1))
            {
                if (f.Index == Head - 1)
                    Head = f.Index;
                else
                    break;
            }

            _logger?.LogInformation("Index file at path '{0}' was rebuilt. Using 'Head' position '{1}' and 'Tail' position '{2}'.", _indexFilePath, Head, Tail);

            // Save the discovered index values to the file.
            UpdateIndex();
        }

        // Returns true if index was loaded, false if it needs to be recalculated.
        internal bool LoadIndex()
        {
            // If the index file doesn't exist, use default head/tail values of 0.
            if (!_fileProvider.FileExists(_indexFilePath))
            {
                _logger?.LogDebug("Index file '{0}' does not exist. Head and Tail positions will start at 0.", _indexFilePath);
                return true;
            }

            var line = _fileProvider.ReadAllText(_indexFilePath);

            // If the index file is empty or just whitespace, use default head/tail values of 0.
            if (string.IsNullOrWhiteSpace(line))
            {
                _logger?.LogWarning("Error parsing Index file at path '{0}'. File exists, but was empty.", _indexFilePath);
                return false;
            }

            var idxs = line.Split(' ');

            // If the index file doesn't have exactly 2 elements, use default head/tail values of 0.
            if (idxs.Length != 2)
            {
                _logger?.LogWarning("Error parsing Index file at path '{0}'. File exists and was not empty, but did not have exactly 2 values.", _indexFilePath);
                return false;
            }

            // Attempt to parse the int values. If they are not ints, use default head/tail values of 0.
            if (int.TryParse(idxs[0], out int indexHead) && int.TryParse(idxs[1], out int indexTail))
            {
                _logger?.LogDebug("Index file at path '{0}' contains 'Head' position '{1}' and 'Tail' position '{2}'", _indexFilePath, indexHead, indexTail);
                Head = indexHead;
                Tail = indexTail;
                return true;
            }
            else
            {
                _logger?.LogWarning("Error parsing Index file at path '{0}'. File exists and was not empty, but the positions were not a valid int values. File contents: {1}", _indexFilePath, line);
                return false;
            }
        }

        private string GetFilePath(int index)
        {
            return Path.Combine(QueueDirectory, index.ToString().PadLeft(9, '0'));
        }

        private void UpdateIndex()
        {
            var contents = $"{Head} {Tail}";

            // Obtain a lock to ensure that only one process can write to the index file at a time.
            try
            {
                using var outstream = _fileProvider.OpenFile(_indexFilePath, FileMode.Create, FileAccess.Write, FileShare.None);
                using var sw = new StreamWriter(outstream);

                sw.Write(contents);
            }
            catch (Exception ex)
            {
                _logger?.LogWarning("Failed to update Index file at path '{0}' with contents '{1}': {2}", _indexFilePath, contents, ex.ToMinimized());
                throw;
            }
        }
    }
}
