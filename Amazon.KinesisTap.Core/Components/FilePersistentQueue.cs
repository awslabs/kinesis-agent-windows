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
    using System.Linq;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// A file-based implementation of the <see cref="ISimpleQueue{T}"/> interface.
    /// This class is built on files persisted in a "Queue" directory, with "Index" files that contain the current positions in the queue.
    /// </summary>
    /// <typeparam name="T">The type of item stored in the queue.</typeparam>
    public class FilePersistentQueue<T> : ISimpleQueue<T>
    {
        private readonly ISerializer<T> serializer;
        private readonly ILogger logger;
        private readonly string indexFilePath;
        private readonly object fileLock = new object();

        const int MAX_CAPACITY = 1000000000;

        public FilePersistentQueue(int capacity, string directory, ISerializer<T> serializer, ILogger logger = null)
        {
            this.logger = logger;
            this.serializer = serializer;
            this.Capacity = capacity;
            if (this.Capacity > MAX_CAPACITY)
                throw new ArgumentException($"The maximum capacity is {MAX_CAPACITY}");

            this.QueueDirectory = directory;
            if (!Directory.Exists(this.QueueDirectory))
                Directory.CreateDirectory(this.QueueDirectory);

            this.indexFilePath = Path.Combine(this.QueueDirectory, "Index");
            if (!this.LoadIndex())
                this.DiscoverIndex();
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
        /// Gets the directory containing the queued items.
        /// </summary>
        public string QueueDirectory { get; }

        /// <inheritdoc />
        public int Count => this.Tail - this.Head;

        /// <inheritdoc />
        public int Capacity { get; }

        /// <inheritdoc />
        public bool TryDequeue(out T item)
        {
            if (this.Count > 0)
            {
                var filepath = this.GetFilePath(this.Head);
                try
                {
                    // Open the file using the "DeleteOnClose" flag so that it is deleted after being read.
                    // This eliminates the need to delete the file in a separate operation.
                    using (var fs = new FileStream(filepath, FileMode.Open, FileAccess.Read, FileShare.None, 4096, FileOptions.DeleteOnClose))
                        item = this.serializer.Deserialize(fs);

                    this.logger?.LogTrace("[{0}] Successfully dequeued item from persistent queue. {1} items left in queue.", nameof(FilePersistentQueue<T>.TryDequeue), this.Count);
                    return true;
                }
                catch (Exception ex)
                {
                    // If this fails, we don't want it to get stuck.
                    // We'll log the error, with the bad file name, and increment the head counter in the finally.
                    this.logger?.LogError("Error dequeuing object in file {0}: {1}", filepath, ex.ToMinimized());
                }
                finally
                {
                    this.Head++;
                    this.UpdateIndex();
                }
            }

            this.logger?.LogDebug("[{0}] No records left in persistent queue.", nameof(FilePersistentQueue<T>.TryDequeue));

            item = default(T);
            return false;
        }

        /// <inheritdoc />
        public bool TryEnqueue(T item)
        {
            if (this.Count >= this.Capacity)
            {
                this.logger?.LogDebug("[{0}] Persistent queue full, cannot enqueue new item.", nameof(FilePersistentQueue<T>.TryEnqueue));
                return false;
            }

            var path = this.GetFilePath(Tail);
            try
            {
                using (var outstream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None))
                    this.serializer.Serialize(outstream, item);
            }
            catch (Exception ex)
            {
                this.logger?.LogError("Failed to add new item to FilePersistentQueue: {0}", ex.ToMinimized());
                if (File.Exists(path)) File.Delete(path);
                return false;
            }

            this.Tail++;
            this.UpdateIndex();

            this.logger?.LogTrace("[{0}] Successfully enqueued new item in persistent queue, {1} items now in queue.", nameof(FilePersistentQueue<T>.TryEnqueue), this.Count);
            return true;
        }

        internal void DiscoverIndex()
        {
            // Enumerate the directory for all files to get a list of file names.
            // The file names contain the incrementing index value, so we'll parse the
            // name portion into an int and sort the array based on that value.
            var files = Directory.GetFiles(this.QueueDirectory)
                .Select(i => new { Path = i, Index = int.TryParse(i.Split(Path.DirectorySeparatorChar).Last(), out int ind) ? ind : -2 })
                .Where(i => i.Index > -1) // This is faster than >= 0.
                .OrderByDescending(i => i.Index)
                .ToList();

            // If there are no files in the persistent queue, return.
            if (files.Count == 0) return;

            // Temporarily use the last item in the list as the head. We'll verify this later.
            this.Head = files.First().Index;

            // The first element in the array will be the last item written, so that will be our tail.
            // We need to increment the value by 1 so that TryEnqueue doesn't overwrite the last record.
            this.Tail = this.Head + 1;

            // If head and tail are the same, return.
            if (files.Count == 1) return;

            // To find the actual head, we need to step back through each file to find the last item in a consecutive sequence.
            // Since we're not deleting bad files, there may be some orphaned files in the queue, so we need to make sure
            // that we don't use one of those as the head. As soon as we detect that the next file in the list isn't in
            // a consecutive sequence from the tail, return. Skip the first entry since it's the current head value.
            foreach (var f in files.Skip(1))
            {
                if (f.Index == this.Head - 1)
                    this.Head = f.Index;
                else
                    break;
            }

            this.logger?.LogInformation("Index file at path '{0}' was rebuilt. Using 'Head' position '{1}' and 'Tail' position '{2}'.", this.indexFilePath, this.Head, this.Tail);

            // Save the discovered index values to the file.
            this.UpdateIndex();
        }

        // Returns true if index was loaded, false if it needs to be recalculated.
        internal bool LoadIndex()
        {
            // If the index file doesn't exist, use default head/tail values of 0.
            if (!File.Exists(this.indexFilePath))
            {
                this.logger?.LogDebug("Index file '{0}' does not exist. Head and Tail positions will start at 0.", this.indexFilePath);
                return true;
            }

            var line = File.ReadAllText(this.indexFilePath);

            // If the index file is empty or just whitespace, use default head/tail values of 0.
            if (string.IsNullOrWhiteSpace(line))
            {
                this.logger?.LogWarning("Error parsing Index file at path '{0}'. File exists, but was empty.", this.indexFilePath);
                return false;
            }

            var idxs = line.Split(' ');

            // If the index file doesn't have exactly 2 elements, use default head/tail values of 0.
            if (idxs.Length != 2)
            {
                this.logger?.LogWarning("Error parsing Index file at path '{0}'. File exists and was not empty, but did not have exactly 2 values.", this.indexFilePath);
                return false;
            }

            // Attempt to parse the int values. If they are not ints, use default head/tail values of 0.
            if (int.TryParse(idxs[0], out int indexHead) && int.TryParse(idxs[1], out int indexTail))
            {
                this.logger?.LogDebug("Index file at path '{0}' contains 'Head' position '{1}' and 'Tail' position '{2}'", this.indexFilePath, indexHead, indexTail);
                this.Head = indexHead;
                this.Tail = indexTail;
                return true;
            }
            else
            {
                this.logger?.LogWarning("Error parsing Index file at path '{0}'. File exists and was not empty, but the positions were not a valid int values. File contents: {1}", this.indexFilePath, line);
                return false;
            }
        }

        private string GetFilePath(int index)
        {
            return Path.Combine(this.QueueDirectory, index.ToString().PadLeft(9, '0'));
        }

        private void UpdateIndex()
        {
            var contents = $"{this.Head} {this.Tail}";

            // Obtain a lock to ensure that only one process can write to the index file at a time.
            lock (this.fileLock)
            {
                try
                {
                    using (var outstream = new FileStream(this.indexFilePath, FileMode.Create, FileAccess.Write, FileShare.None))
                    using (var sw = new StreamWriter(outstream))
                    {
                        sw.Write(contents);
                    }
                }
                catch (Exception ex)
                {
                    this.logger?.LogWarning("Failed to update Index file at path '{0}' with contents '{1}': {2}", this.indexFilePath, contents, ex.ToMinimized());
                    throw;
                }
            }
        }
    }
}
