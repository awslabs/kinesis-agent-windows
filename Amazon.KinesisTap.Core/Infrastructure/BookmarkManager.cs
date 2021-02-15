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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;
    using Microsoft.Extensions.Logging;

    public class BookmarkManager
    {
        // Start from 1, so anything that tries to save a bookmark with id '0' does nothing.
        private int nextBookmarkInfoIdCounter = 1;

        // Controls concurrent requests to create new bookmarks.
        private readonly SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);

        // Stores the bookmark objects.
        private readonly ConcurrentDictionary<int, BookmarkInfo> bookmarks = new ConcurrentDictionary<int, BookmarkInfo>();

        // Stores a mapping between bookmark names and their Id's, for fast lookups.
        private readonly ConcurrentDictionary<string, int> bookmarkMap = new ConcurrentDictionary<string, int>();

        /// <summary>
        /// Saves a bookmark.
        /// </summary>
        /// <param name="bookmarkId">The Id of the bookmark to save.</param>
        /// <param name="position">The position which should be used when writing the bookmark.</param>
        /// <param name="logger">An optional <see cref="ILogger"/> implementation used for logging update operations and errors.</param>
        public void SaveBookmark(int bookmarkId, long position, ILogger logger)
        {
            // When sources are stopped, they will persist their current bookmarks and remove them.
            // In the event that a source is stopped before the sink can call back to update the bookmark,
            // this code will find that there is no bookmark with that Id available, and will just return.
            if (!bookmarks.TryGetValue(bookmarkId, out BookmarkInfo bookmark)) return;

            try
            {
                // Only one process can update a bookmark at a given time, so we control concurrency by
                // using a semaphore that is defined inside the bookmark object itself.
                bookmark.Semaphore.Wait();

                // Don't update a bookmark with a newer or same value.
                if (bookmark.Position >= position) return;

                // When a source file rolls (and truncates), the effective position will be reset to 0.
                // However, since the buffer isn't aware of the rolling, it will just update it to the last uploaded batch.
                // This will mean that any new events will be dropped. To prevent this, when the source detects that a "roll"
                // has occurred, it will update the position to "-1", so that this method knows to discard the last updated position
                // and use the value "0" instead.
                bookmark.Position = bookmark.Position == -1 ? 0 : position;

                // Invoke the update action that is configured for bookmark.
                logger?.LogDebug("Updating bookmark '{0}' to position '{1}'", bookmark.Name, bookmark.Position);
                bookmark.UpdateAction.Invoke(position);
            }
            catch (Exception ex)
            {
                logger?.LogError("Failed to update bookmark {0}: {1}", bookmark.Name, ex.ToMinimized());
            }
            finally
            {
                bookmark.Semaphore.Release();
            }
        }

        /// <summary>
        /// Resets the position of a bookmark in memory.
        /// This is used for rolling file sources, where the same file name is used but content is truncated.
        /// </summary>
        /// <param name="name">The name of the bookmark to update.</param>
        /// <param name="newPosition">The new position that should be used for future updates.</param>
        public void ResetBookmarkPosition(string name, long newPosition)
        {
            if (!bookmarks.TryGetValue(GetBookmarkId(name), out BookmarkInfo bookmark)) return;

            // Since we're updating the value, we need to ensure that we're locking the bookmark so nothing else can update it.
            bookmark.Semaphore.Wait();
            bookmark.Position = newPosition;
            bookmark.Semaphore.Release();
        }

        /// <summary>
        /// Registers a new bookmark with the BookmarkManager.
        /// </summary>
        /// <param name="name">The name of the bookmark. This must be unique across the system.</param>
        /// <param name="initialPosition">The position at which the bookmark should start.</param>
        /// <param name="action">The callback to be invoked by the AWSBufferedSink implementation when it needs to persist a bookmark's new position</param>
        public BookmarkInfo RegisterBookmark(string name, long initialPosition, Action<long> action)
        {
            // If the bookmark has already been registered, return the already registered bookmark.
            if (bookmarkMap.TryGetValue(name, out int bookmarkId) && bookmarks.TryGetValue(bookmarkId, out BookmarkInfo bookmark))
                return bookmark;

            // Since bookmarks have a unique ID, we want to ensure that only one thread can increment
            // the id at a time, so we use a semaphone inside the class to control concurrency.
            semaphore.Wait();
            bookmark = new BookmarkInfo
            {
                Id = nextBookmarkInfoIdCounter,
                Name = name,
                Position = initialPosition,
                UpdateAction = action
            };

            if (bookmarks.TryAdd(bookmark.Id, bookmark))
                bookmarkMap.TryAdd(bookmark.Name, bookmark.Id);

            nextBookmarkInfoIdCounter++;
            semaphore.Release();
            return bookmark;
        }

        /// <summary>
        /// Removes a bookmark.
        /// </summary>
        /// <param name="bookmarkId">The Id of the bookmark to remove.</param>
        public void RemoveBookmark(int bookmarkId)
        {
            if (!bookmarks.TryRemove(bookmarkId, out BookmarkInfo bookmark)) return;
            bookmarkMap.TryRemove(bookmark.Name, out _);
            bookmark.Semaphore.Dispose();
        }

        /// <summary>
        /// Removes a bookmark.
        /// </summary>
        /// <param name="name">The name of the bookmark to remove.</param>
        public void RemoveBookmark(string name)
        {
            RemoveBookmark(GetBookmarkId(name));
        }

        /// <summary>
        /// Retrieves a bookmark object that has been registered to the BookmarkManager.
        /// Returns null if no bookmark with that name is found.
        /// </summary>
        /// <param name="name">The name of the bookmark</param>
        public BookmarkInfo GetBookmark(string name)
        {
            return bookmarks.TryGetValue(GetBookmarkId(name), out BookmarkInfo result) ? result : null;
        }

        /// <summary>
        /// Retrieves the Id of a bookmark given the bookmark name.
        /// Returns 0 if no bookmark with that name is found.
        /// </summary>
        /// <param name="name">The name of the bookmark</param>
        public int GetBookmarkId(string name)
        {
            return bookmarkMap.TryGetValue(name, out int result) ? result : 0;
        }

        /// <summary>
        /// Retrieves an enumerable of all registered bookmarks.
        /// </summary>
        public IEnumerable<BookmarkInfo> GetBookmarks()
        {
            return bookmarks.Values;
        }
    }
}
