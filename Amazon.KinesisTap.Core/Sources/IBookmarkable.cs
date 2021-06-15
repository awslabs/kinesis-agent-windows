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
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Represent a component that works with bookmarks.
    /// </summary>
    /// <remarks>
    /// How it works: A bookmark-able source implements this interface and registers itself with the bookmark manager.
    /// When the bookmark manager flushes data to disk, it calls <see cref="SerializeBookmarks"/> to get the data to be persisted.
    /// When the source sends data (e.g. OnNext()), if BookmarkOnBufferFlush is enabled
    /// the record envelope should include a <see cref="RecordBookmark"/> that contains the source's key and other relevant info (e.g. position).
    /// The bookmark manager will execute <see cref="OnBookmarkCallback"/> once the corresponding records have been sent.
    /// </remarks>
    public interface IBookmarkable
    {
        /// <summary>
        /// Handle the event when the bookmark data is loaded from disk.
        /// </summary>
        /// <param name="bookmarkData">Bookmark data.</param>
        void OnBookmarkLoaded(byte[] bookmarkData);

        /// <summary>
        /// Handle the event when the records have been sent.
        /// </summary>
        /// <param name="recordBookmarkData">List of bookmark data of the records</param>
        ValueTask OnBookmarkCallback(IEnumerable<RecordBookmark> recordBookmarkData);

        /// <summary>
        /// Produces the bookmark data. Called by bookmark manager when flushes to disk.
        /// </summary>
        /// <returns>Bookmark data to be flushed to disk. The 'null' return value indicates that the bookmark should be removed.</returns>
        byte[] SerializeBookmarks();

        /// <summary>
        /// Unique key that defines this component in the bookmark system. Usually this is set to the source's ID
        /// </summary>
        string BookmarkKey { get; }
    }
}
