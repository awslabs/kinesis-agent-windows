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
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Centralized bookmark manager.
    /// </summary>
    public interface IBookmarkManager
    {
        /// <summary>
        /// Startup the system.
        /// </summary>
        /// <param name="stopToken">Token that is cancelled when the session exits.</param>
        /// <returns>Task that completes when the system is started.</returns>
        ValueTask StartAsync(CancellationToken stopToken);

        /// <summary>
        /// Stop the system.
        /// </summary>
        /// <param name="gracefulCancelToken">Token that is cancelled when the session no longer exit gracefully.</param>
        /// <returns>Task that completes when the system is stopped.</returns>
        ValueTask StopAsync(CancellationToken gracefulCancelToken);

        /// <summary>
        /// Register a source to this system, allowing the bookmark data to be synchronized 
        /// and bookmark events associated with that source to be handled.
        /// </summary>
        /// <param name="source">Source to be registered.</param>
        /// <param name="stopToken">Cancellation token.</param>
        Task RegisterSourceAsync(IBookmarkable source, CancellationToken stopToken = default);

        /// <summary>
        /// Called when records have been sent to the sink's destination. This is used for 'BookmarkOnFlush'. 
        /// This operation is asynchronous so that it does not block the sink.
        /// </summary>
        /// <param name="sourceKey">Key of the source of the records.</param>
        /// <param name="bookmarkData">List of bookmark data of the records.</param>
        ValueTask BookmarkCallback(string sourceKey, IEnumerable<RecordBookmark> bookmarkData);
    }
}
