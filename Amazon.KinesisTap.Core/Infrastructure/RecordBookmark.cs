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
    /// <summary>
    /// Bookmark context for a record.
    /// </summary>
    /// <remarks>
    /// The <see cref="SourceKey"/> is the key of the IBookmarkable source, used to identify which source the record comes from.
    /// However within one source there might be many 'streams' that produce data (think files in a Directory source).
    /// All this information is required to handle the bookmark event correctly.
    /// Therefore we need the <see cref="StreamId"/> property to indicate this information.
    /// How <see cref="StreamId"/> is constructed is up to the source it self. For example, in the Directory source it is the file's path.
    /// </remarks>
    public abstract class RecordBookmark
    {
        protected RecordBookmark(string sourceKey, string name)
        {
            SourceKey = sourceKey;
            StreamId = name;
        }

        /// <summary>
        /// The bookmark key of the IBookmarkable source.
        /// </summary>
        public string SourceKey { get; }

        /// <summary>
        /// Identifier for the stream within the source where the record comes from.
        /// </summary>
        public string StreamId { get; }
    }
}
