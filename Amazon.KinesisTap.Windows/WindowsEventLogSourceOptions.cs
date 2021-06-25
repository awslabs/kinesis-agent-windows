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
using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.Windows
{
    internal class WindowsEventLogSourceOptions
    {
        /// <summary>
        /// Whether to flush bookmark when records are sent to d
        /// </summary>
        public bool BookmarkOnBufferFlush { get; set; } = false;

        /// <summary>
        /// Include event data
        /// </summary>
        public bool IncludeEventData { get; set; } = false;

        /// <summary>
        /// Initial position setting
        /// </summary>
        public InitialPositionEnum InitialPosition { get; set; } = InitialPositionEnum.Bookmark;

        /// <summary>
        /// Initial position timestamp
        /// </summary>
        public DateTime InitialPositionTimestamp { get; set; } = DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Utc);

        /// <summary>
        /// Custom filters
        /// </summary>
        public string[] CustomFilters { get; set; } = Array.Empty<string>();
    }
}
