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
using System.Text;
using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.Filesystem
{
    /// <summary>
    /// Operational settings for <see cref="AsyncDirectorySource{TData, TContext}{T}"/>
    /// </summary>
    public class DirectorySourceOptions
    {
        public string[] NameFilters { get; set; }

        public bool IncludeSubdirectories { get; set; }

        public int NumberOfConsecutiveIOExceptionsToLogError { get; set; } = 3;

        public bool BookmarkOnBufferFlush { get; set; }

        public string[] IncludeDirectoryFilter { get; set; }

        public int QueryPeriodMs { get; set; } = 1000;

        public InitialPositionEnum InitialPosition { get; set; }

        public DateTime InitialPositionTimestamp { get; set; } = DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Local);

        public Encoding PreferedEncoding { get; set; }

        public int ReadBatchSize { get; set; } = 4000;

        public bool OmitLineNumber { get; set; }
    }
}
