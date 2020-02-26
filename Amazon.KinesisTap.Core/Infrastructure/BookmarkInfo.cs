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
    using System.Threading;

    public class BookmarkInfo
    {
        public string Name { get; set; }

        public int Id { get; set; }

        public long Position { get; set; }

        public Action<long> UpdateAction { get; set; }

        public SemaphoreSlim Semaphore { get; private set; } = new SemaphoreSlim(1, 1);
    }
}
