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
    /// Record bookmark data based on integer position
    /// </summary>
    public class IntegerPositionRecordBookmark : RecordBookmark
    {
        public IntegerPositionRecordBookmark(string sourceKey, string name, long position)
            : base(sourceKey, name)
        {
            Position = position;
        }

        /// <summary>
        /// Integer position of the record.
        /// </summary>
        public long Position { get; }
    }
}
