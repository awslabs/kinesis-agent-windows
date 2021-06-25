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
namespace Amazon.KinesisTap.AWS.Failover.Components
{
    /// <summary>
    /// An interface for AWS Client region state.
    /// </summary>
    public interface IFailoverRegion<TRegion>
    {
        /// <summary>
        /// Get Region status.
        /// </summary>
        /// <returns>Region status.</returns>
        public bool Available();

        /// <summary>
        /// Set Region in-use.
        /// </summary>
        public void MarkInUse();

        /// <summary>
        /// Set Region is-down.
        /// </summary>
        public void MarkIsDown();

        /// <summary>
        /// Reset Region state.
        /// </summary>
        public void Reset();
    }
}
