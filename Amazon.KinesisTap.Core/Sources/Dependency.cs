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

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Represents a local resource depended on by a source (a service running or a directory existing, for example).
    /// </summary>
    public abstract class Dependency : IDisposable
    {
        /// <summary>
        /// A user understandable string which describes the dependency.
        /// </summary>
        public abstract string Name { get; }

        /// <summary>
        /// Returns true if the dependency is available for use.
        /// </summary>
        /// <returns>True if the dependency is available for use.</returns>
        public abstract bool IsDependencyAvailable();

        #region IDisposable Support
        /// <summary>
        /// Release resources if desired.
        /// </summary>
        /// <param name="disposing">Are releasing resources desired</param>
        protected abstract void Dispose(bool disposing);
        
        // Releases any resources related to the dependency.
        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
