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
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Represents a requirement that a directory exists.
    /// </summary>
    public class DirectoryDependency : Dependency
    {
        public string DirectoryPath { get; private set; }
        public override string Name => $"Directory {DirectoryPath}";
       

        public DirectoryDependency(string directoryPath)
        {
            Guard.ArgumentNotNull(directoryPath, "directoryPath");
            this.DirectoryPath = directoryPath;
        }

        /// <summary>
        /// Returns true if the directory is available for use.
        /// </summary>
        /// <returns>True if the directory is available for use.</returns>
        public override bool IsDependencyAvailable()
        {
            return Directory.Exists(DirectoryPath);
        }

        /// <summary>
        /// Release resources if desired.  There are no resources to release in this case.
        /// </summary>
        /// <param name="disposing">Are releasing resources desired</param>
        protected override void Dispose(bool disposing)
        {
        }
    }
}
