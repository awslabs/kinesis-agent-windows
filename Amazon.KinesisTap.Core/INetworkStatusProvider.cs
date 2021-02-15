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
    /// Interface for provider of network status information
    /// </summary>
    public interface INetworkStatusProvider
    {
        /// <summary>
        /// Indicate whether the network is available
        /// </summary>
        /// <returns>Whether the network is available</returns>
        bool IsAvailable();
        /// <summary>
        /// Indicate whether can upload
        /// </summary>
        /// <param name="priority">Indicate the designed priority. Smaller number is higher.</param>
        /// <returns>Whether the program can upload</returns>
        bool CanUpload(int priority);
        /// <summary>
        /// Indicate whether can download
        /// </summary>
        /// <param name="priority">Indicate the designed priority. Smaller number is higher.</param>
        /// <returns>Whether the program can download</returns>
        bool CanDownload(int priority);
    }
}
