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
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Interface for all KinesisTap plugins.
    /// </summary>
    public interface IPlugIn
    {
        /// <summary>
        /// Plugin's ID.
        /// </summary>
        string Id { get; set; }

        /// <summary>
        /// Start the plugin. 
        /// </summary>
        /// <param name="stopToken">A token that throws when the session that the plugin belongs to stops.</param>
        /// <returns>A task that completes when the plugin is started.</returns>
        ValueTask StartAsync(CancellationToken stopToken);

        /// <summary>
        /// Stop the plugin.
        /// </summary>
        /// <returns>A task that completes when the plugin is fully stopped.</returns>
        ValueTask StopAsync(CancellationToken gracefulStopToken);
    }
}
