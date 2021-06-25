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
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Hosting
{
    /// <summary>
    /// Manages the sessions.
    /// </summary>
    public interface ISessionManager : IDisposable
    {
        /// <summary>
        /// Starts the session manager.
        /// </summary>
        /// <param name="stoppingToken">Used when the service is stopped.</param>
        Task StartAsync(CancellationToken stoppingToken);

        /// <summary>
        /// Stops the session manager and all the sessions.
        /// </summary>
        /// <param name="stoppingToken">Used when the shut down is no longer graceful</param>
        Task StopAsync(CancellationToken stoppingToken);

        /// <summary>
        /// Launch a validated session, from a config outside the configuration directory.
        /// </summary>
        /// <param name="configPath">Path to configuration's file.</param>
        /// <param name="cancellationToken">Used to cancel the operation.</param>
        /// <returns>The launched session.</returns>
        Task<ISession> LaunchValidatedSession(string configPath, CancellationToken cancellationToken);
    }
}
