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
    /// This is a new concept defined to work with supporting multiple configuration files.
    /// A 'Session' is an workflow associated with loading, executing, and stopping a configuration files.
    /// The <see cref="ISessionManager"/> class takes care of discovering config files and creating sessions.
    /// </summary>
    public interface ISession : IDisposable
    {
        /// <summary>
        /// The name of the session. For the default session, this is null. 
        /// </summary>
        string Name { get; }

        /// <summary>
        /// The session's display name. For default session, this value is 'default'. For other session, it's the same as <see cref="Name"/>
        /// </summary>
        string DisplayName { get; }

        /// <summary>
        /// A flag indicating whether the session has been disposed.
        /// </summary>
        bool Disposed { get; }

        /// <summary>
        /// The point in time that this session starts up.
        /// </summary>
        DateTime StartTime { get; }

        /// <summary>
        /// Whether this is a 'validated' session.
        /// </summary>
        bool IsValidated { get; }

        /// <summary>
        /// Whether this is the default session
        /// </summary>
        bool IsDefault { get; }

        /// <summary>
        /// Start the Session
        /// </summary>
        /// <param name="abortToken">Used to abort the start operation</param>
        Task StartAsync(CancellationToken abortToken);

        /// <summary>
        /// Stop the Session
        /// </summary>
        /// <param name="gracefulStopToken">Throws when the 'stop' operation is no longer graceful (i.e. due to service shutting down)</param>
        Task StopAsync(CancellationToken gracefulStopToken);
    }
}
