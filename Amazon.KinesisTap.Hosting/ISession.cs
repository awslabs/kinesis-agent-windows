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
using Amazon.KinesisTap.Core.Metrics;
using System;
using System.Collections.Generic;

namespace Amazon.KinesisTap.Hosting
{
    /// <summary>
    /// This is a new concept defined to work with supporting multiple configuration files.
    /// A 'Session' is an workflow associated with loading, executing, and stopping a configuration files.
    /// The SessionManager class takes care of discovering config files and creating sessions.
    /// </summary>
    public interface ISession : IDisposable
    {
        /// <summary>
        /// The ID of the session.
        /// </summary>
        int Id { get; }

        /// <summary>
        /// A flag indicating whether the session has been disposed.
        /// </summary>
        bool Disposed { get; }

        /// <summary>
        /// The point in time that this session starts up.
        /// </summary>
        DateTime StartTime { get; }

        /// <summary>
        /// Start the Log manager
        /// </summary>
        void Start();

        /// <summary>
        /// Stop the Log manager
        /// </summary>
        void Stop(bool serviceStopping);

        /// <summary>
        /// Publish service-level metrics. This is only called on the default session.
        /// </summary>
        void PublishServiceLevelCounter(string id, string category, CounterTypeEnum counterType, IDictionary<string, MetricValue> counters);
    }
}
