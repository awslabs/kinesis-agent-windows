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
using Microsoft.Extensions.Configuration;
using System;

namespace Amazon.KinesisTap.Hosting
{
    /// <summary>
    /// The default session factory that creates <see cref="Session"/>.
    /// </summary>
    public class DefaultSessionFactory : ISessionFactory
    {
        private readonly IServiceProvider _services;
        private readonly IMetrics _metrics;

        public DefaultSessionFactory(IServiceProvider services, IMetrics metrics)
        {
            _services = services;
            _metrics = metrics;
        }

        /// <inheritdoc/>
        public ISession CreateSession(string name, IConfiguration config) => new Session(name, config, _metrics, _services, false);

        /// <inheritdoc/>
        public ISession CreateValidatedSession(string name, IConfiguration config) => new Session(name, config, _metrics, _services, true);
    }
}
