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
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace Amazon.KinesisTap.Core
{
    public interface IPlugInContext
    {
        /// <summary>
        /// Configuration for the Plugin
        /// </summary>
        IConfiguration Configuration { get; }

        /// <summary>
        /// Plugin's logger
        /// </summary>
        ILogger Logger { get; }

        /// <summary>
        /// Plugin's metrics publisher
        /// </summary>
        IMetrics Metrics { get; }

        /// <summary>
        /// <see cref="BookmarkManager"/> instance for the Plugin
        /// </summary>
        BookmarkManager BookmarkManager { get; }

        /// <summary>
        /// Returns the Credential Provider instance from its ID
        /// </summary>
        /// <param name="id">Credential Provider's ID</param>
        ICredentialProvider GetCredentialProvider(string id);

        /// <summary>
        /// KinesisTap ParameterStore
        /// </summary>
        IParameterStore ParameterStore { get; }

        /// <summary>
        /// Place to store other context data
        /// </summary>
        IDictionary<string, object> ContextData { get; }

        /// <summary>
        /// Allow plugins to check for network's status. This property could be null.
        /// </summary>
        NetworkStatus NetworkStatus { get; }

        /// <summary>
        /// Id of the session.
        /// </summary>
        int SessionId { get; }

        /// <summary>
        /// Whether the configuration file that the plugin configuration comes from has been validated or not.
        /// </summary>
        bool Validated { get; set; }
    }
}
