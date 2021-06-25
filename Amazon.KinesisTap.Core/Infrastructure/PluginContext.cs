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
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Amazon.KinesisTap.Core.Metrics;
using System;

namespace Amazon.KinesisTap.Core
{
    /// <inheritdoc/>
    public class PluginContext : IPlugInContext
    {
        public const string SOURCE_TYPE = "SOURCE_TYPE";
        public const string SINK_TYPE = "SINK_TYPE";
        public const string PARSER_FACTORIES = "PARSER_FACTORIES";
        public const string SOURCE_OUTPUT_TYPE = "SOURCE_OUTPUT_TYPE";
        private readonly IDictionary<string, ICredentialProvider> _credentialProviders;

        public PluginContext(IConfiguration config, ILogger logger, IMetrics metrics)
            : this(config, logger, metrics, null, null, null)
        {
        }

        public PluginContext(
            IConfiguration config,
            ILogger logger,
            IMetrics metrics,
            IBookmarkManager bookmarkManager,
            IDictionary<string, ICredentialProvider> credentialProviders,
            IParameterStore parameterStore)
        {
            Configuration = config;
            Logger = logger;
            Metrics = metrics;
            BookmarkManager = bookmarkManager;
            _credentialProviders = credentialProviders;
            ParameterStore = parameterStore;
        }

        /// <inheritdoc/>
        public IConfiguration Configuration { get; }

        /// <inheritdoc/>
        public ILogger Logger { get; }

        /// <inheritdoc/>
        public IMetrics Metrics { get; }

        /// <inheritdoc/>
        public IParameterStore ParameterStore { get; }

        /// <inheritdoc/>
        public ICredentialProvider GetCredentialProvider(string id) => _credentialProviders?[id];

        /// <inheritdoc/>
        public IDictionary<string, object> ContextData { get; } = new Dictionary<string, object>();

        /// <inheritdoc/>
        public static ILogger ServiceLogger { get; set; }

        /// <inheritdoc/>
        public NetworkStatus NetworkStatus { get; set; }

        /// <inheritdoc/>
        public string SessionName { get; set; }

        /// <inheritdoc/>
        public bool Validated { get; set; }

        /// <inheritdoc/>
        public IServiceProvider Services { get; set; }

        /// <inheritdoc/>
        public IBookmarkManager BookmarkManager { get; }
    }
}
