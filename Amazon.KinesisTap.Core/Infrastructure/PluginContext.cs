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

namespace Amazon.KinesisTap.Core
{
    public class PluginContext : IPlugInContext
    {
        public const string SOURCE_TYPE = "SOURCE_TYPE";
        public const string SINK_TYPE = "SINK_TYPE";
        public const string PARSER_FACTORIES = "PARSER_FACTORIES";
        public const string SOURCE_OUTPUT_TYPE = "SOURCE_OUTPUT_TYPE";

        private readonly IConfiguration _config;
        private readonly ILogger _logger;
        private readonly IMetrics _metrics;
        private readonly IDictionary<string, ICredentialProvider> _credentialProviders;
        private readonly IParameterStore _parameterStore;
        private readonly IDictionary<string, object> _contextData = new Dictionary<string, object>();
        private readonly BookmarkManager _bookmarkManager;

        public PluginContext(IConfiguration config, ILogger logger, IMetrics metrics)
            : this(config, logger, metrics, new BookmarkManager(), null, null)
        {
        }

        public PluginContext(IConfiguration config, ILogger logger, IMetrics metrics, BookmarkManager bookmarkManager)
            : this(config, logger, metrics, bookmarkManager, null, null)
        {
        }

        public PluginContext(IConfiguration config, ILogger logger, IMetrics metrics, BookmarkManager bookmarkManager,
            IDictionary<string, ICredentialProvider> credentialProviders, IParameterStore parameterStore)
        {
            _config = config;
            _logger = logger;
            _metrics = metrics;
            _bookmarkManager = bookmarkManager;
            _credentialProviders = credentialProviders;
            _parameterStore = parameterStore;
        }

        public IConfiguration Configuration => _config;

        public ILogger Logger => _logger;

        public IMetrics Metrics => _metrics;

        public BookmarkManager BookmarkManager => _bookmarkManager;

        public IParameterStore ParameterStore => _parameterStore;

        public ICredentialProvider GetCredentialProvider(string id) => _credentialProviders?[id];

        public IDictionary<string, object> ContextData => _contextData;

        public static ILogger ServiceLogger { get; internal set; }

        public NetworkStatus NetworkStatus { get; set; }

        public int SessionId { get; set; }

        public bool Validated { get; set; }
    }
}
