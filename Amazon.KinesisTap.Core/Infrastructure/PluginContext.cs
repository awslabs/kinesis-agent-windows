using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Amazon.KinesisTap.Core.Metrics;

namespace Amazon.KinesisTap.Core
{

    public class PluginContext : IPlugInContext
    {
        public const string SOURCE_TYPE = "SOURCE_TYPE";
        public const string SINK_TYPE = "SINK_TYPE";

        private readonly IConfiguration _config;
        private readonly ILogger _logger;
        private readonly IMetrics _metrics;
        private readonly IDictionary<string, ICredentialProvider> _credentialProviders;
        private readonly IParameterStore _parameterStore;
        private readonly IDictionary<string, object> _contextData = new Dictionary<string, object>();

        public PluginContext(IConfiguration config, ILogger logger, IMetrics metrics) : this(config, logger, metrics, null, null)
        {
        }

        public PluginContext(IConfiguration config, ILogger logger, IMetrics metrics, IDictionary<string, ICredentialProvider> credentialProviders, IParameterStore parameterStore)
        {
            _config = config;
            _logger = logger;
            _metrics = metrics;
            _credentialProviders = credentialProviders;
            _parameterStore = parameterStore;
        }

        public IConfiguration Configuration => _config;

        public ILogger Logger => _logger;

        public IMetrics Metrics => _metrics;

        public IParameterStore ParameterStore => _parameterStore;

        public ICredentialProvider GetCredentialProvider(string id) => _credentialProviders?[id];

        public IDictionary<string, object> ContextData => _contextData;
    }
}
