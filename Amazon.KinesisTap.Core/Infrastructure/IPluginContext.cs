using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public interface IPlugInContext
    {
        IConfiguration Configuration { get; }

        ILogger Logger { get; }

        IMetrics Metrics { get; }

        ICredentialProvider GetCredentialProvider(string id);

        IParameterStore ParameterStore { get; }

        /// <summary>
        /// Place to store other context data
        /// </summary>
        IDictionary<string, object> ContextData { get; }
    }
}
