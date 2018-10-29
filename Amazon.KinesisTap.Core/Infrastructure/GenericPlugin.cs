using System;
using System.Collections.Generic;
using System.Text;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Amazon.KinesisTap.Core.Metrics;

namespace Amazon.KinesisTap.Core
{
    public abstract class GenericPlugin : IGenericPlugin
    {
        protected IPlugInContext _context;
        protected IConfiguration _config;
        protected ILogger _logger;
        protected IMetrics _metrics;

        public GenericPlugin(IPlugInContext context)
        {
            this._context = context;
            this._config = context.Configuration;
            this._logger = context.Logger;
            this._metrics = context.Metrics;
           this.Id = _config[ConfigConstants.ID];
        }

        public string Id { get; set; }

        public abstract void Start();

        public abstract void Stop();
    }
}
