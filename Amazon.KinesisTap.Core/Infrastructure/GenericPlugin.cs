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
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Amazon.KinesisTap.Core.Metrics;
using System.Threading.Tasks;
using System.Threading;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Minimal base class for all generic plugins with synchronous Start/Stop methods.
    /// </summary>
    public abstract class GenericPlugin : IGenericPlugin
    {
        protected IPlugInContext _context;
        protected IConfiguration _config;
        protected ILogger _logger;
        protected IMetrics _metrics;

        public GenericPlugin(IPlugInContext context)
        {
            _context = context;
            _config = context.Configuration;
            _logger = context.Logger;
            _metrics = context.Metrics;
            Id = _config[ConfigConstants.ID];
        }

        public string Id { get; set; }

        public ValueTask StartAsync(CancellationToken stopToken)
        {
            Start();
            return ValueTask.CompletedTask;
        }

        public ValueTask StopAsync(CancellationToken stopToken)
        {
            Stop();
            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// When implemented, start the plugin synchronously.
        /// </summary>
        public abstract void Start();

        /// <summary>
        /// When implemented, stop the plugin synchronously and returns once the plugin is stopped.
        /// </summary>
        public abstract void Stop();

        protected int GetSettingIntWithDefault(string key, int defaultValue)
        {
            string stringValue = _config[key];
            if (!string.IsNullOrWhiteSpace(stringValue))
            {
                if (int.TryParse(stringValue, out int intValue))
                {
                    return intValue;
                }
                else
                {
                    throw new ConfigurationException($"{key} must be an integer");
                }
            }
            else
            {
                return defaultValue;
            }
        }
    }
}
