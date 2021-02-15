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
using System.IO;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.AutoUpdate
{
    /// <summary>
    /// This class is used to update configuration files
    /// </summary>
    public class ConfigurationFileUpdater : TimerPlugin
    {
        protected readonly int _downloadNetworkPriority;

        /// <summary>
        /// Source Url of the configuration file, such as an https://, s3:// or file:// url 
        /// </summary>
        public string Source { get; set; }

        /// <summary>
        /// The Path of the configuration file to update. It could be absolute or relative (to the main exe) path. 
        /// </summary>
        public string Destination { get; set; }

        public ConfigurationFileUpdater(IPlugInContext context) : base(context)
        {
            int minuteInterval = Utility.ParseInteger(_config[ConfigConstants.INTERVAL], 5); //Default to 5 minutes
            if (minuteInterval < 1) minuteInterval = 1; //Set minimum to 1 minute
            this.Interval = TimeSpan.FromMinutes(minuteInterval);
            this.Source = Utility.ResolveVariables(_config["Source"], Utility.ResolveVariable);
            this.Destination = _config["Destination"];
            if (!int.TryParse(_config[ConfigConstants.DOWNLOAD_NETWORK_PRIORITY], out _downloadNetworkPriority))
            {
                _downloadNetworkPriority = ConfigConstants.DEFAULT_NETWORK_PRIORITY;
            }
        }

        protected async override Task OnTimer()
        {
            try
            {
                //Skip if network not available
                if (_networkStatus?.CanDownload(_downloadNetworkPriority) != true)
                {
                    _logger?.LogInformation($"Skip configuration download due to network not allowed to download.");
                    return;
                }

                _logger?.LogDebug($"Running config updater. Downloading {this.Source}.");
                var configDownloader = UpdateUtility.CreateDownloaderFromUrl(this.Source, _context);
                string newConfig = await configDownloader.ReadFileAsStringAsync(this.Source);
                string configPath = UpdateUtility.ResolvePath(this.Destination);
                if (!File.Exists(configPath) || !newConfig.Equals(File.ReadAllText(configPath)))
                {
                    _logger?.LogInformation($"Config file changed. Updating configuration file.");
                    File.WriteAllText(configPath, newConfig);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError($"Error download {this.Source}. Exception: {ex.ToMinimized()}");
            }
        }
    }
}
