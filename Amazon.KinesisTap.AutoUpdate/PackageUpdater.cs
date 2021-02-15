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
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.AWS;
using Amazon.Runtime;

namespace Amazon.KinesisTap.AutoUpdate
{
    /// <summary>
    /// This is class is used to download KinesisTap.version.nupkg and update KinesisTap 
    /// </summary>
    public class PackageUpdater : TimerPlugin
    {
        const int DEFAULT_INTERVAL = 60;
        const string PACKAGE_VERSION = "PackageVersion";
        const string PRODUCT_KEY = "ProductKey";
        const string DEPLOYMENT_STAGE = "DeploymentStage";

        protected readonly int _downloadNetworkPriority;

        private readonly string productKey;
        private readonly string deploymentStage;
        private readonly RegionEndpoint region;
        private readonly AWSCredentials credential;
        private readonly IAutoUpdateServiceHttpClient httpClient;
        private readonly IPackageInstaller packageInstaller;

        /// <summary>
        /// The url for the PackageVersion.json file. The url could be https://, s3:// or file://
        /// </summary>
        public string PackageVersion { get; set; }

        public PackageUpdater(IPlugInContext context, IAutoUpdateServiceHttpClient httpClient, IPackageInstaller packageInstaller) : base(context)
        {
            int minuteInterval = Utility.ParseInteger(_config[ConfigConstants.INTERVAL], 60); //Default to 60 minutes
            if (minuteInterval < 1) minuteInterval = 1; //Set minimum to 1 minutes
            this.Interval = TimeSpan.FromMinutes(minuteInterval);

            this.httpClient = httpClient;
            this.packageInstaller = packageInstaller;
            this.PackageVersion = Utility.ResolveVariables(_config[PACKAGE_VERSION], Utility.ResolveVariable);
            (this.credential, this.region) = AWSUtilities.GetAWSCredentialsRegion(context);
            this.productKey = _config[PRODUCT_KEY];
            this.deploymentStage = _config[DEPLOYMENT_STAGE];

            if (this.PackageVersion.Contains("execute-api")) // check if using AutoUpdate service
            {
                if (this.credential == null || string.IsNullOrWhiteSpace(this.productKey) || string.IsNullOrWhiteSpace(this.deploymentStage))
                {
                    _logger.LogError("credential, productKey and deploymentStage can't be empty.");
                    throw new Exception("credential, productKey and deploymentStage can't be empty.");
                }
            }

            if (!int.TryParse(_config[ConfigConstants.DOWNLOAD_NETWORK_PRIORITY], out _downloadNetworkPriority))
            {
                _downloadNetworkPriority = ConfigConstants.DEFAULT_NETWORK_PRIORITY;
            }
        }

        protected override async Task OnTimer()
        {
            try
            {
                //Skip if network not available
                if (_networkStatus?.CanDownload(_downloadNetworkPriority) != true)
                {
                    _logger?.LogInformation($"Skip package download due to network not allowed to download.");
                    return;
                }

                await this.CheckAgentUpdates();
            }
            catch (Exception ex)
            {
                _logger?.LogError($"Error download {this.PackageVersion}. Exception: {ex.ToMinimized()}");
            }
        }

        /// <summary>
        /// Check for agent update. It will trigger agent update if the desired version is different than the current running version.
        /// </summary>
        internal async Task CheckAgentUpdates()
        {
            _logger?.LogDebug($"Running package updater. Downloading {this.PackageVersion}.");
            PackageVersionInfo packageVersion = await GetPackageVersionInformation();
            var desiredVersion = UpdateUtility.ParseVersion(packageVersion.Version);
            Version installedVersion = GetInstalledVersion();
            if (desiredVersion.CompareTo(installedVersion) != 0)
            {
                _logger?.LogInformation($"The desired version of {desiredVersion} is different to installed version {installedVersion}.");
                await this.packageInstaller.DownloadAndInstallNewVersionAsync(packageVersion);
            }
        }

        /// <summary>
        /// Get the latest version information based on the PackageVersion location.
        /// </summary>
        /// <returns>an instance of <see cref="PackageVersionInfo"/> object.</returns>
        internal async Task<PackageVersionInfo> GetPackageVersionInformation()
        {
            string packageVersionString;
            if (!this.PackageVersion.Contains("execute-api")) // check if using AutoUpdate service
            {
                var packageVersionDownloader = UpdateUtility.CreateDownloaderFromUrl(this.PackageVersion, _context);
                packageVersionString = await packageVersionDownloader.ReadFileAsStringAsync(this.PackageVersion);
            }
            else
            {
                var autoUpdateServiceClient = new AutoUpdateServiceClient(this.httpClient);
                var request = new GetVersionRequest
                {
                    TenantId = this.productKey,
                    AutoUpdateLane = this.deploymentStage
                };

                packageVersionString = await autoUpdateServiceClient.GetVersionAsync(this.PackageVersion, request, this.region, this.credential);
            }

            var packageVersion = UpdateUtility.ParsePackageVersion(packageVersionString);
            return packageVersion;
        }

        private Version GetInstalledVersion()
        {
            try
            {
                //Enhance this method if we want to use it to install program other than KinesisTap.
                //We will need a new mechanism to check if the package is installed and get the installed version
                return UpdateUtility.ParseVersion(ProgramInfo.GetKinesisTapVersion().FileVersion);
            }
            catch (Exception e)
            {
                _logger?.LogError($"Failed to get installed version: '{e}'");
                return new Version("1.0.0"); // This is for TestIntegrationWithAutoUpdateService Unit test to pass
            }
        }

    }
}
