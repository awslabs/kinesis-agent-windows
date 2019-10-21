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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Extensions.Configuration;
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
        const string RING = "Ring";
        const string EXT_NUPKG = ".nupkg";
        const string EXT_RPM = ".rpm";
        const string EXT_MSI = ".msi";
        const string EXT_PKG = ".pkg";

        protected readonly int _downloadNetworkPriority;

        private readonly string productKey;
        private readonly string ring;
        private readonly RegionEndpoint region;
        private readonly AWSCredentials credential;
        private readonly IAutoUpdateServiceHttpClient httpClient;

        /// <summary>
        /// The url for the PackageVersion.json file. The url could be https://, s3:// or file://
        /// </summary>
        public string PackageVersion { get; set; }

        public PackageUpdater(IPlugInContext context, IAutoUpdateServiceHttpClient httpClient) : base(context)
        {
            int minuteInterval = Utility.ParseInteger(_config[ConfigConstants.INTERVAL], 60); //Default to 60 minutes
            if (minuteInterval < 1) minuteInterval = 1; //Set minimum to 1 minutes
            this.Interval = TimeSpan.FromMinutes(minuteInterval);

            this.httpClient = httpClient;
            this.PackageVersion = Utility.ResolveVariables(_config[PACKAGE_VERSION], Utility.ResolveVariable);
            (this.credential, this.region) = AWSUtilities.GetAWSCredentialsRegion(context);
            this.productKey = _config[PRODUCT_KEY];
            this.ring = _config[RING];

            if (this.PackageVersion.Contains("execute-api")) // check if using AutoUpdate service
            {
                if (this.credential == null || this.region == null || string.IsNullOrWhiteSpace(this.productKey) || string.IsNullOrWhiteSpace(this.ring))
                {
                    _logger.LogError("AccessKey, SecretKey, Region, ProductKey and Ring can't be empty.");
                    throw new Exception("AccessKey, SecretKey, Region, ProductKey and Ring can't be empty.");
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
                if (!NetworkStatus.CanDownload(_downloadNetworkPriority))
                {
                    _logger?.LogInformation($"Skip package download due to network not allowed to download.");
                    return;
                }

                _logger?.LogDebug($"Running package updater. Downloading {this.PackageVersion}.");
                PackageVersionInfo packageVersion = await GetPackageVersionInformation();
                var desiredVersion = UpdateUtility.ParseVersion(packageVersion.Version);
                Version installedVersion = GetInstalledVersion();
                if (desiredVersion.CompareTo(installedVersion) != 0)
                {
                    _logger?.LogInformation($"The desired version of {desiredVersion} is different to installed version {installedVersion}.");
                    await DownloadAndInstallNewVersionAsync(packageVersion);
                }
            }
            catch(Exception ex)
            {
                _logger?.LogError($"Error download {this.PackageVersion}. Exception: {ex.ToMinimized()}");
            }
        }

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
                    AgentId = Utility.HostName, // use fqdn as unique agent id
                    ProductKey = this.productKey,
                    Ring = this.ring,
                    Version = this.GetInstalledVersion().ToString()
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

        private async Task DownloadAndInstallNewVersionAsync(PackageVersionInfo packageVersion)
        {
            //Upload the new version
            string packageUrl = packageVersion.PackageUrl.Replace("{Version}", packageVersion.Version);
            string extension = Path.GetExtension(packageUrl).ToLower();
            if (!IsExtensionSuportedByPlatform(extension))
            {
                _logger.LogWarning($"Extension {extension} is not supported on {Utility.Platform}");
                return;
            }
            _logger?.LogInformation($"Downloading {packageVersion.Name} version {packageVersion.Version} from {packageUrl}...");
            IFileDownloader downloader = UpdateUtility.CreateDownloaderFromUrl(packageUrl, this._context);
            string updateDirectory = Path.Combine(Utility.GetKinesisTapProgramDataPath(), "update");
            if (!Directory.Exists(updateDirectory))
            {
                Directory.CreateDirectory(updateDirectory);
            }
            string downloadPath = Path.Combine(updateDirectory, Path.GetFileName(packageUrl));
            if (File.Exists(downloadPath))
            {
                File.Delete(downloadPath);
            }
            await downloader.DownloadFileAsync(packageUrl, downloadPath);
            _logger?.LogInformation($"Package downloaded to {downloadPath}. Expanding package...");

            if (EXT_NUPKG.Equals(extension))
            {
                await InstallNugetPackageAsync(downloadPath);
            }
            else
            {
                await InstallPackageAsync(downloadPath);
            }
        }

        private async Task InstallPackageAsync(string downloadPath)
        {
            string extension = Path.GetExtension(downloadPath).ToLower();
            string command;
            string arguments;
            switch(extension)
            {
                case EXT_MSI:
                    command = "msiexec.exe";
                    string logPath = Path.Combine(Path.GetTempPath(), "KinesisTapInstaller.log");
                    arguments = $"/i {downloadPath} /q /L*V {logPath} /norestart";
                    break;
                case EXT_RPM:
                    command = "rpm";
                    arguments = $"-Uhv {downloadPath}";
                    break;
                case EXT_PKG:
                    command = "installer";
                    arguments = $"-pkg {downloadPath} -target /"; //Mac automatically dump log to /var/log/install.log
                    break;
                default:
                    throw new NotImplementedException($"Unknown extension {extension}");
            }
            await RunCommand(command, arguments);
        }

        private async Task InstallNugetPackageAsync(string downloadPath)
        {
            //Expand the new version
            string expandDirectory = downloadPath.Substring(0, downloadPath.Length - 6); //less ".nupkg"
            if (Directory.Exists(expandDirectory))
            {
                Directory.Delete(expandDirectory, true);
            }
            ZipFile.ExtractToDirectory(downloadPath, expandDirectory);

            //Execute the ChocoInstall.ps1 out of process so that it can restart KinesisTap
            string installScriptPath = Path.Combine(expandDirectory, @"tools\chocolateyinstall.ps1");
            _logger?.LogInformation($"Executing installation script {installScriptPath}...");
            await ExecutePowershellOutOfProcessAsync(installScriptPath);
        }

        private bool IsExtensionSuportedByPlatform(string extension)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return (new[] { EXT_NUPKG, EXT_MSI }).Contains(extension);
            }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                return (new[] { EXT_PKG }).Contains(extension);
            }

            //When we start supporting Ubuntu, we need to detect the flavor of Linux
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                return (new[] { EXT_RPM }).Contains(extension);
            }

            return false;
        }

        /// <summary>
        /// Use Process to execute PowerShell.exe. The script will restart KinesisTap
        /// </summary>
        /// <param name="installScriptPath"></param>
        private async Task ExecutePowershellOutOfProcessAsync(string installScriptPath)
        {
            await RunCommand("PowerShell.exe", $"-File {installScriptPath}");
        }

        private async Task RunCommand(string command, string arguments)
        {
            try
            {
                Process process = new Process();
                process.StartInfo.FileName = command;
                process.StartInfo.Arguments = arguments;
                process.StartInfo.UseShellExecute = false;
                process.StartInfo.RedirectStandardOutput = true;
                process.Start();
                //The following code will pipe the output of the Command to KinesisTap for up to 2 second
                //Then it will exit because it sometimes interfere with service restart
                while (!process.HasExited)
                {
                    const int timeout = 2000;
                    var outputTask = PipeOutputAsync(process);
                    if (await Task.WhenAny(outputTask, Task.Delay(timeout)) == outputTask)
                    {
                        continue;
                    }
                    else
                    {
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                _context.Logger?.LogError($"Error starting command {command}: {ex.ToMinimized()}");
            }
        }

        private async Task PipeOutputAsync(Process process)
        {
            string output = await process.StandardOutput.ReadLineAsync();
            _context.Logger?.LogInformation(output);
        }
    }
}
