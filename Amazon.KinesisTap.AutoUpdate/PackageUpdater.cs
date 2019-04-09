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
using System.Text;
using System.Threading.Tasks;

using AsyncFriendlyStackTrace;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.AutoUpdate
{
    /// <summary>
    /// This is class is used to download KinesisTap.version.nupkg and update KinesisTap 
    /// </summary>
    public class PackageUpdater : TimerPlugin
    {
        const int DEFAULT_INTERVAL = 60;
        const string PACKAGE_VERSION = "PackageVersion";

        /// <summary>
        /// The url for the PackageVersion.json file. The url could be https://, s3:// or file://
        /// </summary>
        public string PackageVersion { get; set; }

        public PackageUpdater(IPlugInContext context) : base(context)
        {
            int minuteInterval = Utility.ParseInteger(_config[ConfigConstants.INTERVAL], 60); //Default to 60 minutes
            if (minuteInterval < 1) minuteInterval = 1; //Set minimum to 1 minutes
            this.Interval = TimeSpan.FromMinutes(minuteInterval);
            this.PackageVersion = Utility.ResolveVariables(_config[PACKAGE_VERSION], Utility.ResolveVariable);
        }

        protected override async Task OnTimer()
        {
            try
            {
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

        private async Task<PackageVersionInfo> GetPackageVersionInformation()
        {
            var packageVersionDownloader = UpdateUtility.CreateDownloaderFromUrl(this.PackageVersion, _context);
            string packageVersionString = await packageVersionDownloader.ReadFileAsStringAsync(this.PackageVersion);
            var packageVersion = UpdateUtility.ParsePackageVersion(packageVersionString);
            return packageVersion;
        }

        private Version GetInstalledVersion()
        {
            //Enhance this method if we want to use it to install program other than KinesisTap.
            //We will need a new mechanism to check if the package is installed and get the installed version
            return UpdateUtility.ParseVersion(ProgramInfo.GetKinesisTapVersion().FileVersion);
        }

        private async Task DownloadAndInstallNewVersionAsync(PackageVersionInfo packageVersion)
        {
            //Upload the new version
            string packageUrl = packageVersion.PackageUrl.Replace("{Version}", packageVersion.Version);
            _logger?.LogInformation($"Downloading {packageVersion.Name} version {packageVersion.Version} from {packageUrl}...");
            IFileDownloader downloader = UpdateUtility.CreateDownloaderFromUrl(packageUrl, this._context);
            string updateDirectory = Path.Combine(Utility.GetKinesisTapProgramDataPath(), "update");
            if (!Directory.Exists(updateDirectory))
            {
                Directory.CreateDirectory(updateDirectory);
            }
            string downloadPath = Path.Combine(updateDirectory, $"KinesisTap.{packageVersion.Version}.nupkg");
            if (File.Exists(downloadPath))
            {
                File.Delete(downloadPath);
            }
            await downloader.DownloadFileAsync(packageUrl, downloadPath);
            _logger?.LogInformation($"Package downloaded to {downloadPath}. Expanding package...");

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

        /// <summary>
        /// Use Process to execute PowerShell.exe. The script will restart KinesisTap
        /// </summary>
        /// <param name="installScriptPath"></param>
        private async Task ExecutePowershellOutOfProcessAsync(string installScriptPath)
        {
            try
            {
                Process process = new Process();
                process.StartInfo.FileName = "PowerShell.exe";
                process.StartInfo.Arguments = $"-File {installScriptPath}";
                process.StartInfo.UseShellExecute = false;
                process.StartInfo.RedirectStandardOutput = true;
                process.Start();
                //The following code will pipe the output of the Powershell to KinesisTap for up to 2 second
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
            catch(Exception ex)
            {
                _context.Logger?.LogError($"Error starting powershell script: {ex.ToMinimized()}");
            }
        }

        private async Task PipeOutputAsync(Process process)
        {
            string output = await process.StandardOutput.ReadLineAsync();
            _context.Logger?.LogInformation(output);
        }
    }
}
