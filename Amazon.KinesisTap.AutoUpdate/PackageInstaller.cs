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
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.AutoUpdate
{
    public class PackageInstaller : IPackageInstaller
    {
        const string EXT_NUPKG = ".nupkg";
        const string EXT_RPM = ".rpm";
        const string EXT_MSI = ".msi";
        const string EXT_PKG = ".pkg";

        private readonly ILogger _logger;
        private readonly IPlugInContext _context;

        public PackageInstaller(IPlugInContext context)
        {
            _context = context;
            _logger = context.Logger;
        }

        /// <summary>
        /// Download the new version of KinesisTap build using package Url and install it.
        /// </summary>
        /// <param name="packageVersion">an instance of <see cref="PackageVersionInfo"/> object.</param>
        public async Task DownloadAndInstallNewVersionAsync(PackageVersionInfo packageVersion)
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
            switch (extension)
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
                _logger?.LogError($"Error starting command {command}: {ex.ToMinimized()}");
            }
        }

        private async Task PipeOutputAsync(Process process)
        {
            string output = await process.StandardOutput.ReadLineAsync();
            _logger?.LogInformation(output);
        }
    }
}
