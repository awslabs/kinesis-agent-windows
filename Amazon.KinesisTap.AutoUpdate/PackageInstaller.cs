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
using System.Management.Automation;
using System.Management.Automation.Runspaces;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Logging;
using static Amazon.KinesisTap.AutoUpdate.UpdateUtility;

namespace Amazon.KinesisTap.AutoUpdate
{
    public class PackageInstaller : IPackageInstaller
    {
        const string EXT_NUPKG = ".nupkg";
        const string EXT_RPM = ".rpm";
        const string EXT_MSI = ".msi";
        const string EXT_PKG = ".pkg";

        /// <summary>
        /// Set of KinesisTap's verified publishers.
        /// </summary>
        private static readonly ISet<string> _builtInMsiAllowedPublishers = new HashSet<string>
        {
            "Amazon.com Services LLC", "Amazon Web Services, Inc."
        };

        private readonly ILogger _logger;
        private readonly IPlugInContext _context;
        private readonly IMetrics _metrics;
        private readonly ISet<string> _allowedPublishers;
        private readonly IAppDataFileProvider _appDataFileProvider;
        private readonly bool _skipSignatureVerification;

        public PackageInstaller(IPlugInContext context,
            IAppDataFileProvider appDataFileProvider,
            ISet<string> allowedPublishers,
            bool skipSignatureVerification)
        {
            _context = context;
            _logger = context.Logger;
            _allowedPublishers = allowedPublishers;
            _skipSignatureVerification = skipSignatureVerification;
            _logger = context.Logger;
            _metrics = context.Metrics;

            _metrics?.InitializeCounters(string.Empty, MetricsConstants.CATEGORY_PLUGIN, CounterTypeEnum.Increment, new Dictionary<string, MetricValue>
            {
                { $"{UpdateMetricsConstants.Prefix}{UpdateMetricsConstants.PackagesDownloaded}", MetricValue.ZeroCount },
                { $"{UpdateMetricsConstants.Prefix}{UpdateMetricsConstants.PackageSignaturesValid}", MetricValue.ZeroCount },
                { $"{UpdateMetricsConstants.Prefix}{UpdateMetricsConstants.PackageSignaturesInvalid}", MetricValue.ZeroCount },
                { $"{UpdateMetricsConstants.Prefix}{UpdateMetricsConstants.PowershellExecutions}", MetricValue.ZeroCount }
            });
            _appDataFileProvider = appDataFileProvider;
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

            IFileDownloader downloader = CreateDownloaderFromUrl(packageUrl, _context);

            string relativeUpdateDirectory = "update";
            _appDataFileProvider.CreateDirectory(relativeUpdateDirectory);

            string relativeDownloadPath = Path.Combine(relativeUpdateDirectory, Path.GetFileName(packageUrl));
            
            // make sure existing update directory is clean before we download the package.
            if (_appDataFileProvider.FileExists(relativeDownloadPath))
            {
                _appDataFileProvider.DeleteFile(relativeDownloadPath);
            }
            
            await downloader.DownloadFileAsync(packageUrl, relativeDownloadPath);
            
            PublishCounter(UpdateMetricsConstants.PackagesDownloaded, CounterTypeEnum.Increment, 1);

            var absoluteDownloadPath = _appDataFileProvider.GetFullPath(relativeDownloadPath);
            _logger?.LogInformation($"Package downloaded to {absoluteDownloadPath}. Expanding package...");

            if (!_skipSignatureVerification)
            {
                if (EXT_MSI.Equals(extension, StringComparison.Ordinal))
                {
                    if (!await VerifyAuthenticodeSignatureAsync(absoluteDownloadPath))
                    {
                        PublishCounter(UpdateMetricsConstants.PackageSignaturesInvalid, CounterTypeEnum.Increment, 1);
                        _logger.LogWarning("Cannot verify digital signature for package {0}", absoluteDownloadPath);
                        return;
                    }

                    PublishCounter(UpdateMetricsConstants.PackageSignaturesValid, CounterTypeEnum.Increment, 1);
                }
            }
            else
            {
                _logger.LogInformation("Skipping digital signature verification");
            }

            if (EXT_NUPKG.Equals(extension))
            {
                await InstallNugetPackageAsync(absoluteDownloadPath);
            }
            else
            {
                await InstallPackageAsync(absoluteDownloadPath);
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

        /// <summary>
        /// Verify that the authenticode signature of the file is valid, and publisher is included in the allowed list.
        /// </summary>
        /// <param name="filePath">Path to file.</param>
        /// <returns>True iff the signature and publisher is valid.</returns>
        /// <remarks>
        /// This code might throw an exception, however, we won't catch it here.
        /// This function is called within the update cycle which will catch & log any exception that bubbles up.
        /// </remarks>
        internal async Task<bool> VerifyAuthenticodeSignatureAsync(string filePath)
        {
            const string psGetPublisherCommandTemplate =
                "Return (Get-AuthenticodeSignature \"{0}\").SignerCertificate.GetNameInfo([System.Security.Cryptography.X509Certificates.X509NameType]::SimpleName, $false)";

            if (!OperatingSystem.IsWindows())
            {
                throw new PlatformNotSupportedException("Authenticode signature verification is only available on Windows");
            }

            _logger.LogInformation("Verifying authenticode signature of package");

            using var psProcess = new PowerShellProcessInstance(new Version(5, 1), null, null, false);
            using var runspace = RunspaceFactory.CreateOutOfProcessRunspace(new TypeTable(Array.Empty<string>()), psProcess);
            runspace.Open();

            // first we need to get the signature and verify that it's "Valid"
            using var powershell = PowerShell.Create(runspace);
            powershell.AddCommand("Get-AuthenticodeSignature").AddParameter("FilePath", filePath);
            var results = await powershell.InvokeAsync();
            PublishCounter(UpdateMetricsConstants.PowershellExecutions, CounterTypeEnum.Increment, 1);

            if (results.Count == 0)
            {
                return false;
            }
            var signature = results[0];
            var signatureStatus = signature.Properties["Status"]?.Value?.ToString();
            _logger.LogDebug("Signature status: {0}", signatureStatus);
            if (signatureStatus != nameof(SignatureStatus.Valid))
            {
                return false;
            }

            // then we get the publisher of the signing certificate
            var allowedPublishers = _allowedPublishers ?? _builtInMsiAllowedPublishers;
            powershell.AddScript(string.Format(psGetPublisherCommandTemplate, filePath));
            results = await powershell.InvokeAsync();
            PublishCounter(UpdateMetricsConstants.PowershellExecutions, CounterTypeEnum.Increment, 1);

            if (results.Count == 0)
            {
                return false;
            }
            var publisher = results[0].ToString();
            _logger.LogDebug("Signature publisher: {0}", publisher);

            return allowedPublishers.Contains(publisher);
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

        private void PublishCounter(string name, CounterTypeEnum counterType, long count)
            => _metrics?.PublishCounter(string.Empty, MetricsConstants.CATEGORY_PLUGIN, counterType, $"{UpdateMetricsConstants.Prefix}{name}", count, MetricUnit.Count);
    }
}
