using System;
using System.IO;
using System.Runtime.Versioning;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging.Abstractions;

namespace Amazon.KinesisTap.Hosting
{
    /// <summary>
    /// Class used to control access to application data.
    /// </summary>
    internal class AppDataController : AsyncTimingPlugin
    {
        private readonly string _appDataDirectory;
        private readonly IAppDataFileProvider _appDataFileProvider;

        public AppDataController(string appDataDirDirectory, TimeSpan interval, IAppDataFileProvider appDataFileProvider)
            : base(nameof(AppDataController), (int)interval.TotalMilliseconds, true, NullLogger.Instance)
        {
            _appDataDirectory = appDataDirDirectory;
            _appDataFileProvider = appDataFileProvider;

            // execute this detection on start-up
            ExecuteInternal();
        }

        protected override ValueTask ExecuteActionAsync(CancellationToken stopToken)
        {
            ExecuteInternal();
            return default;
        }

        private void ExecuteInternal()
        {
            if (!DetectSensitiveJunction())
            {
                // we don't have anything to do here
                return;
            }

            // sensitive junction detected, first we need to disable logging
            FileSystemUtility.DisableAllNLogTargets(NLog.LogManager.Configuration);

            // disable writes to AppData folder
            _appDataFileProvider.DisableWrite();

            return;
        }

        /// <summary>
        /// Returns true iff junction in sensitive directories is detected.
        /// </summary>
        private bool DetectSensitiveJunction()
        {
            if (!OperatingSystem.IsWindows())
            {
                // junction is a Windows specific thing
                return false;
            }

            return CheckForJunctionInSelfAndParents(_appDataDirectory) || CheckForJunctionSubdirectories(_appDataDirectory);
        }

        /// <summary>
        /// Check if a directory or any of its parents is a junction.
        /// </summary>
        [SupportedOSPlatform("windows")]
        private bool CheckForJunctionInSelfAndParents(string directory)
        {
            var parent = Path.GetDirectoryName(directory);
            if (string.IsNullOrEmpty(parent))
            {
                // this means we are at a root (drive-level) directory, stop checking 
                return false;
            }

            try
            {
                if (FileSystemUtility.GetDirectoryReparseType(directory) == FileSystemUtility.DirectoryReparseType.MountPoint)
                {
                    return true;
                }
            }
            catch (DirectoryNotFoundException)
            {
                return false;
            }

            return CheckForJunctionInSelfAndParents(parent);
        }

        /// <summary>
        /// Detect junction sub-directories of a directory.
        /// </summary>
        [SupportedOSPlatform("windows")]
        private bool CheckForJunctionSubdirectories(string directory)
        {
            if (!Directory.Exists(directory))
            {
                return false;
            }

            // check for all sub-directories
            var subDirs = Directory.GetDirectories(directory, "*", new EnumerationOptions
            {
                IgnoreInaccessible = true,
                RecurseSubdirectories = true
            });

            foreach (var subDir in subDirs)
            {
                try
                {
                    if (FileSystemUtility.GetDirectoryReparseType(subDir) == FileSystemUtility.DirectoryReparseType.MountPoint)
                    {
                        return true;
                    }
                }
                catch (DirectoryNotFoundException)
                {
                }
            }

            return false;
        }
    }
}
