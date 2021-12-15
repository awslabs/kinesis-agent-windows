using System;
using System.IO;
using Amazon.KinesisTap.Core.Test;
using Amazon.KinesisTap.Test.Common;
using NLog.Targets;
using Xunit;
using Xunit.Abstractions;

namespace Amazon.KinesisTap.Hosting.Test
{
    [Collection(nameof(FileSystemUtilityTest))]
    public class FileSystemUtilityTest : IDisposable
    {
#pragma warning disable CA1416 // Validate platform compatibility
        protected readonly ITestOutputHelper _output;
        protected readonly string _testDir = Path.Combine(TestUtility.GetTestHome(), Guid.NewGuid().ToString());

        public FileSystemUtilityTest(ITestOutputHelper output)
        {
            _output = output;
            if (Directory.Exists(_testDir))
            {
                Directory.Delete(_testDir, true);
            }
        }

        public void Dispose()
        {
            if (Directory.Exists(_testDir))
            {
                Directory.Delete(_testDir, true);
            }
        }

        [WindowsOnlyFact]
        public void NormalDirectory_ShouldReturnNoReparsePoint()
        {
            // create test directory
            var dir = Path.Combine(_testDir, "normal");
            Directory.CreateDirectory(dir);

            // Get the reparse type
            var type = FileSystemUtility.GetDirectoryReparseType(dir);

            // assert that it's 'None'
            Assert.Equal(FileSystemUtility.DirectoryReparseType.None, type);
        }

        [WindowsOnlyFact]
        public void Symlink_ShouldReturnSymlinkReparsePoint()
        {
            // create target directory
            var target = Path.Combine(_testDir, "target");
            Directory.CreateDirectory(target);

            // create symlink directory
            var symlink = Path.Combine(_testDir, "symlink");
            TestUtility.RunWindowsCommand($"mklink /D \"{symlink}\" \"{target}\"", _output);

            // Get the reparse type and assert it's 'Symlink'
            var type = FileSystemUtility.GetDirectoryReparseType(symlink);
            Assert.Equal(FileSystemUtility.DirectoryReparseType.Symlink, type);
        }

        [WindowsOnlyFact]
        public void Junction_ShouldReturnJunctionReparsePoint()
        {
            // create target directory
            var target = Path.Combine(_testDir, "target");
            Directory.CreateDirectory(target);

            // create junction directory
            var junction = Path.Combine(_testDir, "junction");
            TestUtility.RunWindowsCommand($"mklink /j \"{junction}\" \"{target}\"", _output);

            try
            {
                // Get the reparse type and assert it's 'MountPoint'
                var type = FileSystemUtility.GetDirectoryReparseType(junction);
                Assert.Equal(FileSystemUtility.DirectoryReparseType.MountPoint, type);
            }
            finally
            {
                // delete the junction first to make sure clean-up doesn't fail
                Directory.Delete(junction);
            }
        }

        [WindowsOnlyFact]
        public void NoDir_ShouldThrowDirectoryNotFoundException()
        {
            var dir = Path.Combine(_testDir, "nothing");
            Assert.Throws<DirectoryNotFoundException>(() => FileSystemUtility.GetDirectoryReparseType(dir));
        }

        [Fact]
        public void DisableNLogTargets_TargetsShouldBeDisabled()
        {
            // setup logging configuration with some targets
            var nlogConfig = new NLog.Config.LoggingConfiguration();
            nlogConfig.AddTarget(new ConsoleTarget("console"));
            nlogConfig.AddTarget(new FileTarget("file"));
            foreach (var target in nlogConfig.AllTargets)
            {
                nlogConfig.AddRuleForAllLevels(target.Name, "*");
            }

            // call method
            FileSystemUtility.DisableAllNLogTargets(nlogConfig);

            // assert that all rules are disabled
            foreach (var rule in nlogConfig.LoggingRules)
            {
                foreach (var level in NLog.LogLevel.AllLevels)
                {
                    Assert.False(rule.IsLoggingEnabledForLevel(level));
                }
            }
        }

#pragma warning restore CA1416 // Validate platform compatibility
    }
}
