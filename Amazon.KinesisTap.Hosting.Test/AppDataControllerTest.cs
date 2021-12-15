using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Test;
using Amazon.KinesisTap.Test.Common;
using Moq;
using Xunit;
using Xunit.Abstractions;

namespace Amazon.KinesisTap.Hosting.Test
{
    [Collection(nameof(AppDataControllerTest))]
    public class AppDataControllerTest : IDisposable
    {
        protected readonly ITestOutputHelper _output;
        protected readonly string _testDir = Path.Combine(TestUtility.GetTestHome(), Guid.NewGuid().ToString());
        protected readonly string _targetDir;

        public AppDataControllerTest(ITestOutputHelper output)
        {
            _output = output;
            _targetDir = Path.Combine(_testDir, "target");
            Directory.CreateDirectory(_targetDir);
            NLog.LogManager.LoadConfiguration("NLog.xml");
        }

        public void Dispose()
        {
            if (Directory.Exists(_testDir))
            {
                Directory.Delete(_testDir, true);
            }
        }

        [WindowsOnlyFact]
        public async Task JunctionNotExist_ShouldNotDisableWrite()
        {
            // setup
            var mockFileProvider = new Mock<IAppDataFileProvider>(MockBehavior.Loose);
            mockFileProvider.Setup(p => p.DisableWrite()).Verifiable();
            var controller = new AppDataController(_targetDir, TimeSpan.FromMilliseconds(100), mockFileProvider.Object);

            // make sure DisableWrite is not called in constructor
            mockFileProvider.Verify(p => p.DisableWrite(), Times.Never);

            // start the controller routine and wait for some time
            await RunControllerWithActionAsync(controller, 1000, 0, () => { });

            // make sure DisableWrite is not called during runtime
            mockFileProvider.Verify(p => p.DisableWrite(), Times.Never);
        }

        [WindowsOnlyFact]
        public async Task SymlinkExist_ShouldNotDisableWrite()
        {
            // create symlink
            var symlinkDir = Path.Combine(_testDir, "symlink");
            TestUtility.RunWindowsCommand($"mklink /D \"{symlinkDir}\" \"{_targetDir}\"", _output);

            // setup
            var mockFileProvider = new Mock<IAppDataFileProvider>(MockBehavior.Loose);
            mockFileProvider.Setup(p => p.DisableWrite()).Verifiable();
            var controller = new AppDataController(symlinkDir, TimeSpan.FromMilliseconds(100), mockFileProvider.Object);

            // make sure DisableWrite is not called in constructor
            mockFileProvider.Verify(p => p.DisableWrite(), Times.Never);

            // start the controller routine and wait for some time
            await RunControllerWithActionAsync(controller, 1000, 0, () => { });

            // make sure DisableWrite is not called during runtime
            mockFileProvider.Verify(p => p.DisableWrite(), Times.Never);
        }

        [WindowsOnlyFact]
        public void JunctionExistsAtStartup_ShouldDisableWrite()
        {
            // create the junction
            var junction = Path.Combine(_testDir, "junction");
            TestUtility.RunWindowsCommand($"mklink /j \"{junction}\" \"{_targetDir}\"", _output);

            try
            {
                // setup
                var mockFileProvider = new Mock<IAppDataFileProvider>(MockBehavior.Loose);
                mockFileProvider.Setup(p => p.DisableWrite()).Verifiable();
                var controller = new AppDataController(junction, TimeSpan.FromMilliseconds(1000), mockFileProvider.Object);

                // make sure DisableWrite is called
                mockFileProvider.Verify(p => p.DisableWrite(), Times.Once);
            }
            finally
            {
                // delete the junction first to make sure clean-up doesn't fail
                Directory.Delete(junction);
            }
        }

        [WindowsOnlyFact]
        public void JunctionExistInParentOnStartup_ShouldDisableWrite()
        {
            // create the junction
            var junction = Path.Combine(_testDir, "junction");
            TestUtility.RunWindowsCommand($"mklink /j \"{junction}\" \"{_targetDir}\"", _output);

            try
            {
                // create the data directory as subdirectory
                var dataDir = Path.Combine(junction, "data");
                Directory.CreateDirectory(dataDir);

                // setup
                var mockFileProvider = new Mock<IAppDataFileProvider>(MockBehavior.Loose);
                mockFileProvider.Setup(p => p.DisableWrite()).Verifiable();
                var controller = new AppDataController(dataDir, TimeSpan.FromMilliseconds(1000), mockFileProvider.Object);

                // make sure DisableWrite is called
                mockFileProvider.Verify(p => p.DisableWrite(), Times.Once);
            }
            finally
            {
                // delete the junction first to make sure clean-up doesn't fail
                Directory.Delete(junction);
            }
        }

        [WindowsOnlyTheory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(10)]
        public void JunctionExistsInSubDirOnStartup_ShouldDisableWrite(int depth)
        {
            // create data dir
            var dataDir = Path.Combine(_testDir, "data");
            Directory.CreateDirectory(dataDir);

            // create the junction as sub-directory
            var junctionPath = dataDir;
            for (var i = 0; i < depth; i++)
            {
                junctionPath = Path.Combine(junctionPath, $"sub{i}");
                Directory.CreateDirectory(junctionPath);
            }
            var junction = Path.Combine(junctionPath, "junction");
            TestUtility.RunWindowsCommand($"mklink /j \"{junction}\" \"{_targetDir}\"", _output);

            try
            {
                // setup
                var mockFileProvider = new Mock<IAppDataFileProvider>(MockBehavior.Loose);
                mockFileProvider.Setup(p => p.DisableWrite()).Verifiable();
                var controller = new AppDataController(dataDir, TimeSpan.FromMilliseconds(1000), mockFileProvider.Object);

                // make sure DisableWrite is called
                mockFileProvider.Verify(p => p.DisableWrite(), Times.Once);
            }
            finally
            {
                // delete the junction first to make sure clean-up doesn't fail
                Directory.Delete(junction);
            }
        }

        [WindowsOnlyFact]
        public async Task JunctionExistsAtRuntime_ShouldDisableWrite()
        {
            var junction = Path.Combine(_testDir, "junction");

            // setup
            var mockFileProvider = new Mock<IAppDataFileProvider>(MockBehavior.Loose);
            mockFileProvider.Setup(p => p.DisableWrite()).Verifiable();
            var controller = new AppDataController(junction, TimeSpan.FromMilliseconds(1000), mockFileProvider.Object);

            await RunControllerWithActionAsync(controller, 500, 500,
                () => TestUtility.RunWindowsCommand($"mklink /j \"{junction}\" \"{_targetDir}\"", _output));

            try
            {
                // make sure DisableWrite is called
                mockFileProvider.Verify(p => p.DisableWrite(), Times.AtLeastOnce);
            }
            finally
            {
                // delete the junction first to make sure clean-up doesn't fail
                Directory.Delete(junction);
            }
        }

        [WindowsOnlyFact]
        public async Task JunctionExistsInParentAtRuntime_ShouldDisableWrite()
        {
            var junction = Path.Combine(_testDir, "junction");
            var dataDir = Path.Combine(junction, "data");

            // setup
            var mockFileProvider = new Mock<IAppDataFileProvider>(MockBehavior.Loose);
            mockFileProvider.Setup(p => p.DisableWrite()).Verifiable();
            var controller = new AppDataController(dataDir, TimeSpan.FromMilliseconds(1000), mockFileProvider.Object);

            await RunControllerWithActionAsync(controller, 500, 500,
                () =>
                {
                    // create the junction
                    TestUtility.RunWindowsCommand($"mklink /j \"{junction}\" \"{_targetDir}\"", _output);
                    // create the data directory
                    Directory.CreateDirectory(dataDir);
                });

            try
            {
                // make sure DisableWrite is called
                mockFileProvider.Verify(p => p.DisableWrite(), Times.AtLeastOnce);
            }
            finally
            {
                // delete the junction first to make sure clean-up doesn't fail
                Directory.Delete(junction);
            }
        }

        [WindowsOnlyTheory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(10)]
        public async Task JunctionExistInSubDirAtRuntime_ShouldDisableWrite(int depth)
        {
            // create data dir
            var dataDir = Path.Combine(_testDir, "data");
            Directory.CreateDirectory(dataDir);

            // setup
            var mockFileProvider = new Mock<IAppDataFileProvider>(MockBehavior.Loose);
            mockFileProvider.Setup(p => p.DisableWrite()).Verifiable();
            var controller = new AppDataController(dataDir, TimeSpan.FromMilliseconds(1000), mockFileProvider.Object);

            var junction = string.Empty;

            await RunControllerWithActionAsync(controller, 500, 500,
                () =>
                {
                    // create the junction as sub-directory
                    var junctionPath = dataDir;
                    for (var i = 0; i < depth; i++)
                    {
                        junctionPath = Path.Combine(junctionPath, $"sub{i}");
                        Directory.CreateDirectory(junctionPath);
                    }
                    junction = Path.Combine(junctionPath, "junction");
                    TestUtility.RunWindowsCommand($"mklink /j \"{junction}\" \"{_targetDir}\"", _output);
                });

            try
            {
                // make sure DisableWrite is called
                mockFileProvider.Verify(p => p.DisableWrite(), Times.AtLeastOnce);
            }
            finally
            {
                // delete the junction first to make sure clean-up doesn't fail
                Directory.Delete(junction);
            }
        }

        /// <summary>
        /// Start the controller, wait for <paramref name="period1Ms"/>, execute the action, wait for <paramref name="period2Ms"/>, then stop controller.
        /// </summary>
        private static async Task RunControllerWithActionAsync(AppDataController controller, int period1Ms, int period2Ms, Action action)
        {
            using var cts = new CancellationTokenSource();
            await controller.StartAsync(cts.Token);
            await Task.Delay(period1Ms);

            action.Invoke();
            await Task.Delay(period2Ms);

            cts.Cancel();
            await controller.StopAsync(default);
        }
    }
}
