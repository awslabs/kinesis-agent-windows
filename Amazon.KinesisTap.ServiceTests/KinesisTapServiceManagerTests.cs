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
namespace Amazon.KinesisTap.ServiceTests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Threading;
    using Amazon.KinesisTap.Core;
    using Amazon.KinesisTap.Core.Test;
    using Microsoft.Extensions.Logging;
    using Moq;
    using Xunit;

    public class KinesisTapServiceManagerTests : IDisposable
    {
        public KinesisTapServiceManagerTests()
        {
            ProgramInfo.KinesisTapPath = Path.Combine(AppContext.BaseDirectory, ConfigConstants.KINESISTAP_EXE_NAME);
        }

        /// <summary>
        /// This test verifies simple stop/start behavior, with no special conditions.
        /// </summary>
        [Fact]
        public void SimpleStartStop()
        {
            var service = this.Setup(out var repo, out var logger, out _, out _);
            service.Start();
            using (service.StartCompleted)
                Assert.True(service.StartCompleted.Wait(KinesisTapServiceManager.MaximumServiceOperationDuration));

            service.Stop();
            using (service.StopCompleted)
                Assert.True(service.StopCompleted.Wait(KinesisTapServiceManager.MaximumServiceOperationDuration));

            Assert.NotEmpty(logger.Entries);
            repo.Verify();
        }

        /// <summary>
        /// This test verifies stop behavior where a source that takes longer than 5 seconds to stop
        /// does not hold up the entire stop operation (i.e. LogManager moves on to stop other components).
        /// The source Mock will throw an error after 10 seconds, but since we're not waiting that long
        /// for it to be thrown, the test will never encounter that error.
        /// </summary>
        [Fact]
        public void SourceStopTimedOut()
        {
            var service = this.Setup(out var repo, out var logger, out _, out var source, setupSource: false);
            source.Setup(i => i.Start())
                .Callback(() => logger.LogDebug("MockSource started"))
                .Verifiable();
            source.Setup(i => i.Stop())
                .Callback(() =>
                {
                    logger.LogDebug("MockSource stopped");
                    Thread.Sleep(TimeSpan.FromSeconds(10));
                    throw new Exception("This shouldn't do anything since LogManager is not waiting for this task to fully complete.");
                })
                .Verifiable();

            service.Start();
            using (service.StartCompleted)
                Assert.True(service.StartCompleted.Wait(KinesisTapServiceManager.MaximumServiceOperationDuration));

            var stopwatch = new Stopwatch();
            stopwatch.Start();
            service.Stop();
            using (service.StopCompleted)
                Assert.True(service.StopCompleted.Wait(KinesisTapServiceManager.MaximumServiceOperationDuration));

            Assert.True(stopwatch.Elapsed.TotalSeconds < 5);
            Assert.NotEmpty(logger.Entries);
            repo.Verify();
        }

        /// <summary>
        /// This test verifies stop behavior where a source that takes longer than 5 seconds to stop
        /// does not hold up the entire stop operation (i.e. LogManager moves on to stop other components).
        /// The source Mock will throw an error after 10 seconds, but since we're not waiting that long
        /// for it to be thrown, the test will never encounter that error.
        /// </summary>
        [Fact]
        public void SourceStartTimedOut()
        {
            var tooLong = KinesisTapServiceManager.MaximumServiceOperationDuration.Add(TimeSpan.FromSeconds(2));
            var service = this.Setup(out var repo, out var logger, out _, out var source, setupSource: false);
            source.Setup(i => i.Start())
                .Callback(() =>
                {
                    logger.LogDebug("MockSource started");
                    Thread.Sleep(tooLong);

                    // LogManager will log and swallow errors that occur when calling the Start() method of sources and sinks.
                    // We will throw an error here to make sure that an error thrown during Start() does not cause the service to crash.
                    throw new Exception("Some Startup Error");
                })
                .Verifiable();
            source.Setup(i => i.Stop())
                .Verifiable();

            var stopwatch = new Stopwatch();
            service.Start();
            using (service.StartCompleted)
                Assert.True(service.StartCompleted.Wait(KinesisTapServiceManager.MaximumServiceOperationDuration));

            Assert.True(stopwatch.Elapsed < tooLong);

            service.Stop();
            using (service.StopCompleted)
                Assert.True(service.StopCompleted.Wait(KinesisTapServiceManager.MaximumServiceOperationDuration));

            Assert.NotEmpty(logger.Entries);
            Assert.Contains($"{KinesisTapServiceManager.ServiceName} took longer than {KinesisTapServiceManager.MaximumServiceOperationDuration} to start.", logger.Entries);
            repo.Verify();
        }

        /// <summary>
        /// This test verifies stop behavior where a sink that takes longer than 30 seconds to stop does 
        /// not prevent the service from shutting down (thus triggering an error in the Service Control Manager).
        /// The sink Mock will throw an error after 30 seconds, but since we're not waiting that long
        /// for it to be thrown, the test will never encounter that error.
        /// </summary>
        [Fact]
        public void SinkStopTimedOut()
        {
            var tooLong = KinesisTapServiceManager.MaximumServiceOperationDuration.Add(TimeSpan.FromSeconds(2));
            var service = this.Setup(out var repo, out var logger, out var sink, out _, setupSink: false);
            sink.Setup(i => i.Start())
                .Callback(() => logger.LogDebug("MockSink started"))
                .Verifiable();
            sink.Setup(i => i.Stop())
                .Callback(() =>
                {
                    logger.LogDebug("MockSink stopped");
                    Thread.Sleep(tooLong);
                    throw new Exception("This shouldn't do anything since the parent class shouldn't have waited long enough for this to happen.");
                })
                .Verifiable();

            service.Start();
            using (service.StartCompleted)
                Assert.True(service.StartCompleted.Wait(KinesisTapServiceManager.MaximumServiceOperationDuration));

            var stopwatch = new Stopwatch();
            stopwatch.Start();
            service.Stop();
            using (service.StopCompleted)
                Assert.True(service.StopCompleted.Wait(KinesisTapServiceManager.MaximumServiceOperationDuration));

            Assert.True(stopwatch.Elapsed > KinesisTapServiceManager.MaximumServiceOperationDuration);
            Assert.True(stopwatch.Elapsed < tooLong);

            Assert.NotEmpty(logger.Entries);

            // Verify that a log message was written when the sink failed to stop within the acceptable timeout.
            Assert.Contains($"{KinesisTapServiceManager.ServiceName} could not shut down all components within the maximum service stop interval.", logger.Entries);
            repo.Verify();
        }

        private KinesisTapServiceManager Setup(out MockRepository repo, out MemoryLogger memLogger, out Mock<IEventSink> sink, out Mock<IEventSource<string>> source, bool setupSink = true, bool setupSource = true)
        {
            var logger = new MemoryLogger("UnitTest");
            repo = new MockRepository(MockBehavior.Loose);

            sink = repo.Create<IEventSink>();
            sink.SetupProperty(i => i.Id, "MockSink");

            // If setupSink is true, set up default Start and Stop implementations.
            if (setupSink)
            {
                sink.Setup(i => i.Start())
                    .Callback(() => logger.LogDebug("MockSink started"))
                    .Verifiable();
                sink.Setup(i => i.Stop())
                    .Callback(() => logger.LogDebug("MockSink stopped"))
                    .Verifiable();
            }

            source = repo.Create<IEventSource<string>>();
            source.SetupProperty(i => i.Id, "MockSource");

            // If setupSource is true, set up default Start and Stop implementations.
            if (setupSource)
            {
                source.Setup(i => i.Start())
                    .Callback(() => logger.LogDebug("MockSource started"))
                    .Verifiable();
                source.Setup(i => i.Stop())
                    .Callback(() => logger.LogDebug("MockSource stopped"))
                    .Verifiable();
            }

            var sourceFactory = repo.Create<IFactory<ISource>>();
            sourceFactory.Setup(i => i.CreateInstance(It.IsAny<string>(), It.IsAny<IPlugInContext>()))
                .Returns(source.Object)
                .Verifiable();
            sourceFactory.Setup(i => i.RegisterFactory(It.IsAny<IFactoryCatalog<ISource>>()))
                .Callback((IFactoryCatalog<ISource> catalog) => catalog.RegisterFactory("Mock", sourceFactory.Object))
                .Verifiable();

            var sinkFactory = repo.Create<IFactory<IEventSink>>();
            sinkFactory.Setup(i => i.CreateInstance(It.IsAny<string>(), It.IsAny<IPlugInContext>()))
                .Returns(sink.Object)
                .Verifiable();
            sinkFactory.Setup(i => i.RegisterFactory(It.IsAny<IFactoryCatalog<IEventSink>>()))
                .Callback((IFactoryCatalog<IEventSink> catalog) => catalog.RegisterFactory("Mock", sinkFactory.Object))
                .Verifiable();

            var parStore = repo.Create<IParameterStore>();
            var typeLoader = repo.Create<ITypeLoader>();
            typeLoader.Setup(i => i.LoadTypes<IFactory<IEventSink>>())
                .Returns(new List<IFactory<IEventSink>> { sinkFactory.Object })
                .Verifiable();

            typeLoader.Setup(i => i.LoadTypes<IFactory<ISource>>())
                .Returns(new List<IFactory<ISource>> { sourceFactory.Object })
                .Verifiable();

            memLogger = logger;

            // Set the ConfigConstants.KINESISTAP_CONFIG_PATH environment variable to null.
            // If any other test has set this variable, these tests will fail.
            Environment.SetEnvironmentVariable(ConfigConstants.KINESISTAP_CONFIG_PATH, null);
            return new KinesisTapServiceManager(typeLoader.Object, parStore.Object, logger);
        }

        public void Dispose()
        {
            ProgramInfo.KinesisTapPath = null;
        }
    }
}
