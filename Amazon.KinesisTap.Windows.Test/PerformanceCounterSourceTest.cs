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
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Amazon.KinesisTap.Core.Test;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Amazon.KinesisTap.Windows.Test
{
    public class PerformanceCounterSourceTest
    {
        private readonly BookmarkManager _bookmarkManager = new BookmarkManager();

        [Fact]
        public void TestUnitInference()
        {
            Assert.Equal(MetricUnit.Percent, PerformanceCounterSource.InferUnit(null, "% Total Run Time"));
            Assert.Equal(MetricUnit.Percent, PerformanceCounterSource.InferUnit(null, "Data Map Hits %"));
            Assert.Equal(MetricUnit.Percent, PerformanceCounterSource.InferUnit(null, "Slow Tier Destaged Container Fill Ratio (%)"));

            Assert.Equal(MetricUnit.CountSecond, PerformanceCounterSource.InferUnit(null, "Page Table Evictions/sec"));
            Assert.Equal(MetricUnit.CountSecond, PerformanceCounterSource.InferUnit(null, "Requests / Sec"));
            Assert.Equal(MetricUnit.BytesSecond, PerformanceCounterSource.InferUnit(null, "Bytes Received/sec"));
            Assert.Equal(MetricUnit.BytesSecond, PerformanceCounterSource.InferUnit(null, "IL Bytes Jitted / sec"));

            Assert.Equal(MetricUnit.Bytes, PerformanceCounterSource.InferUnit(null, "Bytes Received By Disconnected Clients"));
            Assert.Equal(MetricUnit.Bytes, PerformanceCounterSource.InferUnit(null, "In - Total bytes received"));
            Assert.Equal(MetricUnit.Megabytes, PerformanceCounterSource.InferUnit(null, "Total MBytes"));
            Assert.Equal(MetricUnit.Kilobytes, PerformanceCounterSource.InferUnit(null, " Available KBytes"));

            Assert.Equal(MetricUnit.Seconds, PerformanceCounterSource.InferUnit(null, "Duration - Duration of the session (Seconds)"));
            Assert.Equal(MetricUnit.Seconds, PerformanceCounterSource.InferUnit(null, "Avg. sec/Request"));
            Assert.Equal(MetricUnit.Milliseconds, PerformanceCounterSource.InferUnit(null, "File lock acquire average milliseconds"));
            Assert.Equal(MetricUnit.Milliseconds, PerformanceCounterSource.InferUnit(null, "I/O Database Reads (Attached) Average Latency"));
            Assert.Equal(MetricUnit.HundredNanoseconds, PerformanceCounterSource.InferUnit(null, "Slow tier destage read latency (100 ns)"));
            //This should be count per second
            Assert.Equal(MetricUnit.CountSecond, PerformanceCounterSource.InferUnit(null, "I/O Database Reads (Recovery) Abnormal Latency/sec"));

            Assert.Equal(MetricUnit.Count, PerformanceCounterSource.InferUnit(null, "Monitored Notifications"));
        }

        [Fact]
        public void TestConfiguration()
        {
            var categoriesSection = TestUtility.GetConfig("Sources", "PerformanceCounter").GetSection("Categories");
            var counterUnitsCache = new Dictionary<(string, string), MetricUnit>();
            var performanceCounterSourceLoader = new PerformanceCounterSourceConfigLoader(
                new PluginContext(null, NullLogger.Instance, null, _bookmarkManager),
                counterUnitsCache);
            var categories = performanceCounterSourceLoader.LoadCategoriesConfig(categoriesSection);
            Assert.Equal(5, categories.Count);
            Assert.Single(counterUnitsCache);
            Assert.Equal(MetricUnit.CountSecond, counterUnitsCache.Values.First());
        }

        /// <summary>
        /// Load a config with multiple sections with same "Category" (multi-instance) but different "Counters"
        /// </summary>
        [Fact]
        public void TestLoadNonExistingCategory()
        {
            var config = TestUtility.GetConfig("Sources", "NonExistingCategory");
            var performanceCounterSource = new PerformanceCounterSource(new PluginContext(config, NullLogger.Instance, null));
            performanceCounterSource.Start();
            var results = performanceCounterSource.Query(null);
            var metrics = results.Data;

            Assert.Equal(1, metrics.Count);
            performanceCounterSource.Stop();
        }

        /// <summary>
        /// Load a config with multiple sections with same "Category" (multi-instance) but different "Counters"
        /// </summary>
        [Fact]
        public void TestMultiInstanceCounters_RepeatedCategory()
        {
            var config = TestUtility.GetConfig("Sources", "ProcessorCounter");
            var performanceCounterSource = new PerformanceCounterSource(new PluginContext(config, NullLogger.Instance, null));
            performanceCounterSource.Start();
            var results = performanceCounterSource.Query(null);
            var metrics = results.Data;

            // 'ProcessorCounter' source has 5 counters for each processor
            // the number of instances is #processor + 1 to account for the "_Total" instance
            Assert.Equal(5 * (Environment.ProcessorCount + 1), metrics.Count);
            performanceCounterSource.Stop();
        }

        /// <summary>
        /// Load a config with multiple sections with same "Category"(single-instance) but different "Counters"
        /// </summary>
        [Fact]
        public void TestSingleInstanceCounters_RepeatedCategory()
        {
            var config = TestUtility.GetConfig("Sources", "SystemCounter");
            var performanceCounterSource = new PerformanceCounterSource(new PluginContext(config, NullLogger.Instance, null));
            performanceCounterSource.Start();
            var results = performanceCounterSource.Query(null);
            var metrics = results.Data;
            Assert.Equal(3, metrics.Count);
            performanceCounterSource.Stop();
        }


        /// <summary>
        /// The first section contains processor instance '0'.
        /// The second section contains processor instances '0' and '_Total'.
        /// </summary>
        [Fact]
        public void TestAdditionalCounters_SameInstance()
        {
            var config = TestUtility.GetConfig("Sources", "FirstAndAllProcessorCounter");
            var performanceCounterSource = new PerformanceCounterSource(new PluginContext(config, NullLogger.Instance, null));
            performanceCounterSource.Start();
            var results = performanceCounterSource.Query(null);

            // Assert that instance '0' has 2 counters
            Assert.Equal(2, results.Data.Count(m => m.Key.Id == "0"));

            // Assert that instance '_Total' has 1 counter
            Assert.Equal(1, results.Data.Count(m => m.Key.Id == "_Total"));
            performanceCounterSource.Stop();
        }

        /// <summary>
        /// Load a config with a non-existing and an existing counter
        /// </summary>
        [Fact]
        public void TestLoadNonExistingCounters()
        {
            var config = TestUtility.GetConfig("Sources", "NonExistingCounter");
            var performanceCounterSource = new PerformanceCounterSource(new PluginContext(config, NullLogger.Instance, null));
            performanceCounterSource.Start();
            var results = performanceCounterSource.Query(null);

            // Assert that instance '0' has only 1 counter
            Assert.Equal(1, results.Data.Count);
            performanceCounterSource.Stop();
        }

        /// <summary>
        /// Load a config with a non-existing and an existing counter
        /// </summary>
        [Fact]
        public void TestLoadNonExistingInstances()
        {
            var config = TestUtility.GetConfig("Sources", "NonExistingInstance");
            var performanceCounterSource = new PerformanceCounterSource(new PluginContext(config, NullLogger.Instance, null));
            performanceCounterSource.Start();
            var results = performanceCounterSource.Query(null);

            Assert.Equal(2, results.Data.Count);

            // Assert that instance 'a' has no counters
            Assert.Equal(0, results.Data.Count(m => m.Key.Id == "a"));

            // Assert that instance '0' has 2 counters
            Assert.Equal(2, results.Data.Count(m => m.Key.Id == "0"));
            performanceCounterSource.Stop();
        }

        /// <summary>
        /// Load a config with both 'Instances' and 'InstanceRegex', but InstanceRegex contains an instance set in 'Instances'.
        /// There are 2 configs we test this with: one with 'Instances' and 'InstanceRegex' in the same section,
        /// the other where they are in separate sections.
        /// </summary>
        [Theory]
        [InlineData("ProcessorCountersWithDuplicatedRegex")]
        [InlineData("ProcessorCountersWithDuplicatedRegexInAnotherCategory")]
        public void TestDuplicatedInstanceInRegex(string sourceName)
        {
            var config = TestUtility.GetConfig("Sources", sourceName);
            var performanceCounterSource = new PerformanceCounterSource(new PluginContext(config, NullLogger.Instance, null));
            performanceCounterSource.Start();
            var results = performanceCounterSource.Query(null);

            // Assert the number of data points = #processor
            Assert.Equal(Environment.ProcessorCount, results.Data.Count);

            // Assert single counter for processor '0'
            Assert.Equal(1, results.Data.Count(m => m.Key.Id == "0"));

            performanceCounterSource.Stop();
        }

        [Fact]
        public void TestCombinedCounters_DuplicateCounters()
        {
            var config = TestUtility.GetConfig("Sources", "SystemAndProcessorCounter");
            var performanceCounterSource = new PerformanceCounterSource(new PluginContext(config, NullLogger.Instance, null));
            performanceCounterSource.Start();
            var results = performanceCounterSource.Query(null);
            var metrics = results.Data;

            Assert.Equal(3 * (Environment.ProcessorCount + 1) + 2, metrics.Count);
            performanceCounterSource.Stop();
        }

        [Fact]
        public void TestPerformanceCounterSource()
        {
            var config = TestUtility.GetConfig("Sources", "PerformanceCounter");
            var performanceCounterSource = new PerformanceCounterSource(new PluginContext(config, NullLogger.Instance, null, _bookmarkManager));
            performanceCounterSource.Start();
            var results = performanceCounterSource.Query(null);
            var metrics = results.Data;
            Assert.True(metrics.Count > 0);
            Assert.Contains(metrics, m => m.Value.Value > 0);
            performanceCounterSource.Stop();
        }

        [Fact]
        [Trait("Category", "Integration")]
        public void TestPerformanceCounterSourceWithInstanceRegex()
        {
            var config = TestUtility.GetConfig("Sources", "PerformanceCounterWithInstanceRegex");
            var performanceCounterSource = new PerformanceCounterSource(new PluginContext(config, NullLogger.Instance, null, _bookmarkManager));
            performanceCounterSource.Start();
            var results = performanceCounterSource.Query(null);
            var metrics = results.Data as ICollection<KeyValuePair<MetricKey, MetricValue>>;
            Assert.True(metrics.Count > 0);
            Regex instanceRegex = new Regex("^Local Area Connection\\* \\d$");
            Assert.All(metrics, m => Assert.Matches(instanceRegex, m.Key.Id));
            performanceCounterSource.Stop();
        }


        [Fact]
        [Trait("Category", "Integration")]
        public void TestPerformanceCounterSourceForTransientInstance()
        {
            var config = TestUtility.GetConfig("Sources", "PerformanceCounter");
            var performanceCounterSource = new PerformanceCounterSource(new PluginContext(config, NullLogger.Instance, null, _bookmarkManager));
            performanceCounterSource.Start();
            var results = performanceCounterSource.Query(null);
            var metrics = results.Data as ICollection<KeyValuePair<MetricKey, MetricValue>>;
            Assert.True(metrics.Count > 0);
            Assert.Contains(metrics, m => m.Value.Value > 0);

            Process p = Process.Start("notepad.exe"); // Start a new process by running the notepad.exe

            // Query the process again
            var newResults = performanceCounterSource.Query(null);
            var newMetrics = newResults.Data as ICollection<KeyValuePair<MetricKey, MetricValue>>;

            // Find the different processes between the old process list and new process list
            var diff = newMetrics.Where(n => !metrics.Any(m => m.Key.Id == n.Key.Id)).ToList();

            // If the difference contains the new process, return true
            Assert.True(diff.Where(m => m.Key.Category.Equals("Process") && m.Key.Id.Contains("notepad")).ToList().Count > 0);

            // Kill the process
            p.Kill();

            performanceCounterSource.Stop();
        }

        /// <summary>
        /// Tests the race condition of <see cref="PerformanceCounterSource"/> when the sink is querying while another thread is calling Stop().
        /// </summary>
        [Fact]
        [Trait("Category", "Integration")]
        public void TestPerformanceCounterSourceSafeStop()
        {
            var logger = new MemoryLogger(nameof(TestPerformanceCounterSourceSafeStop));
            var config = TestUtility.GetConfig("Sources", "PerformanceCounter");
            var mockCounterSource = new Mock<PerformanceCounterSource>(MockBehavior.Strict, new PluginContext(config, logger, null, _bookmarkManager));

            // in real deployment, RefreshInstances() might take a long time if there are a lot of counter instances.
            // so we mock this method to simulate the situation
            mockCounterSource.Setup(s => s.RefreshCounters()).Callback(() => Thread.Sleep(1000));
            var source = mockCounterSource.Object;

            source.Start();
            var queryTask = Task.Run(() => source.Query(null));
            Thread.Sleep(500);
            source.Stop();
            queryTask.Wait();

            // assert that no error log message was written
            Assert.Empty(logger.LogLevels.Where(l => l == LogLevel.Error));
        }
    }
}
