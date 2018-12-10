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
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Amazon.KinesisTap.Windows.Test
{
    public class PerformanceCounterSourceTest
    {
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
                new PluginContext(null, NullLogger.Instance, null),
                counterUnitsCache);
            var categories = performanceCounterSourceLoader.LoadCategoriesConfig(categoriesSection);
            Assert.Equal(5, categories.Count);
            Assert.Equal(1, counterUnitsCache.Count);
            Assert.Equal(MetricUnit.CountSecond, counterUnitsCache.Values.First());
        }

        [Fact]
        public void TestPerformanceCounterSource()
        {
            var config = TestUtility.GetConfig("Sources", "PerformanceCounter");
            var performanceCounterSource = new PerformanceCounterSource(new PluginContext(config, NullLogger.Instance, null));
            performanceCounterSource.Start();
            var results = performanceCounterSource.Query(null);
            var metrics = results.Data as ICollection<KeyValuePair<MetricKey, MetricValue>>;
            Assert.True(metrics.Count > 0);
            Assert.Contains(metrics, m => m.Value.Value > 0);
            performanceCounterSource.Stop();
        }

        [Fact]
        [Trait("Category", "Integration")]
        public void TestPerformanceCounterSourceForTransientInstance()
        {
            var config = TestUtility.GetConfig("Sources", "PerformanceCounter");
            var performanceCounterSource = new PerformanceCounterSource(new PluginContext(config, NullLogger.Instance, null));
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
    }
}
