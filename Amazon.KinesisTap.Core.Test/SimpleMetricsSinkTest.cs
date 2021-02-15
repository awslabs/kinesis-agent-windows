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
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    public class SimpleMetricsSinkTest
    {
        private readonly BookmarkManager _bookmarkManager = new BookmarkManager();

        [Fact]
        public void TestMetricsFilterSingleInstance()
        {
            string id = "MetricsFilterSingleInstance";
            MemoryLogger logger = new MemoryLogger(null);
            MockMetricsSink sink = CreateMetricsSink(id, logger);
            SendSampleMetrics(sink);
            sink.Stop(); //Cause flush
            Assert.Equal(2, sink.FilteredLastValues.Count);
            Assert.Equal(0, sink.FilteredAccumulatedValues.Count);
            Assert.Equal(0, sink.FilteredAggregatedAccumulatedValues.Count);
            Assert.Equal(0, sink.FilteredAggregatedLastValues.Count);
        }

        [Fact]
        public void TestMetricsFilterMultipleInstance()
        {
            string id = "MetricsFilterMultipleInstance";
            MemoryLogger logger = new MemoryLogger(null);
            MockMetricsSink sink = CreateMetricsSink(id, logger);
            SendSampleMetrics(sink);
            sink.Stop(); //Cause flush
            Assert.Equal(3, sink.FilteredLastValues.Count);
            Assert.Equal(8, sink.FilteredAccumulatedValues.Count);
            Assert.Equal(0, sink.FilteredAggregatedAccumulatedValues.Count);
            Assert.Equal(0, sink.FilteredAggregatedLastValues.Count);
        }

        [Fact]
        public void TestMetricsFilterAll()
        {
            string id = "MetricsFilterAll";
            MemoryLogger logger = new MemoryLogger(null);
            MockMetricsSink sink = CreateMetricsSink(id, logger);
            SendSampleMetrics(sink);
            sink.Stop(); //Cause flush
            Assert.Equal(5, sink.FilteredLastValues.Count);
            Assert.Equal(8, sink.FilteredAccumulatedValues.Count);
            Assert.Equal(0, sink.FilteredAggregatedAccumulatedValues.Count);
            Assert.Equal(0, sink.FilteredAggregatedLastValues.Count);
        }

        [Fact]
        public void TestMetricsFilterAllErrors()
        {
            string id = "MetricsFilterAllErrors";
            MemoryLogger logger = new MemoryLogger(null);
            MockMetricsSink sink = CreateMetricsSink(id, logger);
            SendSampleMetrics(sink);
            sink.Stop(); //Cause flush
            Assert.Equal(1, sink.FilteredLastValues.Count);
            Assert.Equal(6, sink.FilteredAccumulatedValues.Count);
            Assert.Equal(0, sink.FilteredAggregatedAccumulatedValues.Count);
            Assert.Equal(0, sink.FilteredAggregatedLastValues.Count);
        }

        [Fact]
        public void TestMetricsFilterMultipleErrorsAggregated()
        {
            string id = "MetricsFilterMultipleErrorsAggregated";
            MemoryLogger logger = new MemoryLogger(null);
            MockMetricsSink sink = CreateMetricsSink(id, logger);
            SendSampleMetrics(sink);
            sink.Stop(); //Cause flush
            Assert.Equal(0, sink.FilteredLastValues.Count);
            Assert.Equal(0, sink.FilteredAccumulatedValues.Count);
            Assert.Equal(1, sink.FilteredAggregatedAccumulatedValues.Count);
            Assert.Equal(2L, sink.FilteredAggregatedAccumulatedValues.Values.First().Value);
            Assert.Equal(0, sink.FilteredAggregatedLastValues.Count);
        }

        [Fact]
        public void TestMetricsFilterLatestValueAggregated()
        {
            string id = "MetricsFilterLatestValueAggregated";
            MemoryLogger logger = new MemoryLogger(null);
            MockMetricsSink sink = CreateMetricsSink(id, logger);
            SendSampleMetrics(sink);
            sink.Stop(); //Cause flush
            Assert.Equal(0, sink.FilteredLastValues.Count);
            Assert.Equal(0, sink.FilteredAccumulatedValues.Count);
            Assert.Equal(4, sink.FilteredAggregatedAccumulatedValues.Count);
            Assert.Equal(1, sink.FilteredAggregatedLastValues.Count);
            Assert.Equal(260L, sink.FilteredAggregatedLastValues.Values.First().Value);
        }

        [Fact]
        public void TestMetricsInitialization()
        {
            string id = "TextDecoration";
            MemoryLogger logger = new MemoryLogger(null);
            KinesisTapMetricsSource metrics = new KinesisTapMetricsSource(new PluginContext(null, null, null, _bookmarkManager, null, null));
            MockMetricsSink sink = CreateMetricsSink(id, logger, metrics);
            metrics.Subscribe(sink);

            metrics.InitializeCounters(id, "Sinks", CounterTypeEnum.Increment,
                new Dictionary<string, MetricValue>
                {
                    {"SinksStarted", new MetricValue(2) },
                    {"SinksFailedToStart", new MetricValue(1) }
                });
            sink.Stop();
            Assert.Equal(2, sink.AccumlatedValues.Count);
            Assert.Equal(3, TestUtility.GetMetricsCount(sink.AccumlatedValues));
        }

        [Fact]
        public void TestOnSubscribe()
        {
            string id = "TextDecoration";
            MemoryLogger logger = new MemoryLogger(null);
            KinesisTapMetricsSource metrics = new KinesisTapMetricsSource(new PluginContext(null, null, null, _bookmarkManager, null, null));
            MockMetricsSink sink = CreateMetricsSink(id, logger, metrics);

            metrics.InitializeCounters(id, "Sinks", CounterTypeEnum.Increment,
                new Dictionary<string, MetricValue>
                {
                    {"SinksStarted", new MetricValue(2) },
                    {"SinksFailedToStart", new MetricValue(1) }
                });

            metrics.Subscribe(sink);
            sink.Stop();
            Assert.Equal(2, sink.AccumlatedValues.Count);
            Assert.Equal(3, TestUtility.GetMetricsCount(sink.AccumlatedValues));
        }

        [Fact]
        public void TestDirectorySourceMetricsOnSubscribe()
        {
            IConfiguration config = GetConfig("directorySourceTest");
            config[ConfigConstants.ID] = "TestDirectorySourceMetricsOnSubscribe";

            MemoryLogger logger = new MemoryLogger(null);
            KinesisTapMetricsSource metrics = new KinesisTapMetricsSource(new PluginContext(null, null, null, _bookmarkManager, null, null));

            DirectorySource<string, LogContext> source = new DirectorySource<string, LogContext>(
                TestUtility.GetTestHome(),
                "*.log",
                1000,
                new PluginContext(config, logger, metrics, _bookmarkManager),
                new SingleLineRecordParser());

            MockMetricsSink metricsSink = new MockMetricsSink(3600, new PluginContext(config, logger, metrics, _bookmarkManager));

            source.Start();
            metrics.Subscribe(metricsSink);
            metricsSink.Stop();
            source.Stop();
            Assert.Equal(2, metricsSink.AccumlatedValues.Count);
            Assert.Equal(0, TestUtility.GetMetricsCount(metricsSink.AccumlatedValues));
        }

        [Fact]
        public void TestDirectorySourceMetricsStart()
        {
            IConfiguration config = GetConfig("directorySourceTest");
            config[ConfigConstants.ID] = "TestDirectorySourceMetricsStart";

            MemoryLogger logger = new MemoryLogger(null);
            KinesisTapMetricsSource metrics = new KinesisTapMetricsSource(new PluginContext(null, null, null, _bookmarkManager, null, null));

            DirectorySource<string, LogContext> source = new DirectorySource<string, LogContext>(
                TestUtility.GetTestHome(),
                "*.log",
                1000,
                new PluginContext(config, logger, metrics, _bookmarkManager),
                new SingleLineRecordParser());

            MockMetricsSink metricsSink = new MockMetricsSink(3600, new PluginContext(config, logger, metrics, _bookmarkManager));

            metrics.Subscribe(metricsSink);
            source.Start();
            source.Stop();
            metricsSink.Stop();
            Assert.Equal(2, metricsSink.AccumlatedValues.Count);
            Assert.Equal(0, TestUtility.GetMetricsCount(metricsSink.AccumlatedValues));
        }

        private static void SendSampleMetrics(MockMetricsSink sink)
        {
            sink.OnNext(new MetricsEnvelope("", "Program", CounterTypeEnum.CurrentValue,
                new Dictionary<string, MetricValue>
                {
                    {"SinksStarted", new MetricValue(2) },
                    {"SinksFailedToStart", new MetricValue(1) }
                }));
            sink.OnNext(new MetricsEnvelope("KinesisFirehose1", "Sinks", CounterTypeEnum.Increment,
                new Dictionary<string, MetricValue>
                {
                    {"KinesisFirehoseRecordsSuccess", new MetricValue(100) },
                    {"KinesisFirehoseRecordsFailedNonrecoverable", new MetricValue(3) },
                    {"KinesisFirehoseRecordsFailedRecoverable", new MetricValue(1) },
                    {"KinesisFirehoseRecoverableServiceErrors", new MetricValue(1) }
                }));
            sink.OnNext(new MetricsEnvelope("KinesisFirehose2", "Sinks", CounterTypeEnum.Increment,
                new Dictionary<string, MetricValue>
                {
                    {"KinesisFirehoseRecordsSuccess", new MetricValue(50) },
                    {"KinesisFirehoseRecordsFailedNonrecoverable", new MetricValue(4) },
                    {"KinesisFirehoseRecordsFailedRecoverable", new MetricValue(2) },
                    {"KinesisFirehoseRecoverableServiceErrors", new MetricValue(1) }
                }));
            sink.OnNext(new MetricsEnvelope("KinesisFirehose1", "Sinks", CounterTypeEnum.CurrentValue,
                new Dictionary<string, MetricValue>
                {
                    {"KinesisFirehoseLatency", new MetricValue(350, MetricUnit.Milliseconds) }
                }));
            sink.OnNext(new MetricsEnvelope("KinesisFirehose2", "Sinks", CounterTypeEnum.CurrentValue,
                new Dictionary<string, MetricValue>
                {
                    {"KinesisFirehoseLatency", new MetricValue(250, MetricUnit.Milliseconds) }
                }));
            sink.OnNext(new MetricsEnvelope("KinesisFirehose3", "Sinks", CounterTypeEnum.CurrentValue,
                new Dictionary<string, MetricValue>
                {
                    {"KinesisFirehoseLatency", new MetricValue(180, MetricUnit.Milliseconds) }
                }));
        }

        private MockMetricsSink CreateMetricsSink(string id, ILogger logger, IMetrics metrics = null)
        {
            var config = TestUtility.GetConfig("Sinks", id);
            var sink = new MockMetricsSink(3600, new PluginContext(config, logger, metrics, _bookmarkManager));
            return sink;
        }

        private static IConfiguration GetConfig(string id)
        {
            ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
            string basePath = AppContext.BaseDirectory;
            var config = configurationBuilder
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("testSettings1.json", optional: false, reloadOnChange: false)
                .Build();
            var sections = config.GetSection("Sources").GetChildren();
            foreach (var s in sections)
            {
                if (s[ConfigConstants.ID] == id)
                    return s;
            }
            return null;
        }

    }
}
