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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    public class SimpleMetricsSinkTest
    {
        [Fact]
        public void TestMetricsFilterSingleInstance()
        {
            var id = "MetricsFilterSingleInstance";
            var logger = new MemoryLogger(null);
            var sink = CreateMetricsSink(id, logger);
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
            var id = "MetricsFilterMultipleInstance";
            var logger = new MemoryLogger(null);
            var sink = CreateMetricsSink(id, logger);
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
            var id = "MetricsFilterAll";
            var logger = new MemoryLogger(null);
            var sink = CreateMetricsSink(id, logger);
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
            var id = "MetricsFilterAllErrors";
            var logger = new MemoryLogger(null);
            var sink = CreateMetricsSink(id, logger);
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
            var id = "MetricsFilterMultipleErrorsAggregated";
            var logger = new MemoryLogger(null);
            var sink = CreateMetricsSink(id, logger);
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
            var id = "MetricsFilterLatestValueAggregated";
            var logger = new MemoryLogger(null);
            var sink = CreateMetricsSink(id, logger);
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
            var id = "TextDecoration";
            var logger = new MemoryLogger(null);
            var metrics = new KinesisTapMetricsSource(nameof(TestMetricsInitialization), NullLogger.Instance);
            var sink = CreateMetricsSink(id, logger, metrics);
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
            var id = "TextDecoration";
            var logger = new MemoryLogger(null);
            var metrics = new KinesisTapMetricsSource(nameof(TestOnSubscribe), NullLogger.Instance);
            var sink = CreateMetricsSink(id, logger, metrics);

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
            var sink = new MockMetricsSink(3600, new PluginContext(config, logger, metrics));
            return sink;
        }
    }
}
