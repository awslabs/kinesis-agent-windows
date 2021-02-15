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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using System.Threading;

namespace Amazon.KinesisTap.Windows
{
    public class PerformanceCounterSink : SimpleMetricsSink
    {
        private const string KINESISTAP_PERFORMANCE_COUNTER_CATEGORY = "KinesisTap";
        private const string KINESISTAP_PERFORMANCE_COUNTER_SOURCES_CATEGORY = "KinesisTap Sources";
        private const string KINESISTAP_PERFORMANCE_COUNTER_SINKS_CATEGORY = "KinesisTap Sinks";

        public PerformanceCounterSink(int defaultInterval, IPlugInContext context) : base(defaultInterval, context)
        {
        }

        public override void Start()
        {
            CreateCounterCategoriesIfNotExist();
            base.Start();
            _logger?.LogInformation($"Performance counter sink {this.Id} started.");
        }

        public override void Stop()
        {
            base.Stop();
            _logger?.LogInformation($"Performance counter sink {this.Id} stopped.");
        }

        public static void CreateCounterCategory(string category)
        {
            var counterData = GetCounterData(category);
            PerformanceCounterCategory.Create(category,
                $"Performance counter for AWS KinesisTap {category}.",
                category.Equals(KINESISTAP_PERFORMANCE_COUNTER_CATEGORY) ? PerformanceCounterCategoryType.SingleInstance : PerformanceCounterCategoryType.MultiInstance,
                counterData
            );
        }

        protected override void OnFlush(IDictionary<MetricKey, MetricValue> accumlatedValues, IDictionary<MetricKey, MetricValue> lastValues)
        {
            WriterCounters(accumlatedValues, (c, v) => c.IncrementBy(v));

            WriterCounters(lastValues, (c, v) => c.RawValue = v );
        }

        private void WriterCounters(IDictionary<MetricKey, MetricValue> counterValues, Action<PerformanceCounter, long> writeCounter)
        {
            foreach (var key in counterValues.Keys)
            {
                try
                {
                    using (PerformanceCounter counter = new PerformanceCounter(
                        GetPerformanceCounterCategory(key.Category),
                        key.Name,
                        key.Id,
                        false))
                    {
                        writeCounter(counter, counterValues[key].Value);
                    }
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex.ToMinimized());
                }
            }
        }

        private static string GetPerformanceCounterCategory(string category)
        {
            switch(category)
            {
                case MetricsConstants.CATEGORY_SOURCE:
                    return KINESISTAP_PERFORMANCE_COUNTER_SOURCES_CATEGORY;
                case MetricsConstants.CATEGORY_SINK:
                    return KINESISTAP_PERFORMANCE_COUNTER_SINKS_CATEGORY;
                default:
                    return KINESISTAP_PERFORMANCE_COUNTER_CATEGORY;
            }
        }

        private static void CreateCounterCategoriesIfNotExist()
        {
            int categoryCreateCount = 0;
            if (CreateCounterCategoryIfNotExist(KINESISTAP_PERFORMANCE_COUNTER_CATEGORY))
            {
                categoryCreateCount++;
            }
            if (CreateCounterCategoryIfNotExist(KINESISTAP_PERFORMANCE_COUNTER_SOURCES_CATEGORY))
            {
                categoryCreateCount++;
            }
            if (CreateCounterCategoryIfNotExist(KINESISTAP_PERFORMANCE_COUNTER_SINKS_CATEGORY))
            {
                categoryCreateCount++;
            }
            if (categoryCreateCount > 0)
            {
                Thread.Sleep(5000); //There is a latency for windows to pick up the new category
            }
        }

        private static bool CreateCounterCategoryIfNotExist(string category)
        {
            bool created = false;
            if (!PerformanceCounterCategory.Exists(category))
            {
                CreateCounterCategory(category);
                created = true;
            }
            return created;
        }

        private static CounterCreationDataCollection GetCounterData(string category)
        {
            switch(category)
            {
                case KINESISTAP_PERFORMANCE_COUNTER_CATEGORY:
                    return new CounterCreationDataCollection()
                    {
                        CreateCounterCreationData(string.Empty, MetricsConstants.KINESISTAP_BUILD_NUMBER, PerformanceCounterType.NumberOfItems32),
                        CreateCounterCreationData(string.Empty, MetricsConstants.SOURCE_FACTORIES_LOADED, PerformanceCounterType.NumberOfItems32),
                        CreateCounterCreationData(string.Empty, MetricsConstants.SOURCE_FACTORIES_FAILED_TO_LOAD, PerformanceCounterType.NumberOfItems32),
                        CreateCounterCreationData(string.Empty, MetricsConstants.SINK_FACTORIES_LOADED, PerformanceCounterType.NumberOfItems32),
                        CreateCounterCreationData(string.Empty, MetricsConstants.SINK_FACTORIES_FAILED_TO_LOAD, PerformanceCounterType.NumberOfItems32),
                        CreateCounterCreationData(string.Empty, MetricsConstants.SINKS_STARTED, PerformanceCounterType.NumberOfItems32),
                        CreateCounterCreationData(string.Empty, MetricsConstants.SINKS_FAILED_TO_START, PerformanceCounterType.NumberOfItems32),
                        CreateCounterCreationData(string.Empty, MetricsConstants.SOURCES_STARTED, PerformanceCounterType.NumberOfItems32),
                        CreateCounterCreationData(string.Empty, MetricsConstants.SOURCES_FAILED_TO_START, PerformanceCounterType.NumberOfItems32),
                        CreateCounterCreationData(string.Empty, MetricsConstants.PIPES_CONNECTED, PerformanceCounterType.NumberOfItems32),
                        CreateCounterCreationData(string.Empty, MetricsConstants.PIPES_FAILED_TO_CONNECT, PerformanceCounterType.NumberOfItems32),
                        CreateCounterCreationData(string.Empty, MetricsConstants.SELF_UPDATE_FREQUENCY, PerformanceCounterType.NumberOfItems32),
                        CreateCounterCreationData(string.Empty, MetricsConstants.CONFIG_RELOAD_COUNT, PerformanceCounterType.NumberOfItems32),
                        CreateCounterCreationData(string.Empty, MetricsConstants.CONFIG_RELOAD_FAILED_COUNT, PerformanceCounterType.NumberOfItems32)
                    };

                case KINESISTAP_PERFORMANCE_COUNTER_SOURCES_CATEGORY:
                    return new CounterCreationDataCollection()
                    {
                        CreateCounterCreationData(string.Empty, MetricsConstants.DIRECTORY_SOURCE_BYTES_TO_READ, PerformanceCounterType.NumberOfItems32),
                        CreateCounterCreationData(string.Empty, MetricsConstants.DIRECTORY_SOURCE_FILES_TO_PROCESS, PerformanceCounterType.NumberOfItems32),
                        CreateCounterCreationData(string.Empty, MetricsConstants.DIRECTORY_SOURCE_BYTES_READ, PerformanceCounterType.NumberOfItems32),
                        CreateCounterCreationData(string.Empty, MetricsConstants.DIRECTORY_SOURCE_RECORDS_READ, PerformanceCounterType.NumberOfItems32),
                        CreateCounterCreationData(string.Empty, MetricsConstants.EVENTLOG_SOURCE_EVENTS_READ, PerformanceCounterType.NumberOfItems32),
                        CreateCounterCreationData(string.Empty, MetricsConstants.EVENTLOG_SOURCE_EVENTS_ERROR, PerformanceCounterType.NumberOfItems32),
                    };

                case KINESISTAP_PERFORMANCE_COUNTER_SINKS_CATEGORY:
                    CounterCreationDataCollection counterData = new CounterCreationDataCollection();
                    string[] prefixes = new string[]
                    {
                        MetricsConstants.CLOUDWATCHLOG_PREFIX,
                        MetricsConstants.KINESIS_FIREHOSE_PREFIX,
                        MetricsConstants.KINESIS_STREAM_PREFIX
                    };

                    foreach (string prefix in prefixes)
                    {
                        CounterCreationDataCollection sinkCounterData = new CounterCreationDataCollection()
                        {
                            CreateCounterCreationData(prefix, MetricsConstants.RECOVERABLE_SERVICE_ERRORS, PerformanceCounterType.NumberOfItems32),
                            CreateCounterCreationData(prefix, MetricsConstants.NONRECOVERABLE_SERVICE_ERRORS, PerformanceCounterType.NumberOfItems32),
                            CreateCounterCreationData(prefix, MetricsConstants.RECORDS_ATTEMPTED, PerformanceCounterType.NumberOfItems32),
                            CreateCounterCreationData(prefix, MetricsConstants.BYTES_ATTEMPTED, PerformanceCounterType.NumberOfItems32),
                            CreateCounterCreationData(prefix, MetricsConstants.RECORDS_SUCCESS, PerformanceCounterType.NumberOfItems32),
                            CreateCounterCreationData(prefix, MetricsConstants.RECORDS_FAILED_RECOVERABLE, PerformanceCounterType.NumberOfItems32),
                            CreateCounterCreationData(prefix, MetricsConstants.RECORDS_FAILED_NONRECOVERABLE, PerformanceCounterType.NumberOfItems32),
                            CreateCounterCreationData(prefix, MetricsConstants.LATENCY, PerformanceCounterType.NumberOfItems32),
                        };
                        counterData.AddRange(sinkCounterData);
                    }
                    return counterData;

                default:
                    throw new NotImplementedException($"Category {category} not implemented");
            }
        }

        private static CounterCreationData CreateCounterCreationData(string counterPrefix, string counterName, PerformanceCounterType counterType)
        {
            return new CounterCreationData(counterPrefix + counterName, string.Empty, counterType);
        }
    }
}
