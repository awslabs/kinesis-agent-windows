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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;

namespace Amazon.KinesisTap.Test.Common
{
    public class InMemoryMetricsSource : IMetrics
    {
        public string Id { get; set; }

        public Type GetOutputType()
        {
            return typeof(IDictionary<string, MetricValue>);
        }

        public ConcurrentDictionary<MetricKey, MetricValue> Metrics { get; } = new ConcurrentDictionary<MetricKey, MetricValue>();

        public void InitializeCounters(string id, string category, CounterTypeEnum counterType, IDictionary<string, MetricValue> counters)
            => PublishCounters(id, category, counterType, counters);

        public void PublishCounter(string id, string category, CounterTypeEnum counterType, string counter, long value, MetricUnit unit)
            => PublishCounters(id, category, counterType, new Dictionary<string, MetricValue>
            {
                {counter, new MetricValue(value,unit) }
            });

        public void PublishCounters(string id, string category, CounterTypeEnum counterType, IDictionary<string, MetricValue> counters)
        {
            foreach (var counter in counters)
            {
                var key = new MetricKey { Id = id, Category = category, Name = counter.Key };
                Metrics.AddOrUpdate(key,
                    addValueFactory: (k, val) => val,
                    updateValueFactory: (k, old, val) =>
                    {
                        var newVal = counterType == CounterTypeEnum.CurrentValue
                            ? val.Value
                            : old.Value + val.Value;
                        return new MetricValue(newVal, old.Unit);
                    },
                    factoryArgument: counter.Value);
                Metrics[key] = counter.Value;
            }
        }

        public ValueTask StartAsync(CancellationToken stopToken)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask StopAsync(CancellationToken gracefulStopToken)
        {
            return ValueTask.CompletedTask;
        }

        public IDisposable Subscribe(IObserver<IEnvelope> observer) => throw new InvalidOperationException();

        public IDisposable Subscribe(IObserver<IEnvelope<IDictionary<string, MetricValue>>> observer) => throw new InvalidOperationException();
    }
}
