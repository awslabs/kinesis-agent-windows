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
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Core.Metrics
{
    public class KinesisTapMetricsSource : IMetrics
    {
        private readonly ISubject<IEnvelope<IDictionary<string, MetricValue>>> _subject;
        private readonly IDictionary<string, MetricValue> _currentCounters;
        private readonly IDictionary<MetricKey, IDictionary<string, MetricValue>> _incrementalCounters;
        private readonly ILogger _logger;

        public KinesisTapMetricsSource(string id, ILogger logger)
        {
            Id = id;
            _logger = logger;
            _subject = new SubjectWrapper<IEnvelope<IDictionary<string, MetricValue>>>(OnSubscribe);
            _currentCounters = new ConcurrentDictionary<string, MetricValue>();
            _incrementalCounters = new ConcurrentDictionary<MetricKey, IDictionary<string, MetricValue>>();
        }

        public string Id { get; set; }

        public void PublishCounter(string id, string category, CounterTypeEnum counterType, string counter, long value, MetricUnit unit)
        {
            PublishCounters(id, category, counterType, new Dictionary<string, MetricValue>()
            {
                { counter, new MetricValue(value, unit) }
            });
        }

        public void PublishCounters(string id, string category, CounterTypeEnum counterType, IDictionary<string, MetricValue> counters)
        {
            //Cache a copy of current global counters to publish to new subscribers
            if (string.IsNullOrEmpty(id) && counterType == CounterTypeEnum.CurrentValue)
            {
                foreach (var kv in counters)
                {
                    _currentCounters[kv.Key] = kv.Value;
                }
            };

            _subject.OnNext(new MetricsEnvelope(
                id,
                category,
                counterType,
                counters,
                DateTime.UtcNow
            ));
        }

        public void InitializeCounters(string id, string category, CounterTypeEnum counterType, IDictionary<string, MetricValue> counters)
        {
            if (counterType == CounterTypeEnum.Increment)
            {
                var hasID = false;
                foreach (var key in _incrementalCounters.Keys)
                {
                    if (key.Id == id && key.Category == category)
                    {
                        hasID = true;
                        break;
                    }
                }

                if (!hasID)
                {
                    _incrementalCounters.Add(new MetricKey { Id = id, Category = category }, counters);
                }
            }

            _subject.OnNext(new MetricsEnvelope(
                id,
                category,
                counterType,
                counters,
                DateTime.UtcNow
            ));
        }

        private void OnSubscribe(IObserver<IEnvelope<IDictionary<string, MetricValue>>> observer)
        {
            //Publish the global counters cached
            observer.OnNext(new MetricsEnvelope(string.Empty, MetricsConstants.CATEGORY_PROGRAM, CounterTypeEnum.CurrentValue, _currentCounters));

            foreach (var counter in _incrementalCounters)
            {
                observer.OnNext(new MetricsEnvelope(counter.Key.Id, counter.Key.Category, CounterTypeEnum.Increment, counter.Value));
            }
        }

        public Type GetOutputType() => typeof(IDictionary<string, MetricValue>);

        public ValueTask StartAsync(CancellationToken stopToken)
        {
            _logger.LogInformation("Started");
            return ValueTask.CompletedTask;
        }

        public ValueTask StopAsync(CancellationToken gracefulStopToken)
        {
            _logger.LogInformation("Stopped");
            return ValueTask.CompletedTask;
        }

        public IDisposable Subscribe(IObserver<IEnvelope> observer) => Subscribe((IObserver<IEnvelope<IDictionary<string, MetricValue>>>)observer);

        public IDisposable Subscribe(IObserver<IEnvelope<IDictionary<string, MetricValue>>> observer) => _subject.Subscribe(observer);
    }
}
