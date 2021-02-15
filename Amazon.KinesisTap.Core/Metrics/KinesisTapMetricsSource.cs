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
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Reactive.Subjects;
using System.Text;

namespace Amazon.KinesisTap.Core.Metrics
{
    public class KinesisTapMetricsSource : EventSource<IDictionary<string, MetricValue>>, IMetrics
    {
        private static KinesisTapMetricsSource _instance = null;
        // mutex lock used forthread-safety.
        private static readonly object mutex = new object();

        private ISubject<IEnvelope<IDictionary<string, MetricValue>>> _subject;
        private readonly IDictionary<string, MetricValue> _currentCounters;
        private readonly IDictionary<MetricKey, IDictionary<string, MetricValue>> _incrementalCounters;

        public KinesisTapMetricsSource(IPlugInContext context) : base(context)
        {
            _subject = new SubjectWrapper<IEnvelope<IDictionary<string, MetricValue>>>(this.OnSubscribe);
            _currentCounters = new Dictionary<string, MetricValue>();
            _incrementalCounters = new Dictionary<MetricKey, IDictionary<string, MetricValue>> ();
        }

        public static KinesisTapMetricsSource GetInstance (IPlugInContext context)
        {
            lock (mutex)
            {
                if (_instance == null)
                {
                    _instance = new KinesisTapMetricsSource(context);
                }

                return _instance;
            }
        }

        public static bool PerformanceCounterSinkLoaded { get; set; }

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
                foreach(var kv in counters)
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
                bool hasID = false;
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

        public override void Start()
        {
        }

        public override void Stop()
        {
        }

        public override IDisposable Subscribe(IObserver<IEnvelope<IDictionary<string, MetricValue>>> observer)
        {
            return this._subject.Subscribe(observer);
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
    }
}
