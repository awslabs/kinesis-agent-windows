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
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace Amazon.KinesisTap.Core.Metrics
{
    public abstract class SimpleMetricsSink : EventSink, IObserver<MetricsEnvelope>
    {
        protected int _interval;
        protected string _metricsFilter;
        protected List<Regex> _serviceMetricsFilters = new List<Regex>();
        protected List<Regex> _instanceMetricsFilters = new List<Regex>();
        protected List<Regex> _aggregatedMetricsFilters = new List<Regex>();

        private Timer _flushTimer;

        //Current value counter
        private readonly IDictionary<MetricKey, MetricValue> _lastValues = new Dictionary<MetricKey, MetricValue>();

        //Increment counter
        private IDictionary<MetricKey, MetricValue> _accumulatedValues = new Dictionary<MetricKey, MetricValue>();

        protected SimpleMetricsSink(int defaultInterval, IPlugInContext context) : base(context)
        {
            if (_config != null)
            {
                string interval = _config["interval"];
                if (!string.IsNullOrEmpty(interval))
                {
                    int.TryParse(interval, out _interval);
                }

                _metricsFilter = _config["metricsfilter"];
                if (!string.IsNullOrWhiteSpace(_metricsFilter))
                {
                    ConstructFilters();
                }
            }

            if (_interval == 0)
                _interval = defaultInterval;

            _flushTimer = new Timer(Flush, null, Timeout.Infinite, Timeout.Infinite);
        }

        public override void OnNext(IEnvelope value)
        {
            if (value == null) return;
            try
            {
                if (value is MetricsEnvelope counterData)
                {
                    OnNext(counterData);
                }
                else
                {
                    _logger?.LogError("Metrics sink can only subscribe data derived from MetricsEnvelope.");
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex.ToMinimized());
            }
        }


        public void OnNext(MetricsEnvelope counterData)
        {
            lock (this)
            {
                switch (counterData.CounterType)
                {
                    case CounterTypeEnum.CurrentValue:
                        ProcessCurrentValue(counterData);
                        break;
                    case CounterTypeEnum.Increment:
                        ProcessIncrementValue(counterData);
                        break;
                    default:
                        throw new NotImplementedException($"{counterData} not implemented.");
                }
            }
        }

        public override void Start()
        {
            _flushTimer.Change(Utility.Random.Next(_interval * 1000), _interval * 1000);
        }

        public override void Stop()
        {
            _flushTimer.Change(Timeout.Infinite, Timeout.Infinite);
            Flush(null);
        }

        protected abstract void OnFlush(IDictionary<MetricKey, MetricValue> accumlatedValues, IDictionary<MetricKey, MetricValue> lastValues);

        private void ProcessIncrementValue(MetricsEnvelope counterData)
        {
            foreach (string name in counterData.Data?.Keys)
            {
                var value = counterData.Data[name];
                MetricKey key = GetMetrickKey(counterData, name);
                IncrementValue(_accumulatedValues, key, value);
            }
        }

        private void ProcessCurrentValue(MetricsEnvelope counterData)
        {
            foreach (string name in counterData.Data?.Keys)
            {
                var value = counterData.Data[name];
                MetricKey key = GetMetrickKey(counterData, name);
                _lastValues[key] = value;
            }
        }

        private void IncrementValue(IDictionary<MetricKey, MetricValue> values, MetricKey key, MetricValue value)
        {
            _accumulatedValues.TryGetValue(key, out MetricValue previousValue);
            if (previousValue == null)
            {
                _accumulatedValues[key] = value;
            }
            else
            {
                previousValue.Increment(value);
            }
        }

        private void Flush(object state)
        {
            //Wait maximum half of the interval _interval * 1000/2
            if (Monitor.TryEnter(_flushTimer, _interval * 500))
            {
                try
                {
                    IDictionary<MetricKey, MetricValue> accumulatedValues = null;
                    IDictionary<MetricKey, MetricValue> lastValues = null;
                    lock (this)
                    {
                        accumulatedValues = _accumulatedValues;
                        _accumulatedValues = InitAccumulatedValues();
                        lastValues = new Dictionary<MetricKey, MetricValue>(_lastValues);
                    }
                    OnFlush(accumulatedValues, lastValues);
                }
                catch (Exception ex)
                {
                    _logger?.LogError($"Metrics sink flush error id {this.Id} exception: {ex.ToMinimized()}");
                }
                finally
                {
                    Monitor.Exit(_flushTimer);
                }
            }
        }

        private IDictionary<MetricKey, MetricValue> InitAccumulatedValues()
        {
            return _accumulatedValues.ToDictionary(kv => kv.Key, kv => new MetricValue(0L, kv.Value.Unit));
        }

        private static MetricKey GetMetrickKey(MetricsEnvelope counterData, string name)
        {
            return new MetricKey { Name = name, Id = counterData.Id, Category = counterData.Category };
        }

        private void ConstructFilters()
        {
            //See designer notes at https://w.amazon.com/bin/view/CorpInfra/IPA/Core_Services/KinesisTap/UserGuide/Metrics/#HMetricsFilter28Proposal29
            string[] filters = _metricsFilter.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
            const string AGGREGATED_METRICS_ID = "._Total";
            foreach (var filter in filters)
            {
                var trimmedFilter = filter.Trim();
                if (trimmedFilter.IndexOf('.') < 0)
                {
                    //straight forward service level metrics, such as 'Pipes*'
                    _serviceMetricsFilters.Add(ConvertToRegex(trimmedFilter));
                }
                else
                {
                    if (trimmedFilter.EndsWith(AGGREGATED_METRICS_ID))
                    {
                        _aggregatedMetricsFilters.Add(
                            ConvertToRegex(trimmedFilter.Substring(0, trimmedFilter.Length - AGGREGATED_METRICS_ID.Length))
                        );
                    }
                    else
                    {
                        _instanceMetricsFilters.Add(ConvertToRegex(trimmedFilter));
                    }
                }
            }
        }

        private Regex ConvertToRegex(string filter)
        {
            filter = filter.Replace(".", "\\."); //escape .
            filter = filter.Replace("?", "[^.]"); //Wild card  for a single character
            filter = filter.Replace("*", "[^.]*"); //Wild card for 0 or more characters
            return new Regex($"^{filter}$");
        }

        protected IDictionary<MetricKey, MetricValue> FilterValues(IDictionary<MetricKey, MetricValue> values)
        {
            var filteredValues = values
                .Where(kv =>
                {
                    var k = kv.Key;
                    if (string.IsNullOrWhiteSpace(k.Id))
                    {
                        return _serviceMetricsFilters.Any(regex => regex.IsMatch(k.Name));
                    }
                    else
                    {
                        return _instanceMetricsFilters.Any(regex => regex.IsMatch($"{k.Name}.{k.Id}"));
                    }
                }).ToDictionary(kv => kv.Key, kv => kv.Value);
            return filteredValues;
        }

        protected IDictionary<MetricKey, MetricValue> FilterAndAggregateValues(IDictionary<MetricKey, MetricValue> values, Func<IEnumerable<MetricValue>, MetricValue> aggregator)
        {
            var filteredValues = values
                //Filtering
                .Where(kv =>
                {
                    var k = kv.Key;
                    if (string.IsNullOrWhiteSpace(k.Id))
                    {
                        return false; //Can only filter multiple instance variables
                    }
                    else
                    {
                        return _aggregatedMetricsFilters.Any(regex => regex.IsMatch(k.Name));
                    }
                })
                //Grouping
                .GroupBy(kv => new MetricKey { Category = kv.Key.Category, Name = kv.Key.Name })
                //Aggregation
                .ToDictionary(g => g.Key, g => aggregator(g.Select(kv => kv.Value)));
            return filteredValues;
        }
    }
}
