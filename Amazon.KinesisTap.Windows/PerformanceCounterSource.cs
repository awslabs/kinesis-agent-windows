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
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Windows
{
    public class PerformanceCounterSource : IDataSource<ICollection<KeyValuePair<MetricKey, MetricValue>>>
    {
        //Match the first. So more specific one first
        private static readonly string[] SIZE_KEYWORDS = { "MBytes", "Megabytes", "KBytes", "Kilobytes", "Bytes" };
        private static readonly string[] TIME_KEYWORDS = new[] { "Milliseconds", "Seconds", "100 ns", "Latency", "sec/" };
        private static readonly string[] PERCENT_KEYWORDS = new[] { "%", "Percent" };

        private readonly IPlugInContext _context;

        //Capture the performance counter source configurations
        private readonly IList<CategoryInfo> _categoryInfos;

        //Cache the units for each category/counter pair
        private readonly IDictionary<(string category, string counter), MetricUnit> _counterUnitsCache
            = new Dictionary<(string, string), MetricUnit>();
        private bool _started;
        private readonly object _lockObject = new object();
        private readonly Dictionary<MetricKey, PerformanceCounter> _metricCounters = new Dictionary<MetricKey, PerformanceCounter>();

        public PerformanceCounterSource(IPlugInContext context)
        {
            _context = context;

            var categoriesSection = _context.Configuration.GetSection("Categories");
            _categoryInfos = new PerformanceCounterSourceConfigLoader(_context, _counterUnitsCache)
                .LoadCategoriesConfig(categoriesSection);
            if (_categoryInfos == null || _categoryInfos.Count == 0)
            {
                throw new InvalidOperationException("WindowsPerformanceCounterSource missing required attribute 'Categories'.");
            }
        }

        #region public members
        public string Id { get; set; }

        public void Start()
        {
            lock (_lockObject)
            {
                _started = true;
            }
        }

        public void Stop()
        {
            lock (_lockObject)
            {
                _started = false;
                foreach (var kvp in _metricCounters)
                {
                    kvp.Value.Dispose();
                }

                _metricCounters.Clear();
            }
        }

        public IEnvelope<ICollection<KeyValuePair<MetricKey, MetricValue>>> Query(string query)
        {
            lock (_lockObject)
            {
                if (!_started)
                {
                    return null;
                }

                try
                {
                    RefreshCounters();
                    List<KeyValuePair<MetricKey, MetricValue>> metrics = new List<KeyValuePair<MetricKey, MetricValue>>();
                    ReadCounterMetrics(metrics);
                    return new Envelope<ICollection<KeyValuePair<MetricKey, MetricValue>>>(metrics, DateTime.UtcNow);
                }
                catch (Exception ex)
                {
                    _context.Logger?.LogError($"Error querying performance counter source {Id}: {ex.ToMinimized()}");
                }
                return null;
            }
        }

        public static MetricUnit InferUnit(string category, string counterName)
        {
            bool isPercent = PERCENT_KEYWORDS.Any(kw =>
                counterName.IndexOf(kw, StringComparison.InvariantCultureIgnoreCase) > -1);
            if (isPercent) return MetricUnit.Percent;

            bool isRate = counterName.IndexOf("/sec", StringComparison.InvariantCultureIgnoreCase) > -1
                || counterName.IndexOf("/ sec", StringComparison.InvariantCultureIgnoreCase) > -1
                || counterName.EndsWith("Rate", StringComparison.InvariantCultureIgnoreCase)
                || counterName.StartsWith("Rate of");

            string sizeKeyword = SIZE_KEYWORDS.FirstOrDefault(kw =>
                counterName.IndexOf(kw, StringComparison.InvariantCultureIgnoreCase) > -1);
            bool isSize = !string.IsNullOrEmpty(sizeKeyword);

            if (isRate)
            {
                if (isSize)
                {
                    switch (char.ToLower(sizeKeyword[0]))
                    {
                        case 'm':
                            return MetricUnit.MegabytesSecond;
                        case 'k':
                            return MetricUnit.KilobytesSecond;
                        default:
                            return MetricUnit.BytesSecond;
                    }
                }
                else
                {
                    return MetricUnit.CountSecond;
                }
            }
            else //not rate
            {
                if (isSize)
                {
                    switch (char.ToLower(sizeKeyword[0]))
                    {
                        case 'm':
                            return MetricUnit.Megabytes;
                        case 'k':
                            return MetricUnit.Kilobytes;
                        default:
                            return MetricUnit.Bytes;
                    }
                }

                string timeKeyword = TIME_KEYWORDS.FirstOrDefault(kw =>
                    counterName.IndexOf(kw, StringComparison.InvariantCultureIgnoreCase) > -1);
                if (!string.IsNullOrEmpty(timeKeyword))
                {
                    if (char.ToLower(timeKeyword[0]) == 's')
                    {
                        return MetricUnit.Seconds;
                    }
                    else if (timeKeyword.Equals("100 ns"))
                    {
                        return MetricUnit.HundredNanoseconds;
                    }
                    else
                    {
                        return MetricUnit.Milliseconds;
                    }
                }
                else
                {
                    return MetricUnit.Count;
                }
            }
        }
        #endregion


        #region private members for managing performance counters

        private static (HashSet<string> names, IList<Regex> patterns) GetNamesAndPatterns(string[] nameOrPatterns)
        {
            return GetNamesAndPatterns(nameOrPatterns, null);
        }

        private static (HashSet<string> names, IList<Regex> patterns) GetNamesAndPatterns(string[] nameOrPatterns, string regex)
        {
            HashSet<string> names = new HashSet<string>();
            IList<Regex> patterns = new List<Regex>();
            if (nameOrPatterns != null)
            {
                foreach (var nameOrPattern in nameOrPatterns)
                {
                    if (Utility.IsWildcardExpression(nameOrPattern))
                    {
                        string pattern = Utility.WildcardToRegex(nameOrPattern, true);
                        patterns.Add(new Regex(pattern));
                    }
                    else
                    {
                        names.Add(nameOrPattern);
                    }
                }
            }
            if (!string.IsNullOrWhiteSpace(regex))
            {
                patterns.Add(new Regex(regex));
            }
            return (names, patterns);
        }

        /// <summary>
        /// Refresh the list of counters for querying.
        /// We need to do this because the number of instances could change since we use wildcard for instances.
        /// </summary>
        public virtual void RefreshCounters()
        {
            var newCounters = new HashSet<MetricKey>();
            foreach (var categoryInfo in _categoryInfos)
            {
                try
                {
                    // add the counters available in the category to the set
                    AddCategoryCountersToSet(categoryInfo, newCounters);
                }
                catch (Exception ex)
                {
                    _context.Logger?.LogWarning(0, ex, $"Could not add counters for category {categoryInfo.CategoryName}.");
                }
            }

            // now we run a 'diff' between the new and the current set of counters
            // first we check to see what counters are stale
            var stale = new List<MetricKey>();
            foreach (var counterKey in _metricCounters.Keys)
            {
                if (!newCounters.Contains(counterKey))
                {
                    stale.Add(counterKey);
                }
            }

            // remove the stale counters
            foreach (var staleCounter in stale)
            {
                _metricCounters[staleCounter].Dispose();
                _metricCounters.Remove(staleCounter);
                _context.Logger?.LogInformation($"Removed performance counter: category {staleCounter.Category} instance {staleCounter.Id} counter {staleCounter.Name}.");
            }

            // add in the new counters
            foreach (var newCounterKey in newCounters)
            {
                if (_metricCounters.ContainsKey(newCounterKey))
                {
                    continue;
                }

                try
                {
                    var counter = new PerformanceCounter(newCounterKey.Category, newCounterKey.Name, newCounterKey.Id);
                    _metricCounters[newCounterKey] = counter;
                    var instancePhrase = string.IsNullOrEmpty(counter.InstanceName)
                        ? string.Empty
                        : $" instance {counter.InstanceName}";
                    _context.Logger?.LogInformation($"Added performance counter: category {counter.CategoryName}{instancePhrase} counter {counter.CounterName}.");
                }
                catch (Exception ex) when (ex is InvalidOperationException || ex is ArgumentNullException || ex is UnauthorizedAccessException || ex is Win32Exception)
                {
                    // catch the exception caused by the performance counter constructor
                    // this should not happen, because the counter key is obtained from the system's API
                    // however we want to make sure that failure in creating one counter does not impact others.
                    _context.Logger?.LogError(0, ex,
                        $"Error adding counter: category {newCounterKey.Category} instance {newCounterKey.Id} counter {newCounterKey.Name}.");
                }
            }
        }

        private void AddCategoryCountersToSet(CategoryInfo categoryInfo, HashSet<MetricKey> counters)
        {
            // figure out what instances to load for the category
            var category = new PerformanceCounterCategory(categoryInfo.CategoryName);
            switch (category.CategoryType)
            {
                case PerformanceCounterCategoryType.SingleInstance:
                    AddInstanceCountersToSet(category, categoryInfo.CounterFilters, string.Empty, counters);
                    return;
                case PerformanceCounterCategoryType.MultiInstance:
                    break;
                default:
                    return;
            }

            (HashSet<string> instanceNames, IList<Regex> instancePatterns) = GetNamesAndPatterns(categoryInfo.InstanceFilters, categoryInfo.InstanceRegex);

            foreach (var instanceName in instanceNames)
            {
                if (!category.InstanceExists(instanceName))
                {
                    _context.Logger?.LogError($"Instance does not exist for category {categoryInfo.CategoryName} and instance {instanceName}");
                }
            }

            var filteredNames = category
                .GetInstanceNames()
                .Where(i => instanceNames.Contains(i) || instancePatterns.Any(p => p.IsMatch(i)));
            foreach (var instanceName in filteredNames)
            {
                AddInstanceCountersToSet(category, categoryInfo.CounterFilters, instanceName, counters);
            }
        }

        private void AddInstanceCountersToSet(PerformanceCounterCategory category, string[] counterFilter,
            string instanceName, HashSet<MetricKey> counters)
        {
            (HashSet<string> counterNames, IList<Regex> counterPatterns) = GetNamesAndPatterns(counterFilter);

            if (counterPatterns.Count == 0)
            {
                // fast path when we have no wildcard for counters, just get the counter names
                foreach (var counterName in counterNames)
                {
                    counters.Add(new MetricKey
                    {
                        Category = category.CategoryName,
                        Id = instanceName,
                        Name = counterName
                    });
                }

                return;
            }

            // there are patterns for counter names, so we need to get all the counters and match them with the patter
            // unfortunately, there's no GetCounterNames() API, so we need to create all the counters, get their names,
            // and discard them.
            var countersToAdd = category
                .GetCounters(instanceName)
                .Select(c =>
                {
                    var name = c.CounterName;
                    c.Dispose();
                    return name;
                })
                .Where(c => counterNames.Contains(c) || counterPatterns.Any(p => p.IsMatch(c)));

            foreach (var counterName in countersToAdd)
            {
                counters.Add(new MetricKey
                {
                    Category = category.CategoryName,
                    Id = instanceName,
                    Name = counterName
                });
            }
        }

        private void ReadCounterMetrics(IList<KeyValuePair<MetricKey, MetricValue>> metrics)
        {
            foreach (var kvp in _metricCounters)
            {
                try
                {
                    var counter = kvp.Value;
                    var value = (long)counter.NextValue();
                    if (string.IsNullOrEmpty(kvp.Key.Id))
                    {
                        // if the instance name is empty, it means this is a single-instance counter
                        metrics.Add(new KeyValuePair<MetricKey, MetricValue>
                        (
                            new MetricKey { Name = counter.CounterName, Category = counter.CategoryName },
                            new MetricValue(value, GetMetricUnit(counter.CategoryName, counter.CounterName))
                        ));
                    }
                    else
                    {
                        // multiple-instance counter
                        metrics.Add(new KeyValuePair<MetricKey, MetricValue>
                        (
                            new MetricKey { Name = counter.CounterName, Id = counter.InstanceName, Category = counter.CategoryName },
                            new MetricValue(value, GetMetricUnit(counter.CategoryName, counter.CounterName))
                        ));
                    }
                }
                catch (Exception ex) when (ex is InvalidOperationException || ex is Win32Exception || ex is UnauthorizedAccessException)
                {
                    _context.Logger?.LogWarning(0, ex, $"Encountered expcetion when reading performance counter");
                    continue;
                }
            }
        }

        private MetricUnit GetMetricUnit(string category, string counterName)
        {
            if (!_counterUnitsCache.TryGetValue((category, counterName), out MetricUnit unit))
            {
                unit = InferUnit(category, counterName);
                _counterUnitsCache.Add((category, counterName), unit);
            }
            return unit;
        }
        #endregion
    }
}
