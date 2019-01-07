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
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Windows
{
    public class PerformanceCounterSource : IDataSource<ICollection<KeyValuePair<MetricKey, MetricValue>>>
    {
        //Match the first. So more specific one first
        private static readonly string[] SIZE_KEYWORDS = { "MBytes", "Megabytes", "KBytes", "Kilobytes", "Bytes" };
        private static readonly string[] TIME_KEYWORDS = new[] { "Milliseconds", "Seconds", "100 ns", "Latency", "sec/" };
        private static readonly string[] PERCENT_KEYWORDS = new[] { "%", "Percent" };

        private IPlugInContext _context;
        //Capture all the single-instance categories and underlying Performance Counters
        private IDictionary<string, List<PerformanceCounter>> _singleInstanceCategoryCounters;
        //Capture all the multiple-instance categories, underlying instances and performance counters
        private IDictionary<string, IDictionary<string, List<PerformanceCounter>>> _multipleInstanceCategoryCounters;
        //Capture the performance counter source configurations
        private IList<CategoryInfo> _categoryInfos;
        //Cache the units for each category/counter pair
        private IDictionary<(string category, string counter), MetricUnit> _counterUnitsCache;
        private bool started;

        public PerformanceCounterSource(IPlugInContext context)
        {
            _context = context;
            _counterUnitsCache = new Dictionary<(string, string), MetricUnit>();
        }

        #region public members
        public string Id { get; set; }

        public void Start()
        {
            var categoriesSection = _context.Configuration.GetSection("Categories");
            _categoryInfos = new PerformanceCounterSourceConfigLoader(_context, _counterUnitsCache)
                .LoadCategoriesConfig(categoriesSection);
            if (_categoryInfos == null || _categoryInfos.Count == 0)
            {
                throw new InvalidOperationException("WindowsPerformanceCounterSource missing required attribute 'Categories'.");
            }
            LoadPerformanceCounters();
            started = true;
        }

        public void Stop()
        {
            started = false;
            //Capture a copy of counters and set _categoryCounters to null so it can no longer be used
            var singleInstanceCategoryCounters = _singleInstanceCategoryCounters;
            _singleInstanceCategoryCounters = null;
            //Dispose the counters
            foreach(var categoryCounters in singleInstanceCategoryCounters.Values)
            {
                foreach(var counter in categoryCounters)
                {
                    counter?.Dispose();
                }
            }

            var multipleInstanceCategoryCounters = _multipleInstanceCategoryCounters;
            _multipleInstanceCategoryCounters = null;
            foreach (var categoryInstances in multipleInstanceCategoryCounters.Values)
            {
                foreach (var instanceCounters in categoryInstances.Values)
                {
                    DisposeInstanceCounters(instanceCounters);
                }
            }
        }

        public IEnvelope<ICollection<KeyValuePair<MetricKey, MetricValue>>> Query(string query)
        {
            if (!started) return null;

            try
            {
                RefreshInstances();

                List<KeyValuePair<MetricKey, MetricValue>> metrics = new List<KeyValuePair<MetricKey, MetricValue>>();

                ReadSingleInstanceCategoryCounters(metrics);

                ReadMultipleInstanceCategoryCounters(metrics);

                return new Envelope<ICollection<KeyValuePair<MetricKey, MetricValue>>>(metrics, DateTime.UtcNow);
            }
            catch(Exception ex)
            {
                _context.Logger?.LogError($"Error querying performance counter source {this.Id}: {ex.ToMinimized()}");
            }
            return null;
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
                    switch(char.ToLower(sizeKeyword[0]))
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
        private void LoadPerformanceCounters()
        {
            _singleInstanceCategoryCounters = new Dictionary<string, List<PerformanceCounter>>();
            _multipleInstanceCategoryCounters = new Dictionary<string, IDictionary<string, List<PerformanceCounter>>>();

            foreach (var categoryInfo in _categoryInfos)
            {
                try
                {
                    LoadPerformanceCountersForCategory(categoryInfo);
                }
                catch (Exception ex)
                {
                    _context.Logger?.LogError($"Error loading category {categoryInfo.CategoryName}: {ex.ToMinimized()}");
                }
            }
        }

        private void LoadPerformanceCountersForCategory(CategoryInfo categoryInfo)
        {
            string categoryName = categoryInfo.CategoryName;
            var performanceCounterCategory = new PerformanceCounterCategory(categoryName);
            (HashSet<string> counterNames, IList<Regex> counterPatterns) =
                GetNamesAndPatterns(categoryInfo.CounterFilters);

            foreach (var counterName in counterNames)
            {
                if (!performanceCounterCategory.CounterExists(counterName))
                {
                    _context.Logger?.LogError($"Counter does not exist for category {categoryName} and counter {counterName}");
                }
            }

            if (performanceCounterCategory.CategoryType == PerformanceCounterCategoryType.MultiInstance)
            {
                LoadInstancesForCategory(categoryInfo, categoryName, performanceCounterCategory, counterNames, counterPatterns, true);
            }
            else //single instance
            {
                LoadPerformanceCountersForSingleInstanceCategory(categoryName, performanceCounterCategory, counterNames, counterPatterns);
            }
        }

        //This method is responsible for both initial load and refresh of instances
        private void LoadInstancesForCategory(CategoryInfo categoryInfo, 
            string categoryName, 
            PerformanceCounterCategory performanceCounterCategory, 
            HashSet<string> counterNames, 
            IList<Regex> counterPatterns,
            bool isInitialLoad)
        {
            (HashSet<string> instanceNames, IList<Regex> instancePatterns) = GetNamesAndPatterns(categoryInfo.InstanceFilters);
            if (isInitialLoad)
            {
                foreach (var instanceName in instanceNames)
                {
                    if (!performanceCounterCategory.InstanceExists(instanceName))
                    {
                        _context.Logger?.LogError($"Instance does not exist for category {categoryName} and instance {instanceName}");
                    }
                }
            }

            if (!_multipleInstanceCategoryCounters.TryGetValue(categoryName, out IDictionary<string, List<PerformanceCounter>> categoryInstances))
            {
                categoryInstances = new Dictionary<string, List<PerformanceCounter>>();
                _multipleInstanceCategoryCounters[categoryName] = categoryInstances;
            }

            //Create a check list of currently cached instances.
            ISet<string> cachedInstances = new HashSet<string>(categoryInstances.Keys);
            var filteredNames = performanceCounterCategory.GetInstanceNames()
                .Where(instanceName => instanceNames.Contains(instanceName) || instancePatterns.Any(p => p.IsMatch(instanceName)));
            foreach (var instanceName in filteredNames)
            {
                if (cachedInstances.Contains(instanceName))
                {
                    //Remove each verified instance name from the check list.
                    cachedInstances.Remove(instanceName); 
                }
                else
                {
                    LoadPerformanceCounterForCategoryInstance(performanceCounterCategory, counterNames, counterPatterns, categoryInstances, instanceName);
                }
            }

            //Remove from cache and dispose all the instances remain in the check list
            foreach(var instanceName in cachedInstances)
            {
                _context.Logger?.LogInformation($"Remove performance counter instance: category {categoryName} instance {instanceName}.");
                var instanceCounters = categoryInstances[instanceName];
                categoryInstances.Remove(instanceName);
                DisposeInstanceCounters(instanceCounters);
            }
        }

        private void LoadPerformanceCounterForCategoryInstance(PerformanceCounterCategory performanceCounterCategory, 
            HashSet<string> counterNames, 
            IList<Regex> counterPatterns,
            IDictionary<string, 
            List<PerformanceCounter>> categoryInstances, 
            string instanceName)
        {
            if (!categoryInstances.TryGetValue(instanceName, out List<PerformanceCounter> instanceCounters))
            {
                instanceCounters = new List<PerformanceCounter>();
                categoryInstances[instanceName] = instanceCounters;

            }

            var countersToAdd = performanceCounterCategory.GetCounters(instanceName)
                .Where(counter => counterNames.Contains(counter.CounterName) || counterPatterns.Any(p => p.IsMatch(counter.CounterName)));
            instanceCounters.AddRange(countersToAdd);
            LogCountersToAdd(countersToAdd);
        }

        private void LoadPerformanceCountersForSingleInstanceCategory(string categoryName, 
            PerformanceCounterCategory performanceCounterCategory, 
            HashSet<string> counterNames, 
            IList<Regex> counterPatterns)
        {
            if (!_singleInstanceCategoryCounters.TryGetValue(categoryName, out List<PerformanceCounter> counters))
            {
                counters = new List<PerformanceCounter>();
                _singleInstanceCategoryCounters[categoryName] = counters;
                var countersToAdd = performanceCounterCategory.GetCounters()
                    .Where(counter => counterNames.Contains(counter.CounterName) || counterPatterns.Any(p => p.IsMatch(counter.CounterName)));
                counters.AddRange(countersToAdd);
                LogCountersToAdd(countersToAdd);
            }
        }

        private void LogCountersToAdd(IEnumerable<PerformanceCounter> countersToAdd)
        {
            foreach (var counter in countersToAdd)
            {
                string instancePhrase = string.IsNullOrEmpty(counter.InstanceName) ? string.Empty
                    : $" instance {counter.InstanceName}";
                _context.Logger?.LogInformation($"Added performance counter: category {counter.CategoryName}{instancePhrase} counter {counter.CounterName}.");
            }
        }

        private static (HashSet<string> names, IList<Regex> patterns) GetNamesAndPatterns(string[] nameOrPatterns)
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
            return (names, patterns);
        }

        private static void DisposeInstanceCounters(IList<PerformanceCounter> instanceCounters)
        {
            foreach (var counter in instanceCounters)
            {
                counter?.Dispose();
            }
        }

        /// <summary>
        /// Number of instances could change since we use wildcard for instances
        /// </summary>
        private void RefreshInstances()
        {
            //Only need to do this this for multiple-instance categories
            var multipleInstanceCategoryConfigs = _categoryInfos
                .Where(ci => _multipleInstanceCategoryCounters.ContainsKey(ci.CategoryName))
                .ToList();
            foreach(var categoryInfo in multipleInstanceCategoryConfigs)
            {
                string categoryName = categoryInfo.CategoryName;
                var performanceCounterCategory = new PerformanceCounterCategory(categoryName);
                (HashSet<string> counterNames, IList<Regex> counterPatterns) =
                    GetNamesAndPatterns(categoryInfo.CounterFilters);
                LoadInstancesForCategory(categoryInfo, 
                    categoryInfo.CategoryName, 
                    performanceCounterCategory, 
                    counterNames, 
                    counterPatterns, 
                    false);
            }
        }

        private void ReadSingleInstanceCategoryCounters(IList<KeyValuePair<MetricKey, MetricValue>> metrics)
        {
            foreach (var category in _singleInstanceCategoryCounters.Keys)
            {
                var counters = _singleInstanceCategoryCounters[category];
                foreach (var counter in counters)
                {
                    var counterName = counter.CounterName;
                    try
                    {
                        metrics.Add(new KeyValuePair<MetricKey, MetricValue>
                            (
                                new MetricKey { Name = counterName, Category = category },
                                new MetricValue((long)counter.NextValue(), GetMetricUnit(category, counterName))
                            ));
                    }
                    catch (Exception ex)
                    {
                        _context.Logger?.LogError($"Error reading performance counter {category}.{counterName}: {ex.ToMinimized()}");
                    }
                }
            }
        }
        private void ReadMultipleInstanceCategoryCounters(IList<KeyValuePair<MetricKey, MetricValue>> metrics)
        {
            foreach (var category in _multipleInstanceCategoryCounters.Keys)
            {
                var instances = _multipleInstanceCategoryCounters[category];
                foreach (var instanceName in instances.Keys)
                {
                    try
                    {
                        var counters = instances[instanceName];
                        foreach (var counter in counters)
                        {
                            var counterName = counter.CounterName;
                                metrics.Add(new KeyValuePair<MetricKey, MetricValue>
                                    (
                                        new MetricKey { Name = counterName, Id=instanceName, Category = category },
                                        new MetricValue((long)counter.NextValue(), GetMetricUnit(category, counterName))
                                    ));
                        }
                    }
                    catch(InvalidOperationException ioex)
                    {
                        //Instance no longer exist
                        _context.Logger?.LogInformation($"Instance no longer exists for category {category} instance {instanceName}: {ioex}");
                    }
                    catch (Exception ex)
                    {
                        _context.Logger?.LogError($"Error reading performance counter {category} instance {instanceName}: {ex.ToMinimized()}");
                    }
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
