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
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Windows
{
    public class PerformanceCounterSourceConfigLoader
    {
        private IDictionary<(string category, string counter), MetricUnit> _counterUnitsCache;
        private IPlugInContext _context;

        public PerformanceCounterSourceConfigLoader(
            IPlugInContext context,
            IDictionary<(string category, string counter), MetricUnit> counterUnitsCache)
        {
            _context = context;
            _counterUnitsCache = counterUnitsCache;
        }

        public IList<CategoryInfo> LoadCategoriesConfig(IConfigurationSection categoriesSection)
        {
            return categoriesSection
                .GetChildren()
                .Select(categorySection => LoadCategoryConfig(categorySection))
                .ToList();
        }

        #region private members for loading config
        private CategoryInfo LoadCategoryConfig(IConfigurationSection categorySection)
        {
            string category = categorySection["Category"];
            return new CategoryInfo(
                category,
                categorySection["Instances"]?.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries),
                LoadCountersConfig(category, categorySection.GetSection("Counters"))
            );
        }

        private string[] LoadCountersConfig(string category, IConfigurationSection countersSection)
        {
            return countersSection
                .GetChildren()
                .Select(counterSection => LoadCounterConfig(category, counterSection))
                .ToArray();
        }

        private string LoadCounterConfig(string category, IConfigurationSection counterSection)
        {
            string counterFilter = counterSection.Value;
            string unit = null;
            if (counterFilter == null)
            {
                counterFilter = counterSection["Counter"];
                unit = counterSection["Unit"];
                if (!string.IsNullOrEmpty(unit))
                {
                    if (Utility.IsWildcardExpression(counterFilter))
                    {
                        _context.Logger?.LogWarning($"Configuration warning: Cannot supply unit to wildcard expression. Unit is ignored. Category: {category} Counter {counterFilter}");
                    }
                    else
                    {
                        try
                        {
                            _counterUnitsCache.Add((category, counterFilter), Utility.ParseEnum<MetricUnit>(unit.Replace("/", "")));
                        }
                        catch
                        {
                            _context.Logger?.LogWarning($"Configuration warning: Unable to parse unit. Category: {category} Counter {counterFilter} Unit {unit}");
                        }
                    }
                }
            }
            return counterFilter;
        }
        #endregion

    }
}
