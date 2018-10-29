using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Amazon.KinesisTap.DiagnosticTool
{
    public class PerformanceCounterValidator
    {
        private IConfigurationSection _categoriesSection;
        private readonly PerformanceCounterCategory[] _performanceCounterCategories;

        public PerformanceCounterValidator(IConfigurationSection categoriesSection, PerformanceCounterCategory[] performanceCounterCategories)
        {
            _categoriesSection = categoriesSection;
            _performanceCounterCategories = performanceCounterCategories;
        }

        public Boolean ValidateSource(IList<string> messages)
        {
            var categories = _categoriesSection.GetChildren();
            foreach (var categorySection in categories)
            {
                string categoryName = categorySection["Category"];
                string instances = categorySection["Instances"];

                // If it is multiple instance categories, then instances are required
                if (IsMultipleInstanceCategory(categoryName, messages) && instances == null)
                {
                    return false;
                }

                var counters = categorySection.GetSection("Counters").GetChildren();
                foreach (var counterSection in counters)
                {
                    // make sure the performance counter is correctly configured. E.g. the counter unit is parseable
                    if (!ValidateCounter(categoryName, counterSection, messages))
                    {
                        return false;
                    }
                }
            }

            return true;
        }

        private Boolean IsMultipleInstanceCategory(string categoryName, IList<string> messages)
        {
            foreach (var c in _performanceCounterCategories)
            {
                if (c.CategoryName.Equals(categoryName) && c.CategoryType == PerformanceCounterCategoryType.MultiInstance)
                {
                    return true;
                }
            }
            messages.Add($"Instances are required for the multiple instance categories: {categoryName}");
            return false;
        }


        private Boolean ValidateCounter(string category, IConfigurationSection counterSection, IList<string> messages)
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
                        // Configuration warning: Cannot supply unit to wildcard expression. Unit is ignored. But this is not misconfigured
                        return true;
                    }
                    else
                    {
                        try
                        {
                            Utility.ParseEnum<MetricUnit>(unit.Replace("/", ""));
                        }
                        catch
                        {
                            messages.Add($"Unable to parse unit. Category: {category} Counter {counterFilter} Unit {unit}");
                            return false;
                        }
                    }
                }
            }
            return true;
        }
    }
}
