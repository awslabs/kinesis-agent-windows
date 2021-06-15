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
using Amazon.KinesisTap.DiagnosticTool.Core;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.Versioning;

namespace Amazon.KinesisTap.DiagnosticTool
{
    /// <summary>
    /// The validator for the Windows performance counter source
    /// </summary>
    [SupportedOSPlatform("windows")]
    public class PerformanceCounterValidator : ISourceValidator
    {
        // The category of the windows performance counter 
        private readonly PerformanceCounterCategory[] _performanceCounterCategories = PerformanceCounterCategory.GetCategories();

        /// <summary>
        /// Validate the performance counter source section
        /// </summary>
        /// <param name="sourceSection"></param>
        /// <param name="id"></param>
        /// <param name="messages"></param>
        /// <returns></returns>
        public bool ValidateSource(IConfigurationSection sourceSection, string id, IList<string> messages)
        {

            var categories = sourceSection.GetSection("Categories").GetChildren();
            foreach (var categorySection in categories)
            {
                var categoryName = categorySection["Category"];
                var instances = categorySection["Instances"];

                // If it is multiple instance categories, then instances are required
                if (IsMultipleInstanceCategory(categoryName, messages) && instances == null)
                {
                    messages.Add($"Instances are required for the multiple instance categories: {categoryName} in source ID: {id}");
                    return false;
                }

                var counters = categorySection.GetSection("Counters").GetChildren();
                foreach (var counterSection in counters)
                {
                    // make sure the performance counter is correctly configured. E.g. the counter unit is parseable
                    if (!ValidateCounter(categoryName, counterSection, messages))
                    {
                        var counterFilter = counterSection["Counter"];
                        var unit = counterSection["Unit"];
                        messages.Add($"Unable to parse unit in source ID: {id}. The unparseable Unit is '{unit}' in Category '{categoryName}'");
                        return false;
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Detect if the performance counter source is a multiple instance category
        /// </summary>
        /// <param name="categoryName"></param>
        /// <param name="messages"></param>
        /// <returns></returns>
        private bool IsMultipleInstanceCategory(string categoryName, IList<string> messages)
        {
            foreach (var c in _performanceCounterCategories)
            {
                if (c.CategoryName.Equals(categoryName) && c.CategoryType == PerformanceCounterCategoryType.MultiInstance)
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Validate the counter in the performance counter source
        /// </summary>
        /// <param name="category"></param>
        /// <param name="counterSection"></param>
        /// <param name="messages"></param>
        /// <returns></returns>
        private bool ValidateCounter(string category, IConfigurationSection counterSection, IList<string> messages)
        {
            var counterFilter = counterSection.Value;
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
                            return false;
                        }
                    }
                }
            }
            return true;
        }
    }
}
