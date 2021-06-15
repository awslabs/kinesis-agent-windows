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
using System.Linq;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;

namespace Amazon.KinesisTap.Core.EMF
{
    public class CloudWatchMetric
    {
        private readonly MetricScope _scope;
        private HashSet<string> _uniqueDimensions;

        public string Namespace { get; private set; }

        public HashSet<string[]> Dimensions { get; private set; } = new HashSet<string[]>();

        public HashSet<MetricValue> Metrics { get; private set; } = new HashSet<MetricValue>();

        [JsonIgnore]
        public HashSet<string> UniqueDimensions
        {
            get
            {
                if (_uniqueDimensions == null)
                    _uniqueDimensions = new HashSet<string>(Dimensions
                    .SelectMany(i => i)
                    .Distinct());

                return _uniqueDimensions;
            }
        }

        public CloudWatchMetric(MetricScope scope, string @namespace)
        {
            _scope = scope;
            Namespace = @namespace;
        }

        public CloudWatchMetric(IConfiguration configuration)
        {
            Namespace = configuration["Namespace"];
            if (string.IsNullOrWhiteSpace(Namespace))
                throw new Exception("Property 'Namespace' is mandatory in the 'MetricDefinition' section.");

            // The Metrics section is required, so throw an exception if it is missing.
            var metricsSections = configuration.GetSection("Metrics");
            // GetSection never returns null
            if (!metricsSections.Exists())
                throw new Exception("Property 'Metrics' is mandatory in the 'MetricDefinition' section.");

            foreach (var dim in metricsSections.GetChildren())
            {
                var mv = new MetricValue
                {
                    Name = dim["Name"],
                    Unit = dim["Unit"] ?? "None"
                };

                if (dim["Value"] != null && long.TryParse(dim["Value"], out long defaultValue))
                    mv.Value = defaultValue;

                Metrics.Add(mv);
            }

            var dimensionSection = configuration.GetSection("Dimensions");
            if (!dimensionSection.Exists())
            {
                // Add an empty dimensions array if no dimensions are specified.
                Dimensions.Add(new string[0]);
            }
            else
            {
                var dimArray = dimensionSection.GetChildren().ToArray();

                // When the first item's path ends with 1, that means the first element in the array was empty.
                if (dimArray.First().Path.EndsWith("1"))
                    Dimensions.Add(new string[0]);

                foreach (var dim in dimArray)
                {
                    var dimItems = dim.GetChildren();
                    Dimensions.Add(dimItems.Select(i => i.Value).Distinct().OrderBy(i => i).ToArray());
                }
            }
        }

        public CloudWatchMetric AddDimension(HashSet<Dimension> dimensions)
        {
            // If scope was not set in constructor, this method should do nothing.
            if (_scope == null) return this;

            if (dimensions == null || dimensions.Count == 0)
            {
                Dimensions.Add(new string[0]);
                return this;
            }

            foreach (var dimension in dimensions)
            {
                if (!_scope.DimensionValues.ContainsKey(dimension.Name))
                {
                    _scope.DimensionValues[dimension.Name] = dimension.Value;
                }
            }

            Dimensions.Add(dimensions.Select(i => i.Name).ToArray());

            return this;
        }

        public CloudWatchMetric AddDimension(HashSet<Dimension> dimensions, HashSet<string[]> dimensionGroups)
        {
            // If scope was not set in constructor, this method should do nothing.
            if (_scope == null) return this;

            if (dimensions == null || dimensions.Count == 0)
            {
                Dimensions.Add(new string[0]);
                return this;
            }

            foreach (var dimension in dimensions)
            {
                if (!_scope.DimensionValues.ContainsKey(dimension.Name))
                {
                    _scope.DimensionValues[dimension.Name] = dimension.Value;
                }
            }

            foreach (var dg in dimensionGroups)
            {
                Dimensions.Add(dg);
            }

            return this;
        }

        public CloudWatchMetric AddMetrics(HashSet<MetricValue> metrics)
        {
            // If scope was not set in constructor, this method should do nothing.
            if (_scope == null) return this;

            foreach (var metric in metrics)
            {
                if (!_scope.MetricValues.ContainsKey(metric.Name))
                    _scope.MetricValues[metric.Name] = metric.Value ?? 1;

                Metrics.Add(metric);
            }

            return this;
        }

        public CloudWatchMetric AddMetric(string name, double value, string unit)
        {
            // If scope was not set in constructor, this method should do nothing.
            if (_scope == null) return this;

            if (!_scope.MetricValues.ContainsKey(name))
                _scope.MetricValues[name] = value;

            Metrics.Add(new MetricValue { Name = name, Unit = unit });
            return this;
        }
    }
}
