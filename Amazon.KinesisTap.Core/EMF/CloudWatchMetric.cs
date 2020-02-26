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
namespace Amazon.KinesisTap.Core.EMF
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.Extensions.Configuration;
    using Newtonsoft.Json;

    public class CloudWatchMetric
    {
        private readonly MetricScope scope;
        private HashSet<string> uniqueDimensions;

        public string Namespace { get; private set; }

        public HashSet<string[]> Dimensions { get; private set; } = new HashSet<string[]>();

        public HashSet<MetricValue> Metrics { get; private set; } = new HashSet<MetricValue>();

        [JsonIgnore]
        public HashSet<string> UniqueDimensions
        {
            get
            {
                if (this.uniqueDimensions == null)
                    this.uniqueDimensions = new HashSet<string>(this.Dimensions
                    .SelectMany(i => i)
                    .Distinct());

                return this.uniqueDimensions;
            }
        }

        public CloudWatchMetric(MetricScope scope, string @namespace)
        {
            this.scope = scope;
            this.Namespace = @namespace;
        }

        public CloudWatchMetric(IConfiguration configuration)
        {
            this.Namespace = configuration["Namespace"];
            if (string.IsNullOrWhiteSpace(this.Namespace))
                throw new Exception("Property 'Namespace' is mandatory in the 'MetricDefinition' section.");

            // The Metrics section is required, so throw an exception if it is missing.
            var metricsSections = configuration.GetSection("Metrics");
            if (metricsSections == null)
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

                this.Metrics.Add(mv);
            }

            var dimensionSection = configuration.GetSection("Dimensions");
            if (dimensionSection == null)
            {
                // Add an empty dimensions array if no dimensions are specified.
                this.Dimensions.Add(new string[0]);
            }
            else
            {
                var dimArray = dimensionSection.GetChildren().ToArray();

                // When the first item's path ends with 1, that means the first element in the array was empty.
                if (dimArray.First().Path.EndsWith("1"))
                    this.Dimensions.Add(new string[0]);

                foreach (var dim in dimArray)
                {
                    var dimItems = dim.GetChildren();
                    this.Dimensions.Add(dimItems.Select(i => i.Value).Distinct().OrderBy(i => i).ToArray());
                }
            }
        }

        public CloudWatchMetric AddDimension(HashSet<Dimension> dimensions)
        {
            // If scope was not set in constructor, this method should do nothing.
            if (this.scope == null) return this;

            if (dimensions == null || dimensions.Count == 0)
            {
                this.Dimensions.Add(new string[0]);
                return this;
            }

            foreach (var dimension in dimensions)
            {
                if (!this.scope.DimensionValues.ContainsKey(dimension.Name))
                {
                    this.scope.DimensionValues[dimension.Name] = dimension.Value;
                }
            }

            this.Dimensions.Add(dimensions.Select(i => i.Name).ToArray());

            return this;
        }

        public CloudWatchMetric AddDimension(HashSet<Dimension> dimensions, HashSet<string[]> dimensionGroups)
        {
            // If scope was not set in constructor, this method should do nothing.
            if (this.scope == null) return this;

            if (dimensions == null || dimensions.Count == 0)
            {
                this.Dimensions.Add(new string[0]);
                return this;
            }

            foreach (var dimension in dimensions)
            {
                if (!this.scope.DimensionValues.ContainsKey(dimension.Name))
                {
                    this.scope.DimensionValues[dimension.Name] = dimension.Value;
                }
            }

            foreach (var dg in dimensionGroups)
            {
                this.Dimensions.Add(dg);
            }

            return this;
        }

        public CloudWatchMetric AddMetrics(HashSet<MetricValue> metrics)
        {
            // If scope was not set in constructor, this method should do nothing.
            if (this.scope == null) return this;

            foreach (var metric in metrics)
            {
                if (!this.scope.MetricValues.ContainsKey(metric.Name))
                    this.scope.MetricValues[metric.Name] = metric.Value ?? 1;

                this.Metrics.Add(metric);
            }

            return this;
        }

        public CloudWatchMetric AddMetric(string name, double value, string unit)
        {
            // If scope was not set in constructor, this method should do nothing.
            if (this.scope == null) return this;

            if (!this.scope.MetricValues.ContainsKey(name))
                this.scope.MetricValues[name] = value;

            this.Metrics.Add(new MetricValue { Name = name, Unit = unit });
            return this;
        }
    }
}