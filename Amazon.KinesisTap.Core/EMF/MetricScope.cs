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
    using System.IO;
    using Newtonsoft.Json;

    public class MetricScope
    {
        public MetricScope(string version = "0")
        {
            this.EventTimestamp = DateTime.UtcNow;
            this.Version = version;
        }

        public DateTime EventTimestamp { get; private set; }

        public long Timestamp => Utility.ToEpochMilliseconds(this.EventTimestamp);

        public string Version { get; private set; }

        public List<CloudWatchMetric> CloudWatchMetrics { get; private set; } = new List<CloudWatchMetric>();

        public Dictionary<string, double> MetricValues { get; private set; } = new Dictionary<string, double>();

        public Dictionary<string, string> Properties { get; private set; } = new Dictionary<string, string>();

        public Dictionary<string, string> DimensionValues { get; private set; } = new Dictionary<string, string>();

        public MetricScope AddCloudWatchMetric(string @namespace, string metricName, double metricValue, string unit, HashSet<Dimension> dimensions = null)
        {
            this.CloudWatchMetrics.Add(new CloudWatchMetric(this, @namespace).AddMetric(metricName, metricValue, unit).AddDimension(dimensions));
            return this;
        }

        public MetricScope AddCloudWatchMetric(string @namespace, string metricName, double metricValue, string unit, HashSet<Dimension> dimensions, HashSet<string[]> dimensionGroups)
        {
            this.CloudWatchMetrics.Add(new CloudWatchMetric(this, @namespace).AddMetric(metricName, metricValue, unit).AddDimension(dimensions, dimensionGroups));
            return this;
        }

        public MetricScope AddCloudWatchMetrics(string @namespace, HashSet<MetricValue> metrics, HashSet<Dimension> dimensions = null)
        {
            this.CloudWatchMetrics.Add(new CloudWatchMetric(this, @namespace).AddDimension(dimensions).AddMetrics(metrics));
            return this;
        }

        public MetricScope AddCloudWatchMetrics(string @namespace, HashSet<MetricValue> metrics, HashSet<Dimension> dimensions, HashSet<string[]> dimensionGroups)
        {
            this.CloudWatchMetrics.Add(new CloudWatchMetric(this, @namespace).AddDimension(dimensions, dimensionGroups).AddMetrics(metrics));
            return this;
        }

        public MetricScope AddProperty(string propertyName, string propertyValue)
        {
            if (!string.IsNullOrWhiteSpace(propertyName) && !string.IsNullOrWhiteSpace(propertyValue))
                this.Properties[propertyName] = propertyValue;
            return this;
        }

        public void Flush()
        {
            Console.WriteLine(this.ToString());
        }

        public override string ToString()
        {
            using (var sw = new StringWriter())
            using (var tw = new JsonTextWriter(sw))
            {
                tw.WriteStartObject();

                tw.WritePropertyName(nameof(Timestamp));
                tw.WriteValue(this.Timestamp);
                tw.WritePropertyName(nameof(Version));
                tw.WriteValue(this.Version);

                tw.WritePropertyName(nameof(CloudWatchMetrics));
                tw.WriteStartArray();

                foreach (var m in this.CloudWatchMetrics)
                {
                    tw.WriteStartObject();

                    // Namespace
                    tw.WritePropertyName(nameof(m.Namespace));
                    tw.WriteValue(m.Namespace);

                    // Dimensions
                    tw.WritePropertyName(nameof(m.Dimensions));
                    tw.WriteStartArray();
                    foreach (var d in m.Dimensions)
                    {
                        tw.WriteStartArray();
                        foreach (var dv in d)
                            tw.WriteValue(dv);
                        tw.WriteEndArray();
                    }
                    tw.WriteEndArray();

                    // Metrics
                    tw.WritePropertyName(nameof(m.Metrics));
                    tw.WriteStartArray();
                    foreach (var d in m.Metrics)
                    {
                        tw.WriteStartObject();
                        tw.WritePropertyName(nameof(d.Name));
                        tw.WriteValue(d.Name);
                        tw.WritePropertyName(nameof(d.Unit));
                        tw.WriteValue(d.Unit);
                        tw.WriteEndObject();
                    }
                    tw.WriteEndArray();

                    tw.WriteEndObject();
                }

                tw.WriteEndArray();

                foreach (var dim in this.DimensionValues)
                {
                    tw.WritePropertyName(dim.Key);
                    tw.WriteValue(dim.Value);
                }

                foreach (var metric in this.MetricValues)
                {
                    tw.WritePropertyName(metric.Key);
                    tw.WriteValue(metric.Value);
                }

                if (this.Properties.Count > 0)
                {
                    foreach (var kvp in this.Properties)
                    {
                        tw.WritePropertyName(kvp.Key);
                        tw.WriteValue(kvp.Value);
                    }
                }

                tw.WriteEndObject();
                return sw.ToString();
            }
        }
    }
}