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
 using Amazon.KinesisTap.Core.Metrics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Amazon.KinesisTap.Core.Test
{
    public class MockMetricsSink : SimpleMetricsSink
    {
        public MockMetricsSink(int defaultInterval, IPlugInContext context) : base(defaultInterval, context)
        {
        }

        public IDictionary<MetricKey, MetricValue> AccumlatedValues { get; private set; }
        public IDictionary<MetricKey, MetricValue> LastValues { get; private set; }
        public IDictionary<MetricKey, MetricValue> FilteredAccumulatedValues { get; private set; }
        public IDictionary<MetricKey, MetricValue> FilteredLastValues { get; private set; }
        public IDictionary<MetricKey, MetricValue> FilteredAggregatedAccumulatedValues { get; private set; }
        public IDictionary<MetricKey, MetricValue> FilteredAggregatedLastValues { get; private set; }

        protected override void OnFlush(IDictionary<MetricKey, MetricValue> accumlatedValues, IDictionary<MetricKey, MetricValue> lastValues)
        {
            this.AccumlatedValues = accumlatedValues;
            this.LastValues = lastValues;
            if (!string.IsNullOrWhiteSpace(_metricsFilter))
            {
                this.FilteredAccumulatedValues = FilterValues(accumlatedValues);
                this.FilteredLastValues = FilterValues(lastValues);
                this.FilteredAggregatedAccumulatedValues = FilterAndAggregateValues(accumlatedValues, 
                    values => new MetricValue(values.Sum(v => v.Value), values.First().Unit));
                this.FilteredAggregatedLastValues = FilterAndAggregateValues(lastValues, 
                    values => new MetricValue((long)values.Average(v => v.Value), values.First().Unit));
            }
        }
    }
}
