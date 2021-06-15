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

namespace Amazon.KinesisTap.Core.Metrics
{
    /// <summary>
    /// Data structure for value with a unit
    /// </summary>
    public class MetricValue
    {
        public static MetricValue ZeroCount => new MetricValue(0);
        public static MetricValue ZeroBytes => new MetricValue(0, MetricUnit.Bytes);

        public MetricValue(long value) : this(value, MetricUnit.Count)
        {
        }

        public MetricValue(long value, MetricUnit unit)
        {
            Value = value;
            Unit = unit;
        }

        public long Value { get; private set; }

        public MetricUnit Unit { get; }

        public void Increment(long other)
        {
            lock (this)
            {
                Value += other;
            }
        }

        public void Increment(MetricValue other)
        {
            if (Unit == other.Unit)
            {
                Value += other.Value;
            }
            else
            {
                throw new InvalidOperationException($"Unit mismatch: {Unit} and {other.Unit}");
            }
        }
    }
}
