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
using System.Text;

namespace Amazon.KinesisTap.Core.Metrics
{
    /// <summary>
    /// Data structure for value with a unit
    /// </summary>
    public class MetricValue
    {
        private long _value;
        private readonly MetricUnit _unit;

        public static MetricValue ZeroCount => new MetricValue(0);
        public static MetricValue ZeroBytes => new MetricValue(0, MetricUnit.Bytes);

        public MetricValue(long value) : this(value, MetricUnit.Count)
        {

        }

        public MetricValue(long value, MetricUnit unit)
        {
            _value = value;
            _unit = unit;
        }

        public long Value => _value;

        public MetricUnit Unit => _unit;

        public void Increment(long other)
        {
            lock (this)
            {
                this._value += other;
            }
        }

        public void Increment(MetricValue other)
        {
            if (this._unit == other.Unit)
            {
                this._value += other.Value;
            }
            else
            {
                throw new InvalidOperationException($"Unit mismatch: {this._unit} and {other.Unit}");
            }
        }
    }
}
