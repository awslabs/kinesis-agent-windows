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
