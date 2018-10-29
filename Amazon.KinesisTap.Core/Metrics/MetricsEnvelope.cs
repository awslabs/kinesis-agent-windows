using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core.Metrics
{
    public class MetricsEnvelope : Envelope<IDictionary<string, MetricValue>>
    {
        public MetricsEnvelope(string id, string category, CounterTypeEnum counterType, IDictionary<string, MetricValue> counters) : base(counters)
        {
            this.Id = id;
            this.Category = category;
            this.CounterType = counterType;
        }

        public MetricsEnvelope(string id, string category, CounterTypeEnum counterType, IDictionary<string, MetricValue> counters, DateTime timestamp) : base(counters, timestamp)
        {
            this.Id = id;
            this.Category = category;
            this.CounterType = counterType;
        }

        public string Id { get; private set; }
        public string Category { get; private set; }
        public CounterTypeEnum CounterType { get; private set; }
    }
}
