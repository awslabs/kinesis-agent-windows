using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core.Metrics
{
    public interface IMetrics
    {
        void PublishCounter(string id, string category, CounterTypeEnum counterType, string counter, long value, MetricUnit unit);
        void PublishCounters(string id, string category, CounterTypeEnum counterType, IDictionary<string, MetricValue> counters);
        void InitializeCounters(string id, string category, CounterTypeEnum counterType, IDictionary<string, MetricValue> counters);
    }
}
