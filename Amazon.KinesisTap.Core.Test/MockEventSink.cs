using Amazon.KinesisTap.Core.Metrics;
using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core.Test
{
    internal class MockEventSink : EventSink
    {
        private List<string> _records = new List<string>();

        public MockEventSink(IPlugInContext context) : base(context)
        {
        }

        public override void OnNext(IEnvelope envelope)
        {
            string record = base.GetRecord(envelope);
            if (!string.IsNullOrEmpty(record))
            {
                _records.Add(base.GetRecord(envelope));
            }
        }

        public override void Start()
        {
            _metrics?.InitializeCounters(this.Id, MetricsConstants.CATEGORY_SINK, CounterTypeEnum.Increment,
                new Dictionary<string, MetricValue>()
            {
                { "MockSink" + MetricsConstants.RECORDS_SUCCESS, MetricValue.ZeroCount },
                { "MockSink" + MetricsConstants.RECOVERABLE_SERVICE_ERRORS, MetricValue.ZeroCount },
                { "MockSink" + MetricsConstants.NONRECOVERABLE_SERVICE_ERRORS, MetricValue.ZeroCount }
            });
        }

        public override void Stop()
        {
        }

        public List<string> Records => _records;
    }
}
