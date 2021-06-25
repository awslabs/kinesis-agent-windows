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
            _metrics?.InitializeCounters(Id, MetricsConstants.CATEGORY_SINK, CounterTypeEnum.Increment,
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
