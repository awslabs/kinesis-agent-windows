using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.KinesisTap.Windows;
using Microsoft.Diagnostics.Tracing;

namespace Amazon.KinesisTap.EtwEvent.Test
{
    public class MockEtwEventEnvelope : EtwEventEnvelope
    {
 
        public MockEtwEventEnvelope(TraceEvent traceData) : base(traceData)
        {
        }

        public static bool ValidateEnvelope(EtwEventEnvelope envelope)
        {
            return MockTraceEvent.ValidateData(envelope);
        }
    }
}
