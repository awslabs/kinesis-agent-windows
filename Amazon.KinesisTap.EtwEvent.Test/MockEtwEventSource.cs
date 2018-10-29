using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Diagnostics.Tracing;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Windows;

namespace Amazon.KinesisTap.EtwEvent.Test
{
    /// <summary>
    /// An ETW event source which fakes a real Event Tracing for Windows experience without using that Windows feature.
    /// </summary>
    public class MockEtwEventSource : EtwEventSource
    {
        /// <summary>
        /// What thread we are pretending the event was raised on.
        /// </summary>
        public const int MockThreadID = 8568;

        /// <summary>
        /// Whether this provider is enabled.
        /// </summary>
        public bool IsProviderEnabled { get; set; } = false;

        public MockEtwEventSource(string providerName, TraceEventLevel traceLevel, ulong matchAnyKeywords, IPlugInContext context) : base(providerName, traceLevel, matchAnyKeywords, context)
        {
        }

        /// <summary>
        /// Pretend to enable this provider.
        /// </summary>
        protected override void EnableProvider()
        {
            //We don't actually want to enable the provider because this is a unit test and we want to minimize dependencies, but we'd like to make sure this method is called.
            IsProviderEnabled = true;
        }

        /// <summary>
        /// Pretend to obtain events by injecting a single mock event.
        /// </summary>
        protected override void GatherSourceEvents()
        {
            TraceEvent traceData = new MockTraceEvent();
            ProcessTraceEvent(traceData);

            DisposeSourceAndSession();
        }

        /// <summary>
        /// Create and populate a mock event wrapped by a mock envelope
        /// </summary>
        /// <param name="traceData">A mock ETW event to pull the data from</param>
        /// <returns>The mock envelope</returns>
        protected override EtwEventEnvelope WrapTraceEvent(TraceEvent traceData)
        {
            var envelope = new MockEtwEventEnvelope(traceData);
            envelope.Data.ExecutingThreadID = MockThreadID;
            return envelope;
        }
    }
}
