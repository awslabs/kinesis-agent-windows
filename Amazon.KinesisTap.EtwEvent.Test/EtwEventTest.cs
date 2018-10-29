using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using System.Diagnostics;
using Xunit;
using System.IO;
using Microsoft.Diagnostics.Tracing;
using System.Threading;
using Amazon.KinesisTap.Windows;

namespace Amazon.KinesisTap.EtwEvent.Test
{
    /// <summary>
    /// Unit test for the ETW event source 
    /// </summary>
    public class EtwEventTest
    {
        /// <summary>
        /// Create a mock ETW event source, start it (which injects a mock event), stop it, and confirm that a mock event was recorded and has values we 
        /// expect.
        /// </summary>
        [Fact]
        public void TestEventProcessing()
        {
            //Configure
            ListEventSink mockSink = new ListEventSink();

            using (EtwEventSource mockEtwSource = new MockEtwEventSource(MockTraceEvent.ClrProviderName, TraceEventLevel.Verbose, ulong.MaxValue, new PluginContext(null, null, null)))
            {
                mockEtwSource.Subscribe(mockSink);

                //Execute
                mockEtwSource.Start();
                mockEtwSource.Stop();
            }

            //Verify
            Assert.True(mockSink.Count == 1);
            Assert.True(mockSink[0] is EtwEventEnvelope);
            Assert.True(MockEtwEventEnvelope.ValidateEnvelope((EtwEventEnvelope)mockSink[0]), "Event envelope data or event data does not match expected values.");

        }
    }
}
