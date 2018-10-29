using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.KinesisTap.Windows;
using Microsoft.Diagnostics.Tracing;
using System.Net;
using System.Net.NetworkInformation;

namespace Amazon.KinesisTap.EtwEvent.Test
{
    /// <summary>
    /// A mock ETW event used to populate instances of the EtwEvent class.
    /// </summary>
    public class MockTraceEvent : TraceEvent
    {
        /// <summary>
        /// The mock process from where the event is being reported
        /// </summary>
        public const int MockProcessID = 9648;

        /// <summary>
        /// The mock summary of the mock event
        /// </summary>
        public const string MockFormattedMessage = "A nicely formatted message";

        /// <summary>
        /// The ETW provider name we are mocking
        /// </summary>
        public const string ClrProviderName = "Microsoft-Windows-DotNETRuntime";

        /// <summary>
        /// The ETW provider guid we are mocking
        /// </summary>
        public static readonly Guid ClrProviderGuid = new Guid("E13C0D23-CCBC-4E12-931B-D9CC2EEE27E4");

        /// <summary>
        /// The names of the additional mock data being provided.
        /// </summary>
        public static readonly string[] MockPayloadNames = new string[] { "AllocationAmount", "AllocationKind", "ClrInstanceID", "AllocationAmount64", "TypeID", "TypeName", "HeapIndex", "Address" };

        /// <summary>
        /// The values of the additional mock data being provided.
        /// </summary>
        public static readonly object[] MockPayloadValues = new object[] { 106804, 0,  39, 106804, 1897421536, "System.Windows.Threading.DispatcherOperationTaskSource`1[System.Object]", 0, 613292084 };

        public MockTraceEvent() : this(42, 42, "GC", Guid.NewGuid(), 42, "AllocationTick", ClrProviderGuid, ClrProviderName)
        {          
        }

        protected MockTraceEvent(int eventID, int task, string taskName, Guid taskGuid, int opcode, string opcodeName, Guid providerGuid, string providerName) : base(eventID, task, taskName, taskGuid, opcode, opcodeName, providerGuid, providerName)
        {
            
        }

        /// <summary>
        /// Verify that the data contained in the envelope is what we sent.
        /// </summary>
        /// <param name="envelope">What we received</param>
        /// <returns>True if the data contained in the envelope matches what we sent.</returns>
        public static bool ValidateData(EtwEventEnvelope envelope)
        {
            Amazon.KinesisTap.Windows.EtwEvent traceData = envelope.Data;
            return envelope.Data.ProcessID == MockProcessID
                && envelope.Data.ExecutingThreadID == MockEtwEventSource.MockThreadID
                && envelope.Data.MachineName.Equals(Amazon.KinesisTap.Windows.EtwEvent.GetFQDN())
                && traceData.FormattedMessage.Equals(MockFormattedMessage)
                && traceData.ProviderName.Equals(ClrProviderName)
                && ValidatePayload(traceData);
        }

        private static bool ValidatePayload(Windows.EtwEvent traceData)
        {
            for (int i = 0; i < MockPayloadNames.Length; i++)
            {
                if (!traceData.Payload[MockPayloadNames[i]].Equals(MockPayloadValues[i]))
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// The names of the additional data we are mocking.
        /// </summary>
        public override string[] PayloadNames => MockPayloadNames;

        protected override Delegate Target { get; set; }

        /// <summary>
        /// The values for the additional data we are mocking.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public override object PayloadValue(int index)
        {
            return MockPayloadValues[index];
        }

        /// <summary>
        /// The summary of the event we are mocking
        /// </summary>
        public override string FormattedMessage { get; } = MockFormattedMessage;

        /// <summary>
        /// The mock process which is mock reporting the mock event.S
        /// </summary>
        public override int ProcessID { get; } = MockProcessID;
        

    }
}
