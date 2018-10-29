using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Microsoft.Diagnostics.Tracing;
using System.Net;
using System.Net.NetworkInformation;

namespace Amazon.KinesisTap.Windows
{
    /// <summary>
    /// A wrapper around the event data we return to KinesisTap.  This is primarily useful as ETW data 
    /// does not contain a timestamp.  By wrapping the EtwEvent KinesisTap will provide a timestamp.
    /// </summary>
    public class EtwEventEnvelope : Envelope<EtwEvent>
    {

        public EtwEventEnvelope(TraceEvent traceData) : base(new EtwEvent(traceData))
        {
        }

    }
}
