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
using System.Runtime.Versioning;
using Amazon.KinesisTap.Core;
using Microsoft.Diagnostics.Tracing;

namespace Amazon.KinesisTap.Windows
{
    /// <summary>
    /// A wrapper around the event data we return to KinesisTap.  This is primarily useful as ETW data 
    /// does not contain a timestamp.  By wrapping the EtwEvent KinesisTap will provide a timestamp.
    /// </summary>
    [SupportedOSPlatform("windows")]
    public class EtwEventEnvelope : Envelope<EtwEvent>
    {

        public EtwEventEnvelope(TraceEvent traceData) : base(new EtwEvent(traceData))
        {
        }

    }
}
