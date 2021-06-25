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
using Amazon.KinesisTap.Core;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Hosting.Test
{
    /// <summary>
    /// A mock sink used for testing. Records "uploaded" by the sinks are saved to the Records mapping corresponding to the Sinks' IDs
    /// </summary>
    public class MockListSink : IEventSink
    {
        /// <summary>
        /// Store the records streamed by the sink.
        /// Note that this will persist between tests so it needs to be cleaned up in Dispose()
        /// </summary>
        public static IDictionary<string, List<IEnvelope>> Records = new Dictionary<string, List<IEnvelope>>();

        // Mark this as volatile for to make sure tests read the up-to-date value
        private volatile bool _stopped;
        private readonly IPlugInContext _context;

        public MockListSink(IPlugInContext context)
        {
            Id = context.Configuration["Id"];
            if (!Records.ContainsKey(Id))
            {
                Records[Id] = new List<IEnvelope>();
            }

            _context = context;
            _stopped = true;
        }

        public string Id { get; set; }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnNext(IEnvelope value)
        {
            while (!_context.NetworkStatus.IsAvailable() && !_stopped)
            {
                Thread.Sleep(1000);
            }

            if (_stopped)
            {
                return;
            }

            Records[Id].Add(value);
        }

        public ValueTask StartAsync(CancellationToken stopToken)
        {
            _stopped = false;
            return ValueTask.CompletedTask;
        }

        public ValueTask StopAsync(CancellationToken gracefulStopToken)
        {
            _stopped = true;
            return ValueTask.CompletedTask;
        }
    }
}
