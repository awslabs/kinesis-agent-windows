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
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Reactive.Subjects;
using System.Text;
using System.Threading.Tasks;

using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.AWS.Telemetrics
{
    /// <summary>
    /// Used to send telemetry data to a sink
    /// </summary>
    public class TelemetricsSinkConnector :  EventSource<IDictionary<string, object>>, ITelemetricsClient<HttpResponseMessage>
    {
        private ISubject<IEnvelope<IDictionary<string, object>>> _eventSubject = new Subject<IEnvelope<IDictionary<string, object>>>();

        public TelemetricsSinkConnector(IPlugInContext context) : base(context) { }

        public string ClientId { get; set; }

        public string ClientIdParameterName => "CLIENT_GUID";

        public Task<string> CreateClientIdAsync()
        {
            return Task.FromResult(Guid.NewGuid().ToString());
        }

        public Task<HttpResponseMessage> PutMetricsAsync(IDictionary<string, object> data)
        {
            //Since OnNext is asynchronous and we are getting the response back, we simulate a response to satisfy the caller
            HttpResponseMessage response = null;
            try
            {
                _eventSubject.OnNext(new Envelope<IDictionary<string, object>>(data, DateTime.UtcNow));
                response = new HttpResponseMessage(HttpStatusCode.OK);
            }
            catch(Exception ex)
            {
                response = new HttpResponseMessage(HttpStatusCode.InternalServerError)
                {
                    Content = new StringContent(ex.ToString())
                };
            }
            return Task.FromResult(response);
        }

        public override void Start()
        {
        }

        public override void Stop()
        {
        }

        public override IDisposable Subscribe(IObserver<IEnvelope<IDictionary<string, object>>> observer)
        {
            return _eventSubject.Subscribe(observer);
        }
    }
}
