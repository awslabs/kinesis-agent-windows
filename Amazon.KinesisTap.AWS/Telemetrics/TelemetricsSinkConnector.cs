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
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.AWS.Telemetrics
{
    /// <summary>
    /// Used to forward telemetry data to a sink.
    /// </summary>
    public class TelemetricsSinkConnector : EventSource<IDictionary<string, object>>, ITelemetricsClient
    {
        private const string ClientIdParameterName = "CLIENT_GUID";
        private readonly ISubject<IEnvelope<IDictionary<string, object>>> _eventSubject
            = new Subject<IEnvelope<IDictionary<string, object>>>();
        private readonly IParameterStore _parameterStore;

        private string _clientId;

        public TelemetricsSinkConnector(IPlugInContext context) : base(context)
        {
            _parameterStore = context.ParameterStore;
            _clientId = _parameterStore.GetParameter(ClientIdParameterName);
        }

        /// <inheritdoc/>
        public ValueTask<string> GetClientIdAsync(CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(_clientId))
            {
                _clientId = Guid.NewGuid().ToString();
                _parameterStore.SetParameter(ClientIdParameterName, _clientId);
            }

            return ValueTask.FromResult(_clientId);
        }

        /// <inheritdoc/>
        public Task PutMetricsAsync(IDictionary<string, object> data, CancellationToken cancellationToken = default)
        {
            _eventSubject.OnNext(new Envelope<IDictionary<string, object>>(data, DateTime.UtcNow));
            return Task.CompletedTask;
        }

        /// <inheritdoc/>
        public override void Start()
        {
        }

        /// <inheritdoc/>
        public override void Stop()
        {
        }

        /// <inheritdoc/>
        public override IDisposable Subscribe(IObserver<IEnvelope<IDictionary<string, object>>> observer) => _eventSubject.Subscribe(observer);
    }
}
