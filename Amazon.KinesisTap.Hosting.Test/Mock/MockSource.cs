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
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Hosting.Test
{
    internal class MockSource : IEventSource
    {
        internal static readonly ISubject<IEnvelope> Subject = new Subject<IEnvelope>();

        public string Id { get; set; }

        Type IEventSource.GetOutputType() => typeof(string);


        ValueTask IPlugIn.StartAsync(CancellationToken stopToken) => ValueTask.CompletedTask;

        ValueTask IPlugIn.StopAsync(CancellationToken gracefulStopToken) => ValueTask.CompletedTask;

        IDisposable IObservable<IEnvelope>.Subscribe(IObserver<IEnvelope> observer) => Subject.Subscribe(observer);
    }
}
