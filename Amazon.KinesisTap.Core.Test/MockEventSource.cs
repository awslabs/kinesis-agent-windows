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
using System.Reactive.Subjects;

namespace Amazon.KinesisTap.Core.Test
{
    internal class MockEventSource<T> : EventSource<T>
    {
        private ISubject<IEnvelope<T>> _subject = new Subject<IEnvelope<T>>();

        public MockEventSource(IPlugInContext context) : base(context)
        {

        }

        public override void Start()
        {
        }

        public override void Stop()
        {
        }

        public override IDisposable Subscribe(IObserver<IEnvelope<T>> observer)
        {
            return _subject.Subscribe(observer);
        }

        public void MockEvent(T data)
        {
            _subject.OnNext(new Envelope<T>(data));
        }

        public void MockEvent(T data, DateTime timestamp)
        {
            _subject.OnNext(new Envelope<T>(data, timestamp));
        }

        public void MockLogEvent(T data, DateTime timestamp, string rawRecord, string filePath, long position, long lineNumber)
        {
            _subject.OnNext(new LogEnvelope<T>(data, timestamp, rawRecord, filePath, position, lineNumber));
        }
    }

    internal class NonGenericMockEventSource : MockEventSource<string>
    {
        public NonGenericMockEventSource(IPlugInContext context) : base(context)
        {
        }
    }
}
