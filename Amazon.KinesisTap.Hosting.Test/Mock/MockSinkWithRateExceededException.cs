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
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Hosting.Test
{
    internal class MockSinkWithRateExceededException : IEventSink
    {
        private readonly int _noOfExceptions;
        private int _thrownCount = 0;

        public static readonly ConcurrentStack<IEnvelope> Items = new ConcurrentStack<IEnvelope>();

        public MockSinkWithRateExceededException(int numberOfExceptions)
        {
            _noOfExceptions = numberOfExceptions;
        }

        public string Id { get; set; }

        public void OnCompleted()
        {
        }

        public void OnError(Exception error) => throw new NotImplementedException();

        public void OnNext(IEnvelope value) => Items.Push(value);

        public ValueTask StartAsync(CancellationToken stopToken)
        {
            if (Interlocked.Increment(ref _thrownCount) <= _noOfExceptions)
            {
                throw new RateExceededException("Fake exception", null);
            }
            return ValueTask.CompletedTask;
        }

        public ValueTask StopAsync(CancellationToken gracefulStopToken)
        {
            return ValueTask.CompletedTask;
        }
    }
}
