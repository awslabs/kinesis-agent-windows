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
using System.Text;

namespace Amazon.KinesisTap.Core.Test
{
    internal class MockDependentEventSource<T> : DependentEventSource<T>
    {
        public MockSourceStates State { get; private set; } = MockSourceStates.Uninitialized;

        public MockDependentEventSource(IPlugInContext context) : base(new MockDependency(), context)
        {
            State = MockSourceStates.Initialized;
        }

        public bool IsAvailable
        {
            get => ((MockDependency)_dependency).IsAvailable;
            set
            {
                ((MockDependency)_dependency).IsAvailable = value;
            }     
        }


        public override void Start()
        {
            if (_dependency.IsDependencyAvailable())
            {
                State = MockSourceStates.Started;
            }
            else
            {
                Reset();
            }
        }

        public override void Stop()
        {
            State = MockSourceStates.Stopped;
        }

        public override void Reset()
        {
            Stop();
            base.Reset();
        }

        public override IDisposable Subscribe(IObserver<IEnvelope<T>> observer)
        {
            return null;
        }

        protected override void AfterDependencyAvailable()
        {
            Start();
        }
    }
}
