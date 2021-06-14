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
using System.Net.NetworkInformation;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Hosting.Test
{
    /// <summary>
    /// A mock implementation of <see cref="INetworkStatus"/> where properties can be changed globally.
    /// </summary>
    public class MockNetworkStatusProvider : INetworkStatusProvider, IGenericPlugin
    {
        private static volatile bool _isAvailablePrivate = false;

        public static void Disable()
        {
            _isAvailablePrivate = false;
        }

        public static void Enable()
        {
            _isAvailablePrivate = true;
        }

        public string Id { get => nameof(MockNetworkStatusProvider); set => throw new InvalidOperationException(); }

        public UnicastIPAddressInformation IPInfo => throw new NotImplementedException();

        public bool CanDownload(int priority) => _isAvailablePrivate;

        public bool CanUpload(int priority) => _isAvailablePrivate;

        public bool IsAvailable() => _isAvailablePrivate;

        public ValueTask StartAsync(CancellationToken stopToken) => ValueTask.CompletedTask;

        public ValueTask StopAsync(CancellationToken gracefulStopToken) => ValueTask.CompletedTask;
    }
}
