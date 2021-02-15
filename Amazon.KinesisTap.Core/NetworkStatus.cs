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
using System.Collections.Generic;
using System.Linq;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Provide access to network status.
    /// Due to lack of library in .net standard 1.3, the concrete class is supplied by platform specific start-up.
    /// </summary>
    public class NetworkStatus
    {
        public NetworkStatus(INetworkStatusProvider defaultProvider)
        {
            DefaultProvider = defaultProvider;
        }

        private readonly List<INetworkStatusProvider> _addtionalNetworkStatusProviders = new List<INetworkStatusProvider>();

        public INetworkStatusProvider DefaultProvider { get; }

        public bool IsAvailable()
        {
            if (DefaultProvider?.IsAvailable() != true) return false;
            return _addtionalNetworkStatusProviders.All(p => p.IsAvailable()); //All providers must indicate available
        }

        public bool CanUpload(int priority)
        {
            if (DefaultProvider?.CanUpload(priority) != true) return false;
            return _addtionalNetworkStatusProviders.All(p => p.CanUpload(priority)); //All providers must indicate OK
        }

        public bool CanDownload(int priority)
        {
            if (DefaultProvider?.CanDownload(priority) != true) return false;
            return _addtionalNetworkStatusProviders.All(p => p.CanDownload(priority)); //All providers must indicate OK
        }

        internal void RegisterNetworkStatusProvider(INetworkStatusProvider networkStatus)
        {
            _addtionalNetworkStatusProviders.Add(networkStatus);
        }

        internal void ResetNetworkStatusProviders()
        {
            _addtionalNetworkStatusProviders.Clear();
        }
    }
}
