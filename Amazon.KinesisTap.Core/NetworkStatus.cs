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
using System.Linq;
using System.Net.NetworkInformation;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Provide access to network status.
    /// Due to lack of library in .net standard 1.3, the concrete class is supplied by platform specific start-up.
    /// </summary>
    public static class NetworkStatus
    {
        private static List<INetworkStatus> _addtionalNetworkStatusProviders = new List<INetworkStatus>();

        public static INetworkStatus CurrentNetwork { get; internal set; }

        public static bool IsAvailable()
        {
            if (!CurrentNetwork.IsAvailable()) return false;
            return _addtionalNetworkStatusProviders.All(p => p.IsAvailable()); //All providers must indicate available
        }

        public static bool CanUpload(int priority)
        {
            if (!CurrentNetwork.CanUpload(priority)) return false;
            return _addtionalNetworkStatusProviders.All(p => p.CanUpload(priority)); //All providers must indicate OK
        }

        public static bool CanDownload(int priority)
        {
            if (!CurrentNetwork.CanDownload(priority)) return false;
            return _addtionalNetworkStatusProviders.All(p => p.CanDownload(priority)); //All providers must indicate OK
        }

        internal static void RegisterNetworkStatusProvider(INetworkStatus networkStatus)
        {
            _addtionalNetworkStatusProviders.Add(networkStatus);
        }

        internal static void ResetNetworkStatusProviders()
        {
            _addtionalNetworkStatusProviders.Clear();
        }
    }
}
