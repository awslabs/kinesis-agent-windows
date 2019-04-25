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
using System.Threading.Tasks;

using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.Windows
{
    internal class WindowsNetworkStatus : INetworkStatus
    {
        private bool _isAvailable;

        public WindowsNetworkStatus()
        {
            NetworkChange.NetworkAddressChanged += NetworkChange_NetworkAddressChanged;
            NetworkChange.NetworkAvailabilityChanged += NetworkChange_NetworkAvailabilityChanged;
            _isAvailable = IsNetworkAvailable();

        }

        public bool IsAvailable()
        {
            return _isAvailable;
        }


        private void NetworkChange_NetworkAddressChanged(object sender, EventArgs e)
        {
            _isAvailable = IsNetworkAvailable();
        }

        private void NetworkChange_NetworkAvailabilityChanged(object sender, NetworkAvailabilityEventArgs e)
        {
            _isAvailable = IsNetworkAvailable();
        }

        private bool IsNetworkAvailable()
        {
            // only recognizes changes related to Internet adapters
            if (NetworkInterface.GetIsNetworkAvailable())
            {
                // however, this is not always reliable so we check individual interfaces
                foreach (NetworkInterface networkInterface in NetworkInterface.GetAllNetworkInterfaces())
                {
                    // filter so we see only Internet adapters
                    if (networkInterface.OperationalStatus == OperationalStatus.Up)
                    {
                        if ((networkInterface.NetworkInterfaceType != NetworkInterfaceType.Tunnel) &&
                            (networkInterface.NetworkInterfaceType != NetworkInterfaceType.Loopback))
                        {
                            var statistics = networkInterface.GetIPStatistics();
                            if ((statistics.BytesReceived > 0) &&
                                (statistics.BytesSent > 0))
                            {
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        }

    }
}
