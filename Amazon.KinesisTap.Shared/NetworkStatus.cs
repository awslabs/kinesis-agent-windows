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
using System.Linq;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.Shared
{
    /// <summary>
    /// .net core version of NetworkStatus
    /// </summary>
    public class NetworkStatus : INetworkStatus, IIPV4Info
    {
        private bool _isAvailable;

        public NetworkStatus()
        {
            NetworkChange.NetworkAddressChanged += NetworkChange_NetworkAddressChanged;
            NetworkChange.NetworkAvailabilityChanged += NetworkChange_NetworkAvailabilityChanged;
            CheckNetworkAvailability();
        }

        #region INetworkStatus members
        public bool IsAvailable()
        {
            return _isAvailable;
        }

        public bool CanUpload(int priority)
        {
            return IsAvailable();
        }

        public bool CanDownload(int priority)
        {
            return IsAvailable();
        }
        #endregion

        public string IpAddress { get; private set; }

        public string SubnetMask { get; private set; }

        private void NetworkChange_NetworkAddressChanged(object sender, EventArgs e)
        {
            CheckNetworkAvailability();
        }

        private void NetworkChange_NetworkAvailabilityChanged(object sender, NetworkAvailabilityEventArgs e)
        {
            CheckNetworkAvailability();
        }

        private void CheckNetworkAvailability()
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
                                _isAvailable = true;
                                var ipProp = networkInterface.GetIPProperties();
                                var ipInfo = ipProp.UnicastAddresses.FirstOrDefault(ip => ip.Address.AddressFamily == AddressFamily.InterNetwork);
                                if (ipInfo == null)
                                {
                                    this.IpAddress = null;
                                    this.SubnetMask = null;
                                }
                                else
                                {
                                    this.IpAddress = ipInfo.Address.ToString();
                                    this.SubnetMask = ipInfo.IPv4Mask.ToString();
                                }
                                return;
                            }
                        }
                    }
                }
            }
            _isAvailable = false;
            this.IpAddress = null;
            this.SubnetMask = null;
        }
    }
}
