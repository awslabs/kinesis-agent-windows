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
using System.Net;
using System.Net.NetworkInformation;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.Test.Common
{
    /// <summary>
    /// Always returns network available
    /// </summary>
    public class AlwaysAvailableNetworkProvider : INetworkStatusProvider
    {
        private class IpInfo : UnicastIPAddressInformation
        {
            public override long AddressPreferredLifetime => long.MaxValue;

            public override long AddressValidLifetime => long.MaxValue;

            public override long DhcpLeaseLifetime => long.MaxValue;

            public override DuplicateAddressDetectionState DuplicateAddressDetectionState => DuplicateAddressDetectionState.Preferred;

            public override IPAddress IPv4Mask => IPAddress.Parse("255.255.255.0");

            public override PrefixOrigin PrefixOrigin => PrefixOrigin.Dhcp;

            public override SuffixOrigin SuffixOrigin => SuffixOrigin.OriginDhcp;

            public override IPAddress Address => IPAddress.Parse("127.0.0.1");

            public override bool IsDnsEligible => true;

            public override bool IsTransient => false;
        }

        public UnicastIPAddressInformation IPInfo => new IpInfo();

        public string Id { get; set; }

        public bool CanDownload(int priority) => true;

        public bool CanUpload(int priority) => true;

        public bool IsAvailable() => true;

        public ValueTask StartAsync(CancellationToken stopToken) => ValueTask.CompletedTask;

        public ValueTask StopAsync(CancellationToken gracefulStopToken) => ValueTask.CompletedTask;

    }
}
