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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Provide the general networks status for the machine (i.e. how it's connected to the internet)
    /// </summary>
    public class DefaultNetworkStatusProvider : INetworkStatusProvider
    {
        private const string ConnectionEndpoint = "aws.amazon.com";

        private readonly ILogger _logger;
        private readonly SemaphoreSlim _lock = new(1, 1);
        private CancellationToken _stopToken = default;
        private int _isAvailable;
        private UnicastIPAddressInformation _ipInfo;

        public UnicastIPAddressInformation IPInfo => _ipInfo;
        public string Id
        {
            get => nameof(DefaultNetworkStatusProvider);
            set => throw new InvalidOperationException("Cannot set ID for DefaultNetworkStatusProvider");
        }

        public DefaultNetworkStatusProvider(ILogger logger)
        {
            _logger = logger;
            NetworkChange.NetworkAddressChanged += NetworkChange_NetworkAddressChanged;
            NetworkChange.NetworkAvailabilityChanged += NetworkChange_NetworkAvailabilityChanged;
        }

        public async ValueTask StartAsync(CancellationToken stopToken)
        {
            await CheckNetworkInformation(stopToken);
            _stopToken = stopToken;
        }

        public ValueTask StopAsync(CancellationToken gracefulStopToken) => ValueTask.CompletedTask;

        #region INetworkStatus members

        public bool IsAvailable() => _isAvailable > 0;

        public bool CanUpload(int priority) => _isAvailable > 0;

        public bool CanDownload(int priority) => _isAvailable > 0;

        #endregion

        private async void NetworkChange_NetworkAddressChanged(object sender, EventArgs e)
        {
            _logger.LogInformation("Network address changed");
            await CheckNetworkInformation(_stopToken);
        }

        private async void NetworkChange_NetworkAvailabilityChanged(object sender, NetworkAvailabilityEventArgs e)
        {
            _logger.LogInformation("Network availability changed");
            await CheckNetworkInformation(_stopToken);
        }

        private async Task CheckNetworkInformation(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Querying network address information");

            try
            {
                await _lock.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            try
            {
                // try to figure out Internet-facing IP
                var (isAvai, addrInfo) = await QueryAddressInfoFromConnection(cancellationToken);

                if (addrInfo is null)
                {
                    // cannot connect to the Internet
                    // this could be due to lost connection or the machine is behind VPC, so we need to query the address info from device.
                    (isAvai, addrInfo) = await QueryAddressInfoFromDevice(cancellationToken);
                }

                _logger.LogInformation("Network availability: {0}, IP: {1}",
                    isAvai, addrInfo is null ? "none" : addrInfo.Address.ToString());
                Interlocked.Exchange(ref _isAvailable, isAvai ? 1 : 0);
                Interlocked.Exchange(ref _ipInfo, addrInfo);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            finally
            {
                _lock.Release();
            }
        }

        private async Task<(bool, UnicastIPAddressInformation)> QueryAddressInfoFromConnection(CancellationToken cancellationToken)
        {
            for (var attempt = 0; attempt < 2; attempt++)
            {
                try
                {
                    _logger.LogInformation("Probing Internet connection");
                    var addrInfoList = await Utility.GetAddrInfoOfConnectionAsync(ConnectionEndpoint, 443, 2500, cancellationToken);
                    var addrInfo = addrInfoList.FirstOrDefault();
                    if (addrInfo is null)
                    {
                        return (false, null);
                    }

                    return (true, addrInfo);
                }
                catch (SocketException ex)
                {
                    _logger.LogWarning(ex, "Problem connecting to the Internet (host {0})", ConnectionEndpoint);
                    await Task.Delay(2000, cancellationToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error querying address from connection");
                    break;
                }
            }
            return (false, null);
        }

        private async ValueTask<(bool, UnicastIPAddressInformation)> QueryAddressInfoFromDevice(CancellationToken cancellationToken)
        {
            var attempts = 0;
            do
            {
                try
                {
                    if (!NetworkInterface.GetIsNetworkAvailable())
                    {
                        goto NoNetwork;
                    }

                    // get a list of possible network interfaces
                    var networkInterfaceCandidates = NetworkInterface.GetAllNetworkInterfaces()
                        .Where(n => n.OperationalStatus == OperationalStatus.Up &&
                            n.NetworkInterfaceType != NetworkInterfaceType.Tunnel &&
                            n.NetworkInterfaceType != NetworkInterfaceType.Loopback)
                        .Where(n => n.GetIPProperties().UnicastAddresses.Any(
                            ip => ip.Address.AddressFamily == AddressFamily.InterNetwork || ip.Address.AddressFamily == AddressFamily.InterNetworkV6))
                        .ToList();

                    if (networkInterfaceCandidates.Count == 0)
                    {
                        goto NoNetwork;
                    }

                    // try getting the interface with existing traffic
                    var networkInterface = networkInterfaceCandidates.FirstOrDefault(n =>
                    {
                        var statistics = n.GetIPStatistics();
                        return statistics.BytesSent > 0 && statistics.BytesReceived > 0;
                    });
                    // if there's no such interface just use the first one
                    if (networkInterface is null)
                    {
                        networkInterface = networkInterfaceCandidates.First();
                    }
                    var unicastIPAddresses = networkInterface.GetIPProperties().UnicastAddresses;
                    // first try to find IPv4 address, sometimes UnicastAddresses will include both IPv4 and IPv6, we prefer the IPv4 one
                    var addrInfo = unicastIPAddresses.FirstOrDefault(ip => ip.Address.AddressFamily == AddressFamily.InterNetwork);
                    if (addrInfo is null)
                    {
                        addrInfo = unicastIPAddresses.FirstOrDefault(ip => ip.Address.AddressFamily == AddressFamily.InterNetworkV6);
                    }

                    return (true, addrInfo);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error getting network information");
                    await Task.Delay(1000, cancellationToken);
                }
            } while (attempts++ < 5);

            // if we reach this point, it means that the agent is unable to figure out IP address somehow.
            // in this case we're just going to assume that network is on, but no IP is detected
            return (true, null);
NoNetwork:
            return (false, null);
        }
    }
}
