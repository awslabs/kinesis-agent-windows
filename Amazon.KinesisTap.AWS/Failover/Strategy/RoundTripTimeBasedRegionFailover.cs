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
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Amazon.Runtime;

namespace Amazon.KinesisTap.AWS.Failover.Strategy
{
    /// <summary>
    /// A class for client creation based on shortest round trip time region selection.
    /// </summary>
    public class RoundTripTimeBasedRegionFailover<TAWSClient> : FailoverStrategy<TAWSClient> where TAWSClient : AmazonServiceClient
    {
        /// <summary>
        /// Check service health.
        /// </summary>
        protected readonly IFailoverSink<TAWSClient>.CheckServiceReachable _checkServiceReachable = null;

        /// <summary>
        /// Initializes a new instance of the <see cref="RoundTripTimeBasedRegionFailover{TAWSClient}"/> class.
        /// </summary>
        /// <param name="context">The <see cref="IPlugInContext"/> that contains configuration info, logger, metrics etc.</param>
        /// <param name="regionResetWindowInMillis">Time interval to reset region availability after failure.</param>
        /// <param name="createClient">Handler to create AWS service clients.</param>
        /// <param name="checkServiceReachable">Function to check service health.</param>
        public RoundTripTimeBasedRegionFailover(IPlugInContext context, long regionResetWindowInMillis,
            IFailoverStrategy<TAWSClient>.CreateClient createClient, IFailoverSink<TAWSClient>.CheckServiceReachable checkServiceReachable)
            : base(context, regionResetWindowInMillis, createClient)
        {
            // Check service health callback
            _checkServiceReachable = checkServiceReachable;
        }

        /// <inheritdoc/>
        public override TAWSClient GetPrimaryRegionClient()
        {
            // Get Client Credentials
            var (credentials, _)
                = AWSUtilities.GetAWSCredentialsRegion(_context);

            // Setup Client with Primary Region
            // Region selection based on shortest rount trip time
            foreach (var regionEndpoint in Sorted(credentials, _supportedRegions).Result)
            {
                TAWSClient client = GetOrCreateRegionClient(credentials, regionEndpoint, true, true);
                if (client is not null) return client;
            }

            return null;
        }

        /// <inheritdoc/>
        public override TAWSClient GetSecondaryRegionClient()
        {
            // Refresh Credentials
            // Get Client Credentials
            var (credentials, _)
                = AWSUtilities.GetAWSCredentialsRegion(_context);

            // Setup Client with Secondary Region
            // Region selection based on shortest rount trip time
            foreach (var regionEndpoint in Sorted(credentials, _supportedRegions).Result)
            {
                TAWSClient client = GetOrCreateRegionClient(credentials, regionEndpoint);
                if (client is not null) return client;
            }

            return null;
        }

        /// <summary>
        /// Get round trip time based sorted regions.
        /// </summary>
        /// <param name="credentials">Instance of <see cref="AWSCredentials"/> class.</param>
        /// <param name="supportedRegions">Instance of <see cref="List{RegionEndpoint}"/></param>
        /// <returns>Instance of <see cref="List{RegionEndpoint}"/></returns>
        protected async Task<List<RegionEndpoint>> Sorted(AWSCredentials credentials, List<RegionEndpoint> supportedRegions)
        {
            var sortedSupportedRegions = new List<RegionEndpoint>();
            var sortedSupportedRegionsRTT = new List<double>();

            foreach (var regionEndpoint in supportedRegions)
            {
                // Get Client
                TAWSClient client = GetOrCreateRegionClient(credentials, regionEndpoint, false);
                if (client is null) continue;

                // Check service reachable
                (var running, var roundTripTime) = await _checkServiceReachable(client);
                if (running)
                {
                    sortedSupportedRegions.Add(regionEndpoint);
                    sortedSupportedRegionsRTT.Add(roundTripTime);
                }
            }

            // Sorted by RTT
            return Enumerable
                .Zip(sortedSupportedRegions, sortedSupportedRegionsRTT)
                .OrderBy(x => x.Second)
                .Select(x => x.First)
                .ToList();
        }
    }
}
