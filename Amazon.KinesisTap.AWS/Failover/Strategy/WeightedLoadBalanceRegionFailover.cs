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
using Amazon.KinesisTap.AWS.Failover.Extensions;
using Amazon.KinesisTap.Core;
using Amazon.Runtime;
using Microsoft.Extensions.Configuration;

namespace Amazon.KinesisTap.AWS.Failover.Strategy
{
    /// <summary>
    /// A class for client creation based on weighted load balance region selection.
    /// </summary>
    public class WeightedLoadBalanceRegionFailover<TAWSClient> : FailoverStrategy<TAWSClient> where TAWSClient : AmazonServiceClient
    {
        /// <summary>
        /// Gets a Random that uses a consistent seed so that the value is random, but always produces
        /// a consistent result when called with the same parameters.
        /// </summary>
        public static readonly Random ConsistentRandom = new Random(Utility.ComputerName.GetHashCode());

        #region Internal Classes
        /// <summary>
        /// A class for storing Region state.
        /// </summary>
        protected class RegionState
        {
            /// <summary>
            /// Flag to mark region available.
            /// </summary>
            public bool IsAvailable { get; set; }

            /// <summary>
            /// Region endpoint.
            /// </summary>
            public RegionEndpoint Region { get; set; }

            /// <summary>
            /// Region weight.
            /// </summary>
            public int RegionWeight { get; set; }

            /// <summary>
            /// Initializes a new instance of the <see cref="RegionState"/> class.
            /// </summary>
            /// <param name="regionEndpoint">Region endpoint.</param>
            /// <param name="regionWeight">Region weight.</param>
            public RegionState(RegionEndpoint regionEndpoint, int regionWeight)
            {
                Region = regionEndpoint;
                RegionWeight = regionWeight;
                IsAvailable = true;
            }
        }
        #endregion

        /// <summary>
        /// Instance of <see cref="List{RegionEndpoint}"/>
        /// </summary>
        protected readonly List<int> _supportedRegionsWeights = new List<int>();

        /// <summary>
        /// Initializes a new instance of the <see cref="WeightedLoadBalanceRegionFailover{TAWSClient}"/> class.
        /// </summary>
        /// <param name="context">The <see cref="IPlugInContext"/> that contains configuration info, logger, metrics etc.</param>
        /// <param name="regionResetWindowInMillis">Time interval to reset region availability after failure.</param>
        /// <param name="createClient">Handler to create AWS service clients.</param>
        public WeightedLoadBalanceRegionFailover(IPlugInContext context, long regionResetWindowInMillis, IFailoverStrategy<TAWSClient>.CreateClient createClient)
            : base(context, regionResetWindowInMillis, createClient)
        {
            // Parse supported regions weights
            ParseSupportedRegionsWeights();
        }

        /// <inheritdoc/>
        public override TAWSClient GetSecondaryRegionClient()
        {
            // Refresh Credentials
            // Get Client Credentials
            var (credentials, _)
                = AWSUtilities.GetAWSCredentialsRegion(_context);

            // Setup Client with Secondary Region
            // Region selection based on weighted random choice
            var supportedRegions = Enumerable
                .Zip(_supportedRegions, _supportedRegionsWeights)
                .Select(x => new RegionState(x.First, x.Second))
                .OrderBy(x => ConsistentRandom.NextDouble())
                .ToList();
            while (supportedRegions.Any(x => x.IsAvailable))
            {
                // Get Weighted Random Region
                var regionState = Shuffle(supportedRegions.Where(x => x.IsAvailable)
                    .ToList());
                // Mark Selected Random Region unavailable
                regionState.IsAvailable = false;

                TAWSClient client = GetOrCreateRegionClient(credentials, regionState.Region);
                if (client is not null) return client;
            }

            return null;
        }

        /// <summary>
        /// Get weighted shuffled item.
        /// </summary>
        /// <param name="supportedRegions">Instance of <see cref="List{RegionState}"/></param>
        /// <returns>Instance of <see cref="List{RegionEndpoint}"/></returns>
        protected RegionState Shuffle(List<RegionState> supportedRegions)
        {
            // Random selection by Alias Method
            return supportedRegions[new Random()
                .GetAlias(supportedRegions.Select(x => x.RegionWeight)
                .ToList())];
        }

        /// <summary>
        /// Parse supported regions weights from list and update store.
        /// </summary>
        protected virtual void ParseSupportedRegionsWeights()
        {
            IConfiguration _config = _context.Configuration;

            // Parse supported regions from config for failover
            var supportedRegionsWeights = _config.GetSection(ConfigConstants.SUPPORTED_REGIONS_WEIGHTS) is not null
                ? _config.GetSection(ConfigConstants.SUPPORTED_REGIONS_WEIGHTS).Get<List<int>>()
                : null;

            // Valid and non-empty list
            if (supportedRegionsWeights is null || supportedRegionsWeights.Count == 0 || supportedRegionsWeights.Count != _supportedRegions.Count)
            {
                throw new ArgumentException(String.Format("Missing or empty supported regions weights, please provide \"{0}\".",
                    ConfigConstants.SUPPORTED_REGIONS_WEIGHTS));
            }

            // Update Store
            supportedRegionsWeights.ForEach(supportedRegionsWeight => _supportedRegionsWeights.Add(supportedRegionsWeight));
        }
    }
}
