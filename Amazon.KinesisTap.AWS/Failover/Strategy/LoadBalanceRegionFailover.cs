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
using Amazon.KinesisTap.Core;
using Amazon.Runtime;

namespace Amazon.KinesisTap.AWS.Failover.Strategy
{
    /// <summary>
    /// A class for client creation based on load balance region selection.
    /// </summary>
    public class LoadBalanceRegionFailover<TAWSClient> : FailoverStrategy<TAWSClient> where TAWSClient : AmazonServiceClient
    {
        /// <summary>
        /// Gets a Random that uses a consistent seed so that the value is random, but always produces
        /// a consistent result when called with the same parameters.
        /// </summary>
        public static readonly Random ConsistentRandom = new Random(Utility.ComputerName.GetHashCode());

        /// <summary>
        /// Initializes a new instance of the <see cref="LoadBalanceRegionFailover{TAWSClient}"/> class.
        /// </summary>
        /// <param name="context">The <see cref="IPlugInContext"/> that contains configuration info, logger, metrics etc.</param>
        /// <param name="regionResetWindowInMillis">Time interval to reset region availability after failure.</param>
        /// <param name="createClient">Handler to create AWS service clients.</param>
        public LoadBalanceRegionFailover(IPlugInContext context, long regionResetWindowInMillis, IFailoverStrategy<TAWSClient>.CreateClient createClient)
            : base(context, regionResetWindowInMillis, createClient)
        {
        }

        /// <inheritdoc/>
        public override TAWSClient GetSecondaryRegionClient()
        {
            // Refresh Credentials
            // Get Client Credentials
            var (credentials, _)
                = AWSUtilities.GetAWSCredentialsRegion(_context);

            // Setup Client with Secondary Region
            // Region selection based on random choice
            foreach (var regionEndpoint in Shuffle(_supportedRegions))
            {
                TAWSClient client = GetOrCreateRegionClient(credentials, regionEndpoint);
                if (client is not null) return client;
            }

            return null;
        }

        /// <summary>
        /// Get randomly shuffled items.
        /// </summary>
        /// <param name="supportedRegions">Instance of <see cref="List{RegionEndpoint}"/></param>
        /// <returns>Instance of <see cref="List{RegionEndpoint}"/></returns>
        protected List<RegionEndpoint> Shuffle(List<RegionEndpoint> supportedRegions)
        {
            return supportedRegions.OrderBy(x => ConsistentRandom.NextDouble()).ToList();
        }
    }
}
