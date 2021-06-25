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
using Amazon.KinesisTap.AWS.Failover.Components;
using Amazon.KinesisTap.Core;
using Amazon.Runtime;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.AWS.Failover.Strategy
{
    /// <summary>
    /// A class for region selection strategy.
    /// </summary>
    public abstract class FailoverStrategy<TAWSClient> : IFailoverStrategy<TAWSClient> where TAWSClient : AmazonServiceClient
    {
        /// <summary>
        /// Context Logger.
        /// </summary>
        protected readonly ILogger _logger;

        /// <summary>
        /// Plugin context <see cref="IPlugInContext"/>
        /// </summary>
        protected readonly IPlugInContext _context;

        /// <summary>
        /// Instance of <see cref="FailoverRegion{RegionEndpoint}"/>
        /// </summary>
        protected FailoverRegion<RegionEndpoint> _currentRegion = null;

        /// <summary>
        /// Dictionary to store all regions <see cref="FailoverRegion{RegionEndpoint}"/>
        /// </summary>
        protected readonly Dictionary<string, FailoverRegion<RegionEndpoint>> _failoverRegions
            = new Dictionary<string, FailoverRegion<RegionEndpoint>>();

        /// <summary>
        /// Handler to create service clients.
        /// </summary>
        protected readonly IFailoverStrategy<TAWSClient>.CreateClient _createClient = null;

        /// <summary>
        /// Instance of <see cref="List{RegionEndpoint}"/>
        /// </summary>
        protected readonly List<RegionEndpoint> _supportedRegions = new List<RegionEndpoint>();

        /// <summary>
        /// Reset Window time in millis.
        /// </summary>
        private long _regionResetWindowInMillis;

        /// <summary>
        /// Initializes a new instance of the <see cref="FailoverStrategy{TAWSClient}"/> class.
        /// </summary>
        /// <param name="context">The <see cref="IPlugInContext"/> that contains configuration info, logger, metrics etc.</param>
        /// <param name="regionResetWindowInMillis">Time interval to reset region availability after failure.</param>
        /// <param name="createClient">Handler to create AWS service clients.</param>
        public FailoverStrategy(IPlugInContext context, long regionResetWindowInMillis, IFailoverStrategy<TAWSClient>.CreateClient createClient)
        {
            _context = context;
            _logger = context.Logger;

            _regionResetWindowInMillis = regionResetWindowInMillis;
            _createClient = createClient;

            // Parse supported regions
            ParseSupportedRegions();
        }

        /// <summary>
        /// Get current Region. 
        /// </summary>
        /// <returns>Instance of <see cref="FailoverRegion{RegionEndpoint}"/></returns>
        public FailoverRegion<RegionEndpoint> GetCurrentRegion()
        {
            return _currentRegion;
        }

        /// <inheritdoc/>
        public virtual TAWSClient GetPrimaryRegionClient()
        {
            // Get Client Credentials and Primary Region
            var (credentials, regionEndpoint)
                = AWSUtilities.GetAWSCredentialsRegion(_context);

            // If RegionEndpoint is null
            if (regionEndpoint is null)
            {
                regionEndpoint = FallbackRegionFactory.GetRegionEndpoint();
            }

            // Setup Client with Primary Region
            // Check Primary Region is available
            return GetOrCreateRegionClient(credentials, regionEndpoint, true, true);
        }

        /// <inheritdoc/>
        public abstract TAWSClient GetSecondaryRegionClient();

        /// <summary>
        /// Setup region and create client.
        /// </summary>
        /// <param name="credentials">Instance of <see cref="AWSCredentials"/> class.</param>
        /// <param name="regionEndpoint">Instance of <see cref="RegionEndpoint"/> class.</param>
        /// <param name="failover">Fail over to the new region.</param>
        /// <param name="freeCurrentRegion">Mark current region as free.</param>
        /// <returns>Instance of <see cref="AmazonServiceClient"/> class</returns>
        protected virtual TAWSClient GetOrCreateRegionClient(AWSCredentials credentials, RegionEndpoint regionEndpoint, bool failover = true, bool freeCurrentRegion = false)
        {
            var failoverRegion = GetOrCreateRegion(regionEndpoint);

            _logger.LogDebug("Creating client for AWS region: {0}", regionEndpoint.SystemName);
            // Check Failover Region is available
            if (failoverRegion.Available())
            {
                if (failover)
                {
                    // Mark in use
                    failoverRegion.MarkInUse();

                    // Mark current region free
                    if (freeCurrentRegion && _currentRegion is not null)
                    {
                        _currentRegion.Reset();
                    }
                    // Mark current region as down
                    else if (_currentRegion is not null)
                    {
                        _currentRegion.MarkIsDown();
                    }

                    // Store as current region
                    _currentRegion = failoverRegion;
                }

                _logger.LogInformation("Successfully created client for AWS region: {0}.", regionEndpoint.SystemName);

                return _createClient(_context, credentials, failoverRegion.Region);
            }
            else
            {
                _logger.LogDebug("Skipped creating client for AWS region: {0}, currently in use or region is down.", regionEndpoint.SystemName);
            }

            return null;
        }

        /// <summary>
        /// Parse supported regions from list and update store.
        /// </summary>
        protected virtual void ParseSupportedRegions()
        {
            IConfiguration _config = _context.Configuration;

            // Parse supported regions from config for failover
            var supportedRegions = _config.GetSection(ConfigConstants.SUPPORTED_REGIONS) is not null
                ? _config.GetSection(ConfigConstants.SUPPORTED_REGIONS).Get<List<string>>()
                : null;

            // Valid and non-empty list
            if (supportedRegions is null || supportedRegions.Count == 0)
            {
                throw new ArgumentException(String.Format("Missing or empty supported regions, please provide \"{0}\".",
                    ConfigConstants.SUPPORTED_REGIONS));
            }

            // Update Store
            supportedRegions.ForEach(supportedRegion => _supportedRegions.Add(RegionEndpoint.GetBySystemName(supportedRegion)));
        }

        /// <summary>
        /// Get or Create Region. 
        /// </summary>
        /// <param name="region">Region name.</param>
        /// <returns>Instance of <see cref="FailoverRegion{RegionEndpoint}"/></returns>
        private FailoverRegion<RegionEndpoint> GetOrCreateRegion(string region)
        {
            // If exists
            if (_failoverRegions.TryGetValue(region, out var failoverRegion))
            {
                return failoverRegion;
            }

            // If does not exists, create and store
            failoverRegion = new FailoverRegion<RegionEndpoint>(RegionEndpoint.GetBySystemName(region),
                _regionResetWindowInMillis);
            _failoverRegions.Add(region, failoverRegion);

            return failoverRegion;
        }

        /// <summary>
        /// Get or Create Region. 
        /// </summary>
        /// <param name="region">Region name.</param>
        /// <returns>Instance of <see cref="FailoverRegion{RegionEndpoint}"/></returns>
        private FailoverRegion<RegionEndpoint> GetOrCreateRegion(RegionEndpoint region)
        {
            return GetOrCreateRegion(region.SystemName);
        }
    }
}
