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
using Amazon.KinesisTap.Core;
using Amazon.Runtime;

namespace Amazon.KinesisTap.AWS.Failover
{
    /// <summary>
    /// An interface for AWS Client region selection strategy.
    /// </summary>
    public interface IFailoverStrategy<TAWSClient> where TAWSClient : AmazonServiceClient
    {
        /// <summary>
        /// Get primary region client.
        /// </summary>
        /// <returns>Region specific client.</returns>
        public TAWSClient GetPrimaryRegionClient();

        /// <summary>
        /// Get secondary region client.
        /// </summary>
        /// <returns>Region specific client.</returns>
        public TAWSClient GetSecondaryRegionClient();

        /// <summary>
        /// Create AWS Client from plug-in context
        /// </summary>
        /// <param name="context">Instance of <see cref="IPlugInContext"/> class.</param>
        /// <param name="credential">Instance of <see cref="AWSCredentials"/> class.</param>
        /// <param name="region">Instance of <see cref="RegionEndpoint"/> class.</param>
        /// <returns>Instance of <see cref="AmazonServiceClient"/> class.</returns>
        public delegate TAWSClient CreateClient(IPlugInContext context, AWSCredentials credential, RegionEndpoint region = null);
    }
}
