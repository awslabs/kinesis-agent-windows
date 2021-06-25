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
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Amazon.Runtime;

namespace Amazon.KinesisTap.AWS.Failover
{
    /// <summary>
    /// An interface for failover sink.
    /// </summary>
    public interface IFailoverSink<TAWSClient> : IEventSink where TAWSClient : AmazonServiceClient
    {
        /// <summary>
        /// Failback to Primary Region
        /// </summary>
        /// <param name="throttle">Instance of <see cref="Throttle"/> class.</param>
        /// <returns>Instance of <see cref="AmazonServiceClient"/> class.</returns>
        public TAWSClient FailbackToPrimaryRegion(Throttle throttle);

        /// <summary>
        /// Failover to Secondary Region
        /// </summary>
        /// <param name="throttle">Instance of <see cref="Throttle"/> class.</param>
        /// <returns>Instance of <see cref="AmazonServiceClient"/> class.</returns>
        public TAWSClient FailOverToSecondaryRegion(Throttle throttle);

        /// <summary>
        /// Check service health.
        /// </summary>
        /// <param name="client">Instance of <see cref="AmazonServiceClient"/> class.</param>
        /// <returns>Success, Latency in milliseconds.</returns>
        public delegate Task<(bool, double)> CheckServiceReachable(TAWSClient client);
    }
}
