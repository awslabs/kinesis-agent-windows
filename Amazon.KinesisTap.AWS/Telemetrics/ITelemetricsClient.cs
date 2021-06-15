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
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.AWS.Telemetrics
{
    /// <summary>
    /// Used to work send the telemetric data to a particular endpoint. 
    /// </summary>
    public interface ITelemetricsClient
    {
        ////An unique ID to Identify the installation. This could be a Cognito User pool ClientID
        //string ClientId { get; set; }

        /// <summary>
        /// Send the telemetric data, in the form of key-value pairs.
        /// </summary>
        /// <param name="data">Telemetric data.</param>
        /// <param name="cancellationToken">Stop the request.</param>
        /// <returns>Task that completes when data is sent.</returns>
        Task PutMetricsAsync(IDictionary<string, object> data, CancellationToken cancellationToken = default);

        ////Generate a new unique ID
        //Task<string> CreateClientIdAsync();

        ////Allow each client to use its own parameter name to avoid conflict
        //string ClientIdParameterName { get; }

        /// <summary>
        /// Retrieve the Client ID for this telemetric client.
        /// </summary>
        /// <param name="cancellationToken">Cancel this call.</param>
        /// <returns>Client ID.</returns>
        ValueTask<string> GetClientIdAsync(CancellationToken cancellationToken);
    }
}
