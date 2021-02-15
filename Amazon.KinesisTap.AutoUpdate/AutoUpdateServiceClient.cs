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
using Amazon.KinesisTap.AWS;
using Amazon.KinesisTap.Core;
using Amazon.Runtime;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.AutoUpdate
{
    public class AutoUpdateServiceClient
    {
        private const string SERVICE_NAME = "execute-api";
        private readonly IAutoUpdateServiceHttpClient httpClient;

        public AutoUpdateServiceClient(IAutoUpdateServiceHttpClient httpClient)
        {
            this.httpClient = httpClient;
        }

        public async Task<string> GetVersionAsync(string url, GetVersionRequest request, RegionEndpoint region, AWSCredentials creds)
        {
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = HttpClientExtensions.GetStringContent(request)
            };

            // Current AutoUpdate service only has US-WEST-2 endpoint
            await AWSV4SignerExtensions.SignRequestAsync(message, RegionEndpoint.USWest2.SystemName, SERVICE_NAME, creds);
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            {
                return await this.httpClient.SendRequest(message, cts.Token);
            }
        }
    }
}
