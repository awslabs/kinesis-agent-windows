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
using System.Net.Http;
using System.Threading.Tasks;
using Amazon.CognitoIdentity;
using Amazon.CognitoIdentity.Model;
using Amazon.KinesisTap.Core;
using Amazon.Runtime;
using Amazon.Util;

namespace Amazon.KinesisTap.AWS.Telemetrics
{
    public class TelemetricsClient : ITelemetricsClient<HttpResponseMessage>, IDisposable
    {
#if DEBUG
        private const string CLIENT_ID = "ClientId_Debug";
        private const string IDENTITY_POOL_ID = "us-west-2:ee5a1104-d25a-4db9-b4d3-6f81419226ff";
        private const string TELEMETRIC_SERVICE_URI = "https://byaw9mnya8.execute-api.us-west-2.amazonaws.com/prod/";
#else
        private const string CLIENT_ID = "ClientId";
        private const string IDENTITY_POOL_ID = "us-west-2:4a5aa996-94e9-4a18-9932-c5454b7e7bea";
        private const string TELEMETRIC_SERVICE_URI = "https://yxgz0397n8.execute-api.us-west-2.amazonaws.com/prod/";
#endif
        private const string REGION = "us-west-2";
        private const string SERVICE_NAME = "execute-api";


        private IAmazonCognitoIdentity _cognitoIdentityClient;
        private HttpClient _httpClient;
        private Credentials _cognitoCredentials;

        private static IAmazonCognitoIdentity _cognitoIdentity;
        private static ITelemetricsClient<HttpResponseMessage> _defaultInstance;

        public static IAmazonCognitoIdentity CognitoIdentity
        {
            get
            {
                if (_cognitoIdentity == null)
                {
                    _cognitoIdentity = new AmazonCognitoIdentityClient(new AnonymousAWSCredentials(), RegionEndpoint.USWest2);
                }
                return _cognitoIdentity;
            }
        }

        public static ITelemetricsClient<HttpResponseMessage> Default
        {
            get
            {
                if (_defaultInstance == null)
                {
                    _defaultInstance = new TelemetricsClient(CognitoIdentity);
                }
                return _defaultInstance;
            }
            internal set
            {
                _defaultInstance = value;
            }
        }

        public TelemetricsClient(IAmazonCognitoIdentity cognitoIdentityClient)
        {
            _cognitoIdentityClient = cognitoIdentityClient;
            _httpClient = new HttpClient();
        }

        public async Task<string> CreateClientIdAsync()
        {
            var getIdResponse = await _cognitoIdentityClient.GetIdAsync(new GetIdRequest()
            {
                IdentityPoolId = IDENTITY_POOL_ID
            });
            this.ClientId = getIdResponse.IdentityId;
            return this.ClientId;
        }

        public async Task<HttpResponseMessage> PutMetricsAsync(IDictionary<string, object> data)
        {
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Put, TELEMETRIC_SERVICE_URI + "metrics");
            message.Content = HttpClientExtensions.GetStringContent(data);

            //Get credential if no credential or withing 5 minute of expiration
            if (_cognitoCredentials == null || _cognitoCredentials.Expiration - AWSSDKUtils.CorrectedUtcNow < TimeSpan.FromMinutes(5))
            {
                var getCredentialsForIdentityResponse = await _cognitoIdentityClient.GetCredentialsForIdentityAsync(ClientId);
                _cognitoCredentials = getCredentialsForIdentityResponse.Credentials;
            }
            
            await AWSV4SignerExtensions.SignRequestAsync(message, REGION, SERVICE_NAME, _cognitoCredentials);
            var response = await _httpClient.SendAsync(message);
            response.EnsureSuccessStatusCode();
            return response;
        }

        public string ClientId { get; set; }

        public string ClientIdParameterName => CLIENT_ID;


        #region IDisposable Support
        private bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _httpClient?.Dispose();
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
