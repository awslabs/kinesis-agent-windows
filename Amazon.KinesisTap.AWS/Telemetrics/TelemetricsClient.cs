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
using System.Threading;
using System.Threading.Tasks;
using Amazon.CognitoIdentity;
using Amazon.CognitoIdentity.Model;
using Amazon.KinesisTap.Core;
using Amazon.Util;

namespace Amazon.KinesisTap.AWS.Telemetrics
{
    /// <summary>
    /// Sends telemetric data to the default backend.
    /// </summary>
    public class TelemetricsClient : ITelemetricsClient, IDisposable
    {
#if DEBUG
        private const string CLIENT_ID = "ClientId_Debug";
        private const string IDENTITY_POOL_ID = "us-west-2:3cd2324c-0eaa-46ee-bba6-04ff7bdb0f00";
        private const string TELEMETRIC_SERVICE_URI = "https://60q2i1r9q7.execute-api.us-west-2.amazonaws.com/prod/";
#else
        private const string CLIENT_ID = "ClientId";
        private const string IDENTITY_POOL_ID = "us-west-2:d773c513-c447-4b3c-89ba-a2cebde90b2e";
        private const string TELEMETRIC_SERVICE_URI = "https://yq2fhu9ppd.execute-api.us-west-2.amazonaws.com/prod/";
#endif
        private const string REGION = "us-west-2";
        private const string SERVICE_NAME = "execute-api";

        private readonly IAmazonCognitoIdentity _cognitoIdentityClient;
        private readonly IParameterStore _parameterStore;
        private readonly HttpClient _httpClient;
        private Credentials _cognitoCredentials;
        private string _clientId;

        public TelemetricsClient(IAmazonCognitoIdentity cognitoIdentityClient, IParameterStore parameterStore)
        {
            _cognitoIdentityClient = cognitoIdentityClient;
            _parameterStore = parameterStore;
            _clientId = parameterStore.GetParameter(CLIENT_ID);
            _httpClient = new HttpClient();
        }

        /// <inheritdoc/>
        /// <remarks>
        /// ID is retrieved from Cognito and saved to parameter store.
        /// </remarks>
        public async ValueTask<string> GetClientIdAsync(CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(_clientId))
            {
                var getIdResponse = await _cognitoIdentityClient.GetIdAsync(new GetIdRequest
                {
                    IdentityPoolId = IDENTITY_POOL_ID
                }, cancellationToken);
                _clientId = getIdResponse.IdentityId;
                _parameterStore.SetParameter(CLIENT_ID, _clientId);
            }

            return _clientId;
        }

        public async Task PutMetricsAsync(IDictionary<string, object> data, CancellationToken cancellationToken)
        {
            var clientId = await GetClientIdAsync(cancellationToken);
            using var message = new HttpRequestMessage(HttpMethod.Put, TELEMETRIC_SERVICE_URI + "metrics");
            message.Content = HttpClientExtensions.GetStringContent(data);

            //Get credential if no credential or withing 5 minute of expiration
#pragma warning disable CS0618 // Type or member is obsolete
            if (_cognitoCredentials == null || _cognitoCredentials.Expiration - AWSSDKUtils.CorrectedUtcNow < TimeSpan.FromMinutes(5))
#pragma warning restore CS0618 // Type or member is obsolete
            {
                var getCredentialsForIdentityResponse = await _cognitoIdentityClient.GetCredentialsForIdentityAsync(clientId);
                _cognitoCredentials = getCredentialsForIdentityResponse.Credentials;
            }

            await AWSV4SignerExtensions.SignRequestAsync(message, REGION, SERVICE_NAME, _cognitoCredentials);
            using var response = await _httpClient.SendAsync(message, cancellationToken);
            response.EnsureSuccessStatusCode();
        }

        #region IDisposable Support
        private bool _disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _httpClient.Dispose();
                    _cognitoIdentityClient.Dispose();
                }
                _disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}
