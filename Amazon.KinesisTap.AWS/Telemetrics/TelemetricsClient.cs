using Amazon.CognitoIdentity;
using Amazon.CognitoIdentity.Model;
using Amazon.KinesisTap.Core;
using Amazon.Runtime;
using Amazon.Util;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.AWS.Telemetrics
{
    public class TelemetricsClient : ITelemetricsClient<HttpResponseMessage>, IDisposable
    {
#if DEBUG
        private const string IDENTITY_POOL_ID = "us-west-2:3cd2324c-0eaa-46ee-bba6-04ff7bdb0f00";
        private const string TELEMETRIC_SERVICE_URI = "https://60q2i1r9q7.execute-api.us-west-2.amazonaws.com/prod/";
#else
        private const string IDENTITY_POOL_ID = "us-west-2:d773c513-c447-4b3c-89ba-a2cebde90b2e";
        private const string TELEMETRIC_SERVICE_URI = "https://yq2fhu9ppd.execute-api.us-west-2.amazonaws.com/prod/";
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

        public async Task<string> GetClientIdAsync()
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
            return await _httpClient.SendAsync(message);
        }

        public string ClientId { get; set; }


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
