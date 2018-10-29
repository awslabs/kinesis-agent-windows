using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.Util;

namespace Amazon.KinesisTap.AWS
{
    public static class AWSV4SignerExtensions
    {
        private static AWSv4Signer signer = new AWSv4Signer();

        public static async Task SignRequestAsync(this HttpRequestMessage httpRequestMessage, string region, string service, AWSCredentials credentials)
        {
            var creds = credentials.GetCredentials();
            var canonicalizedQueryParameters = string.Empty;

            if (!string.IsNullOrEmpty(httpRequestMessage.RequestUri.Query))
                canonicalizedQueryParameters = signer.CanonicalizeQueryParameters(httpRequestMessage.RequestUri.Query.TrimStart('?'));

            DateTime requestDateTimeInUTC = AWSSDKUtils.CorrectedUtcNow;
            var dictionary = new Dictionary<string, string>
            {
                { "host", httpRequestMessage.RequestUri.Host },
                { "x-amz-date", requestDateTimeInUTC.ToString("yyyyMMddTHHmmssZ") }
            };

            var requestBody = await httpRequestMessage.Content.ReadAsStringAsync();
            if (!string.IsNullOrEmpty(requestBody))
                httpRequestMessage.Content = new StringContent(requestBody, Encoding.UTF8, "application/json");

            var uri = httpRequestMessage.RequestUri;
            string canonicalServiceUri = uri.LocalPath;
            string requestMethod = httpRequestMessage.Method.Method.ToUpper();

            var aWSSigV4AuthorizationValue = signer.GetAWSSigV4AuthorizationValue(dictionary,
                canonicalizedQueryParameters,
                canonicalServiceUri,
                region,
                service,
                creds,
                requestMethod,
                requestBody,
                requestDateTimeInUTC);

            httpRequestMessage.Headers.TryAddWithoutValidation("Authorization", aWSSigV4AuthorizationValue);
            httpRequestMessage.Headers.TryAddWithoutValidation("x-amz-date", requestDateTimeInUTC.ToString("yyyyMMddTHHmmssZ"));

            if (creds.UseToken) httpRequestMessage.Headers.TryAddWithoutValidation("x-amz-security-token", creds.Token);
        }
    }
}
