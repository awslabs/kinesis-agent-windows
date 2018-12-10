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
