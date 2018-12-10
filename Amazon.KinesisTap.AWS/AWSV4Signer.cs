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
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Amazon.Util;
using Amazon.Runtime;

namespace Amazon.KinesisTap.AWS
{
    /// <summary>
    /// AWS V4 Signature method and helpers
    /// </summary>
    public class AWSv4Signer
    {
        private const string ALGORITHM = "HMAC-SHA256";
        private const string EMPTYBODYSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        private const string SCHEME = "AWS4";
        private const string TERMINATOR = "aws4_request";

        /// <summary>
        /// Initializes a new instance of the <see cref="AWSv4Signer"/> class.
        /// </summary>
        public AWSv4Signer()
        {
        }

        /// <summary>
        /// Computes the canonical query parameter.
        /// For AWS4, query parameters must be included in the signing process.
        /// </summary>
        /// <param name="queryParamRaw">The set of query params to be encoded.</param>
        /// <returns>Canonicalized string of query parameters with values</returns>
        public string CanonicalizeQueryParameters(string queryParamRaw)
        {
            if (string.IsNullOrWhiteSpace(queryParamRaw)) return string.Empty;

            // check for empty dictionary

            var sortedParamMap = new SortedDictionary<string, string>();
            foreach (var kvp in queryParamRaw.Split('&'))
            {
                var param = kvp.Split('=');
                sortedParamMap.Add(param[0], param[1]);
            }

            // canonicalize the headers
            var queryParam = new List<string>(sortedParamMap.Count);
            foreach (var kvp in sortedParamMap)
                queryParam.Add($"{kvp.Key}={kvp.Value.Trim()}");

            return string.Join("&", queryParam);
        }

        /// <summary>
        /// Computes the AWSV4 Signature and returns the Authorization value
        /// </summary>
        /// <param name="requestHeaders">Request Headers in a Dictionary</param>
        /// <param name="canonicalizedQueryParameters">Canonizalized Query parameters as a string, created using the <see cref="AWSv4Signer.CanonicalizeQueryParameters(string)"/> method.</param>
        /// <param name="serviceURL">Service Url</param>
        /// <param name="region">AWS Region</param>
        /// <param name="serviceName">Name of Service</param>
        /// <param name="creds">Credentials.</param>
        /// <param name="requestMethod">Request Method</param>
        /// <param name="postDataBody">Data that you want to post (if any)</param>
        /// <param name="requestDate">The datetime of the request</param>
        /// <returns>Hash of the data</returns>
        public string GetAWSSigV4AuthorizationValue(
            IDictionary<string, string> requestHeaders,
            string canonicalizedQueryParameters,
            string serviceURL,
            string region,
            string serviceName,
            ImmutableCredentials creds,
            string requestMethod,
            string postDataBody,
            DateTime requestDate)
        {
            // canonicalize headers names
            var canonicalizedHeaderNames = CanonicalizeHeaderNames(requestHeaders);

            // canonicalize headers
            var canonicalizedHeaders = CanonicalizeHeaders(requestHeaders);

            // check if query param is empty
            if (canonicalizedQueryParameters == null)
                canonicalizedQueryParameters = string.Empty;

            // hash the post data even if its empty
            var postDataBodyHash = string.IsNullOrEmpty(postDataBody) ? EMPTYBODYSHA256 : AWSSDKUtils.BytesToHexString(CryptoUtilFactory.CryptoInstance.ComputeSHA256Hash(Encoding.UTF8.GetBytes(postDataBody))).ToLower();

            // canonicalize the request
            var canonicalizedRequest = CanonicalizeRequest(serviceURL, requestMethod, canonicalizedQueryParameters, canonicalizedHeaderNames, canonicalizedHeaders, postDataBodyHash);

            // hash the canonical request string
            var hashedCanonicalRequest = AWSSDKUtils.BytesToHexString(CryptoUtilFactory.CryptoInstance.ComputeSHA256Hash(Encoding.UTF8.GetBytes(canonicalizedRequest))).ToLower();

            var dateStamp = requestDate.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
            var amzDate = requestDate.ToString("yyyyMMddTHHmmssZ", CultureInfo.InvariantCulture);

            // Credential Scope
            var credentialScope = $"{dateStamp}/{region}/{serviceName}/{TERMINATOR}";

            // string to sign
            var stringToSign = $"{SCHEME}-{ALGORITHM}\n{amzDate}\n{credentialScope}\n{hashedCanonicalRequest}";

            // signature  key
            var signingKey = this.GetSigningKey(region, dateStamp, serviceName, creds.SecretKey);

            // AWS v4 signature
            var signature = AWSSDKUtils.BytesToHexString(HmacSHA256(stringToSign, signingKey)).ToLower();

            // create the authorization
            var authString = new StringBuilder();
            authString.AppendFormat("{0}-{1} ", SCHEME, ALGORITHM);
            authString.AppendFormat("Credential={0}/{1}, ", creds.AccessKey, credentialScope);
            authString.AppendFormat("SignedHeaders={0}, ", canonicalizedHeaderNames);
            authString.AppendFormat("Signature={0}", signature);
            return authString.ToString();
        }

        /// <summary>
        /// Returns the canonical collection of header names that will be included in
        /// the signature. For AWS4, all header names must be included in the process
        /// in sorted canonicalized order.
        /// </summary>
        /// <param name="headers">
        /// The set of header names and values that will be sent with the request
        /// </param>
        /// <returns>
        /// The set of header names canonicalized to a flattened, ;-delimited string
        /// </returns>
        private static string CanonicalizeHeaderNames(IDictionary<string, string> headers)
        {
            if (headers == null || headers.Count == 0)
                throw new Exception("Request headers cannot be empty. You must supply at least 'host' and 'x-amzn-date' to make a sigv4 request.");

            // sort the header names by the dictionary keys
            var headersToSign = new List<string>(headers.Keys);
            headersToSign.Sort(StringComparer.OrdinalIgnoreCase);

            // canonicalize the header names
            return string.Join(";", headersToSign).ToLower();
        }

        /// <summary>
        /// Computes the canonical headers with values for the request.
        /// For AWS4, all headers must be included in the signing process.
        /// </summary>
        /// <param name="headers">The set of headers to be encoded</param>
        /// <returns>Canonicalized string of headers with values</returns>
        private static string CanonicalizeHeaders(IDictionary<string, string> headers)
        {
            // check for empty dictionary
            if (headers == null || headers.Count == 0)
                return string.Empty;

            // sort the headers (keys and values)
            var sortedHeaderMap = new SortedDictionary<string, string>();
            foreach (var header in headers)
                sortedHeaderMap.Add(header.Key.ToLower(), header.Value);

            // canonicalize the headers
            var sb = new StringBuilder();
            foreach (var header in sortedHeaderMap)
                sb.AppendFormat("{0}:{1}\n", header.Key, header.Value?.Trim());

            return sb.ToString();
        }

        /// <summary>
        /// Returns the canonical request string to go into the signer process; this
        /// consists of several canonical sub-parts.
        /// </summary>
        /// <param name="endpointUri">The endpoint</param>
        /// <param name="httpMethod">The http method</param>
        /// <param name="queryParameters">The query parameters</param>
        /// <param name="canonicalizedHeaderNames">
        /// The set of header names to be included in the signature, formatted as a flattened, ;-delimited string
        /// </param>
        /// <param name="canonicalizedHeaders">
        /// The set of headers to be included in the signature, formatted as a flattened, ;-delimited string
        /// </param>
        /// <param name="bodyHash">
        /// Precomputed SHA256 hash of the request body content. For chunked encoding this
        /// should be the fixed string ''.
        /// </param>
        /// <returns>String representing the canonicalized request for signing</returns>
        private static string CanonicalizeRequest(string endpointUri, string httpMethod, string queryParameters, string canonicalizedHeaderNames, string canonicalizedHeaders, string bodyHash)
        {
            var canonicalRequest = new StringBuilder();
            canonicalRequest.AppendFormat("{0}\n", httpMethod);
            canonicalRequest.AppendFormat("{0}\n", CanonicalResourcePath(endpointUri));
            canonicalRequest.AppendFormat("{0}\n", queryParameters);
            canonicalRequest.AppendFormat("{0}\n", canonicalizedHeaders);
            canonicalRequest.AppendFormat("{0}\n", canonicalizedHeaderNames);
            canonicalRequest.Append(bodyHash);
            return canonicalRequest.ToString();
        }

        private static string CanonicalResourcePath(string endpointUri)
        {
            if (string.IsNullOrWhiteSpace(endpointUri)) return "/";

            // encode the path according to the elastic search custom requirements.
            return endpointUri.Replace("*", "%2A");
        }

        private static byte[] HmacSHA256(string data, byte[] key)
        {
            using (var kha = new HMACSHA256(key))
                return kha.ComputeHash(Encoding.UTF8.GetBytes(data));
        }

        private byte[] GetSigningKey(string region, string date, string service, string secretKey)
        {
            var kSecret = Encoding.UTF8.GetBytes((SCHEME + secretKey).ToCharArray());
            var kDate = HmacSHA256(date, kSecret);
            var kRegion = HmacSHA256(region, kDate);
            var kService = HmacSHA256(service, kRegion);
            return HmacSHA256(TERMINATOR, kService);
        }
    }
}
