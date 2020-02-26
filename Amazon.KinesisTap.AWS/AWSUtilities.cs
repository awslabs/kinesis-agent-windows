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
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;

using Amazon.CloudWatchLogs.Model;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Amazon.Util;
using Microsoft.Extensions.Configuration;

using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.AWS
{
    public static class AWSUtilities
    {
        private static string _userAgent;

        public static string EvaluateAWSVariable(string variable)
        {
            if (!variable.StartsWith("{") || !variable.EndsWith("}"))
            {
                //Variable already evaluated
                return variable;
            }

            (string prefix, string variableNoPrefix) = Utility.SplitPrefix(variable.Substring(1, variable.Length - 2), ':');
            switch(variableNoPrefix.ToLower())
            {
                case "instance_id":
                    return EC2InstanceMetadata.InstanceId;
                case "hostname":
                    return EC2InstanceMetadata.Hostname;
                default:
                    if ("ec2".Equals(prefix, StringComparison.CurrentCultureIgnoreCase))
                    {
                        if (!variableNoPrefix.StartsWith("/")) variableNoPrefix = "/" + variableNoPrefix;
                        return EC2InstanceMetadata.GetData(variableNoPrefix);
                    }
                    else if("ec2tag".Equals(prefix, StringComparison.CurrentCultureIgnoreCase))
                    {
                        return EC2Utility.GetTagValue(variableNoPrefix);
                    }
                    else
                    {
                        return variable;
                    }
            }
        }

        public static string GetExpectedSequenceToken(this InvalidSequenceTokenException invalidSequenceTokenException)
        {
            const string searchString = "The next expected sequenceToken is: ";
            string message = invalidSequenceTokenException.Message;
            string expectedSequenceToken = null;
            int indexOfSearchString = message.IndexOf(searchString);
            if (indexOfSearchString > -1)
            {
                expectedSequenceToken = message.Substring(indexOfSearchString + searchString.Length);
            }
            return expectedSequenceToken;
        }

        /// <summary>
        /// Create AWS Client from plug-in context
        /// </summary>
        /// <typeparam name="TAWSClient">The type of AWS Client</typeparam>
        /// <param name="context">Plug-in context</param>
        /// <returns>AWS Client</returns>
        public static TAWSClient CreateAWSClient<TAWSClient>(IPlugInContext context) where TAWSClient : AmazonServiceClient
        {
            (AWSCredentials credential, RegionEndpoint region) = GetAWSCredentialsRegion(context);
            var awsClient = CreateAWSClient<TAWSClient>(credential, region);
            if (context.Configuration["SinkType"]?.ToLower() == AWSEventSinkFactory.CLOUD_WATCH_LOG_EMF)
            {
                awsClient.BeforeRequestEvent += (sender, args) =>
                {
                    if (args is WebServiceRequestEventArgs wsArgs)
                    {
                        SetCommonRequestHeaders(wsArgs);

                        // Add the header required for EMF parsing in CloudWatch Logs
                        wsArgs.Headers["x-amzn-logs-format"] = "Structured";
                    }
                };
            }
            else
            {
                awsClient.BeforeRequestEvent += (sender, args) =>
                {
                    if (args is WebServiceRequestEventArgs wsArgs)
                    {
                        SetCommonRequestHeaders(wsArgs);
                    }
                };
            }
            return awsClient;
        }

        /// <summary>
        /// Generate AWS Client using credential and region
        /// </summary>
        /// <typeparam name="TAWSClient">The type of AWS Client</typeparam>
        /// <param name="credential">AWSCredentials</param>
        /// <param name="region">RegionEndpoint</param>
        /// <returns></returns>
        public static TAWSClient CreateAWSClient<TAWSClient>(AWSCredentials credential, RegionEndpoint region) where TAWSClient : AmazonServiceClient
        {
            TAWSClient awsClient;
            if (region != null)
            {
                awsClient = (TAWSClient)Activator.CreateInstance(typeof(TAWSClient), credential, region);
            }
            else
            {
                awsClient = (TAWSClient)Activator.CreateInstance(typeof(TAWSClient), credential);
            }

            return awsClient;
        }

        public static (AWSCredentials credential, RegionEndpoint region) GetAWSCredentialsRegion(IPlugInContext context)
        {
            IConfiguration config = context.Configuration;
            string id = config[ConfigConstants.ID];
            string credentialRef = config[ConfigConstants.CREDENTIAL_REF];
            string accessKey = config[ConfigConstants.ACCESS_KEY];
            string secretKey = config[ConfigConstants.SECRET_KEY];
            string region = config[ConfigConstants.REGION];
            string profileName = config[ConfigConstants.PROFILE_NAME];
            string roleArn = config[ConfigConstants.ROLE_ARN];
            AWSCredentials credential = null;
            RegionEndpoint regionEndPoint = null;

            //Order 0: If the sink has credential providers, use if
            if (!string.IsNullOrWhiteSpace(credentialRef))
            {
                if (!(context.GetCredentialProvider(credentialRef) is ICredentialProvider<AWSCredentials> credentialProvider))
                {
                    throw new Exception($"Credential {credentialRef} not found or not an AWSCredential.");
                }
                credential = credentialProvider.GetCredentials();
            }
            // Order 1: If the sink has a profile entry, get the credential from profile store
            else if (!string.IsNullOrWhiteSpace(profileName))
            {
                CredentialProfileStoreChain credentialProfileChaine = new CredentialProfileStoreChain();
                if (credentialProfileChaine.TryGetAWSCredentials(profileName, out credential))
                {
                    if (credentialProfileChaine.TryGetProfile(profileName, out CredentialProfile profile))
                    {
                        regionEndPoint = profile.Region;
                    }
                }
                else
                {
                    throw new AmazonServiceException($"Profile name {profileName} not found.");
                }
            }
            // Order 2: If there is an accessKey, create the credential using accessKey and secretKey
            else if (!string.IsNullOrWhiteSpace(accessKey))
            {
                credential = new BasicAWSCredentials(accessKey, secretKey);
            }
            // Order 3: If nothing configured, using the Fallback credentials factory
            else
            {
                credential = FallbackCredentialsFactory.GetCredentials();
            }

            //If roleARN is specified. Assume if from the credential loaded above
            if (!string.IsNullOrWhiteSpace(roleArn))
            {
                credential = new AssumeRoleAWSCredentials(credential, roleArn, $"KinesisTap-{Utility.ComputerName}");
            }

            //Any region override?
            if (!string.IsNullOrWhiteSpace(region))
            {
                regionEndPoint = RegionEndpoint.GetBySystemName(region);
            }

            return (credential, regionEndPoint);
        }

        /// <summary>
        /// Get AWS Credentials from parameters. If none supplied, use the FallbackCredentialsFactory.
        /// </summary>
        /// <param name="accessKey">Access Key</param>
        /// <param name="secretKey">Secret Key</param>
        /// <param name="token">Token</param>
        /// <returns>AWSCredentials</returns>
        public static AWSCredentials GetAWSCredentials(string accessKey, string secretKey, string token)
        {
            if (string.IsNullOrWhiteSpace(accessKey))
            {
                return FallbackCredentialsFactory.GetCredentials();
            }
            else if (string.IsNullOrWhiteSpace(token))
            {
                return new BasicAWSCredentials(accessKey, secretKey);
            }
            else
            {
                return new SessionAWSCredentials(accessKey, secretKey, token);
            }
        }

        public static (string bucketName, string key) ParseS3Url(string url)
        {
            if (Uri.TryCreate(url, UriKind.Absolute, out Uri uri))
            {
                if (uri.Scheme == "s3")
                {
                    return (uri.Host, uri.LocalPath.Substring(1));
                }
                throw new InvalidParameterException($"Not a s3 Url: {url}");
            }
            throw new InvalidParameterException($"Not a wellformed Url: {url}");
        }

        public static string UserAgent
        {
            get
            {
                if (_userAgent == null)
                {
                    string programName = Path.GetFileNameWithoutExtension(ConfigConstants.KINESISTAP_EXE_NAME);
                    string version = ProgramInfo.GetKinesisTapVersion().ProductVersion;
                    string osDescription = RuntimeInformation.OSDescription + " " + Environment.GetEnvironmentVariable("OS");
                    string dotnetFramework = RuntimeInformation.FrameworkDescription;
                    _userAgent = $"{programName}/{version} | {osDescription} | {dotnetFramework}";
                }
                return _userAgent;
            }
        }

        private static void SetCommonRequestHeaders(WebServiceRequestEventArgs wsArgs)
        {
            wsArgs.Headers[AWSSDKUtils.UserAgentHeader] = UserAgent;

            // Keep connections alive rather than establishing a new TLS session each time
            // wsArgs.Headers["Connection"] = "KeepAlive";
        }
    }
}
