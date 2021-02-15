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
namespace Amazon.KinesisTap.AWS
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Reflection;
    using System.Runtime.InteropServices;
    using Amazon.CloudWatchLogs.Model;
    using Amazon.KinesisTap.Core;
    using Amazon.Runtime;
    using Amazon.Runtime.CredentialManagement;
    using Amazon.Util;
    using Microsoft.DotNet.PlatformAbstractions;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Logging;

    public static class AWSUtilities
    {
        private static readonly ConcurrentDictionary<Type, Type> awsClientConfigTypeCache = new ConcurrentDictionary<Type, Type>();
        private static string _userAgent;

        public static string EvaluateAWSVariable(string variable)
        {
            if (!variable.StartsWith("{") || !variable.EndsWith("}"))
            {
                //Variable already evaluated
                return variable;
            }

            (string prefix, string variableNoPrefix) = Utility.SplitPrefix(variable.Substring(1, variable.Length - 2), ':');
            switch (variableNoPrefix.ToLower())
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
                    else if ("ec2tag".Equals(prefix, StringComparison.CurrentCultureIgnoreCase))
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
        public static TAWSClient CreateAWSClient<TAWSClient>(IPlugInContext context, RegionEndpoint regionOverride = null) where TAWSClient : AmazonServiceClient
        {
            (AWSCredentials credential, RegionEndpoint region) = GetAWSCredentialsRegion(context);
            if (regionOverride != null)
                region = regionOverride;

            var clientConfig = CreateAWSClientConfig<TAWSClient>(context, region);
            var awsClient = (TAWSClient)Activator.CreateInstance(typeof(TAWSClient), credential, clientConfig);

            var headers = new Dictionary<string, string> { [AWSSDKUtils.UserAgentHeader] = UserAgent };
            if (context.Configuration[ConfigConstants.SINK_TYPE]?.ToLower() == AWSEventSinkFactory.CLOUD_WATCH_LOG_EMF)
                headers.Add("x-amzn-logs-format", "Structured");

            var customHeaderSection = context.Configuration.GetSection(ConfigConstants.CUSTOM_AWS_CLIENT_HEADERS);
            if (customHeaderSection != null)
            {
                foreach (var customHeader in customHeaderSection.GetChildren())
                    headers.Add(customHeader.Key, ResolveConfigVariable(customHeader.Value));
            }

            awsClient.BeforeRequestEvent += (sender, args) =>
            {
                if (args is WebServiceRequestEventArgs wsArgs)
                {
                    foreach (var kvp in headers)
                        wsArgs.Headers[kvp.Key] = kvp.Value;
                }
            };

            return awsClient;
        }

        public static string ResolveConfigVariable(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return value;
            }
            return Utility.ResolveVariables(value, ConfigVariableEvaluator);
        }

        private static string ConfigVariableEvaluator(string variable)
        {
            var evaluated = Utility.ResolveVariable(variable);
            if (string.IsNullOrEmpty(evaluated))
            {
                return evaluated;
            }
            return AWSUtilities.EvaluateAWSVariable(evaluated);
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
            string region = ResolveConfigVariable(config[ConfigConstants.REGION]);
            string profileName = config[ConfigConstants.PROFILE_NAME];
            string roleArn = ResolveConfigVariable(config[ConfigConstants.ROLE_ARN]);
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
                ConfigureSTSRegionalEndpoint(config);
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

        private static ClientConfig CreateAWSClientConfig<TAWSClient>(IPlugInContext context, RegionEndpoint region) where TAWSClient : AmazonServiceClient
        {
            // The previous mechanism for locating the ClientConfig implementation was a switch block
            // containing all of the known cases that the Amazon.KinesisTap.AWS library referenced.
            // However, this meant that any other SDK clients that were instantiated by other libraries
            // were not able to use this code (e.g. if a plugin used Secrets Manager to store an API key). 
            // We need to do this more dynamically if we want third party plugins to be able to use this.

            // Get the type of the AWS client being created based on the type parameter,
            // and get the corresponding ClientConfig type from the type cache. If it doesn't
            // exist in the cache, do the discovery. Using a cache eliminates redundant loading of
            // assemblies when multiple instances of the same client are created. Since each client
            // may be configured differently, we can't cache the clients (or the configs) themselves,
            // but we can cache the ClientConfig types, since they are readonly objects.
            var configType = awsClientConfigTypeCache.GetOrAdd(typeof(TAWSClient), (type) =>
            {
                // Identify the "ClientConfig" for the requested SDK client. We can use the SDK's class naming
                // convention to identify this type, replacing the word "Client" with "Config" in the client's
                // full type name. For example:
                // Amazon.SecretsManager.AmazonSecretsManagerClient
                // becomes
                // Amazon.SecretsManager.AmazonSecretsManagerConfig
                var configTypeName = type.FullName.Substring(0, type.FullName.IndexOf("Client")) + "Config";

                // Identify the assembly's name. The only way we can do this in this version of .NET is to
                // use the AssemblyQualifiedName and strip off the type's name from the front. For example:
                // Amazon.SecretsManager.AmazonSecretsManagerClient, AWSSDK.SecretsManager, Version=3.3.0.0, Culture=neutral, PublicKeyToken=885c28607f98e604
                // becomes
                // AWSSDK.SecretsManager, Version=3.3.0.0, Culture=neutral, PublicKeyToken=885c28607f98e604
                var assemblyName = type.AssemblyQualifiedName.Substring(type.FullName.Length + 1).Trim();

                // Load the Assembly into an object.
                var assembly = Assembly.Load(new AssemblyName(assemblyName));

                // Get the ClientConfig type from the Assembly object.
                return assembly.GetType(configTypeName);
            });

            // Use Activator to initialize a new instance of the client-specific ClientConfig.
            var clientConfig = (ClientConfig)Activator.CreateInstance(configType);

            // Set the region endpoint property. If the region parameter is null, discover it using
            // the FallbackRegionFactory. This method will return null if it doesn't find anything,
            // so we'll throw an Exception if that's the case (since we won't be able to send any data).
            clientConfig.RegionEndpoint = region ?? FallbackRegionFactory.GetRegionEndpoint();
            if (clientConfig.RegionEndpoint == null)
            {
                context.Logger?.LogError("The 'Region' property was not specified in the configuration, and the agent was unable to discover it automatically.");
                throw new Exception("The 'Region' property was not specified in the configuration, and the agent was unable to discover it automatically.");
            }

            // Check if the configuration contains the ProxyHost property.
            if (!string.IsNullOrWhiteSpace(context.Configuration[ConfigConstants.PROXY_HOST]))
            {
                // Configure the client to use a proxy.
                clientConfig.ProxyHost = context.Configuration[ConfigConstants.PROXY_HOST];

                // If the customer supplied a port number, use that, otherwise use a default of 80.
                clientConfig.ProxyPort = ushort.TryParse(context.Configuration[ConfigConstants.PROXY_PORT], out ushort proxyPort) ? proxyPort : 80;

                context.Logger?.LogDebug("Using proxy host '{0}' with port '{1}'", clientConfig.ProxyHost, clientConfig.ProxyPort);
            }

            // If the configuration contains the ServiceURL property, configure the client to use
            // the supplied service endpoint (this is used for VPC endpoints).
            if (!string.IsNullOrWhiteSpace(context.Configuration[ConfigConstants.SERVICE_URL]))
            {
                // When using alternate service URL's, the AuthenticationRegion property must be set.
                // We'll use the existing region's value for this.
                clientConfig.AuthenticationRegion = clientConfig.RegionEndpoint.SystemName;

                // Try to parse the value into a Uri object. If it doesn't parse correctly, throw an Exception.
                var urlString = context.Configuration[ConfigConstants.SERVICE_URL];
                if (!Uri.TryCreate(urlString, UriKind.Absolute, out Uri uri))
                {
                    var error = string.Format("The 'ServiceURL' property value '{0}' is not in the correct format for a URL.", urlString);
                    context.Logger?.LogError(error);
                    throw new Exception(error);
                }

                clientConfig.ServiceURL = urlString;
                context.Logger?.LogDebug("Using alternate service endpoint '{0}' with AuthenticationRegion '{1}'", clientConfig.ServiceURL, clientConfig.AuthenticationRegion);
            }

            return clientConfig;
        }

        private static void ConfigureSTSRegionalEndpoint(IConfiguration config)
        {
            // Don't set unless the user has specified in the config that they want to use the regional endpoint.
            if (!bool.TryParse(config[ConfigConstants.USE_STS_REGIONAL_ENDPOINTS], out bool useRegionalSTSEndpoint)) return;
            if (!useRegionalSTSEndpoint) return;

            // Don't overwrite an existing value if it has already been set.
            if (!string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(ConfigConstants.STS_REGIONAL_ENDPOINTS_ENV_VARIABLE))) return;

            // Don't set if we can't automatically resolve the region (required for using regional endpoints).
            var autoDiscoveredRegion = FallbackRegionFactory.GetRegionEndpoint();
            if (autoDiscoveredRegion == null || autoDiscoveredRegion.DisplayName == "Unknown") return;

            // Set the AWS_STS_REGIONAL_ENDPOINTS environment variable to Regional.
            // This will mean that customers don't have to set the system-level variable.
            Environment.SetEnvironmentVariable(ConfigConstants.STS_REGIONAL_ENDPOINTS_ENV_VARIABLE, StsRegionalEndpointsValue.Regional.ToString());
        }

        private static void SetCommonRequestHeaders(WebServiceRequestEventArgs wsArgs)
        {
            wsArgs.Headers[AWSSDKUtils.UserAgentHeader] = UserAgent;

            // Keep connections alive rather than establishing a new TLS session each time
            // wsArgs.Headers["Connection"] = "KeepAlive";
        }
    }
}
