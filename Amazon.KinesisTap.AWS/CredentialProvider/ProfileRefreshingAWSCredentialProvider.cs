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
using System.Text;

using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.AWS.CredentialProvider
{
    /// <summary>
    /// Credential provider for profile. Will refresh at default or configured interval.
    /// The configurable parameters are Profile, FilePath and RefreshInterval, all optional.
    /// Usage:
    /// "Credentials": [
    ///	{
    ///		"Id": "myProfileCredential",
    ///		"CredentialType": "ProfileRefreshingAWSCredentialProvider",
    ///		"Profile": "Default" //Optional, default to "Default",
    ///		"FilePath": "FilePath" //Optional, default to %USERPROFILE%/.aws/credentials,
    ///		"Interval": 300 //Optional, default to 300 seconds, or 5 minutes.
    ///	}
    /// ]
    /// </summary>
    public class ProfileRefreshingAWSCredentialProvider : ICredentialProvider<AWSCredentials>
    {
        private readonly ProfileRefreshingAWSCredentials _credentials;

        public ProfileRefreshingAWSCredentialProvider(IPlugInContext context)
        {
            _credentials = new KinesisTapProfileRefreshingAWSCredentials(context);
        }

        public string Id { get; set; }

        public AWSCredentials GetCredentials()
        {
            return _credentials;
        }
    }
}
