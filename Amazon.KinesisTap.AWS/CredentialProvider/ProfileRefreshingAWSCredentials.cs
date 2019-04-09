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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;

namespace Amazon.KinesisTap.AWS.CredentialProvider
{
    /// <summary>
    /// RefreshingAWSCredentials based on AWS configuration profiles. This credential vends AWSCredentials from
    /// the profile configuration file for the default profile, or for a specific, named profile.
    /// The profile configuration file can be at default or configured location.
    /// It refreshes credential at RefreshInterval. The default is 5 minutes.
    /// </summary>
    public class ProfileRefreshingAWSCredentials : RefreshingAWSCredentials
    {
        //Default refresh interval
        private const long DEFAULT_REFRESH_INTERVAL_SECONDS = 5 * 60;

        private const string CREDENTIAL_NOT_FOUND_EXCEPTION_MESSAGE = "Unable to retrieve profile '{0}' from file '{1}'.";
        protected readonly SharedCredentialsFile _credentialFile;
        protected readonly string _profileName;
        protected readonly string _profileFilePath;

        public ProfileRefreshingAWSCredentials()
            : this(SharedCredentialsFile.DefaultProfileName)
        {
        }

        public ProfileRefreshingAWSCredentials(string profileName)
            : this(profileName, SharedCredentialsFile.DefaultFilePath)
        {
        }

        public ProfileRefreshingAWSCredentials(string profileName, string profileFilePath)
        {
            if (string.IsNullOrWhiteSpace(profileName))
                throw new ArgumentNullException("'profileName' argument cannot be null or whitespace");

            if (!File.Exists(profileFilePath))
                throw new CredentialsNotFoundException(string.Format(CREDENTIAL_NOT_FOUND_EXCEPTION_MESSAGE, profileName, profileFilePath));

            this._profileName = profileName;
            this._profileFilePath = profileFilePath;
            this._credentialFile = new SharedCredentialsFile(profileFilePath);
        }

        /// <summary>
        /// Convenient constructor for subclasses
        /// </summary>
        /// <param name="profileInfo"></param>
        protected ProfileRefreshingAWSCredentials((string profileName, string profileFilePath) profileInfo)
            : this(profileInfo.profileName, profileInfo.profileFilePath)
        {
        }

        //Refresh interval in seconds
        public long RefreshInterval { get; set; } = DEFAULT_REFRESH_INTERVAL_SECONDS;

        protected override CredentialsRefreshState GenerateNewCredentials()
        {
            if (this._credentialFile.TryGetProfile(this._profileName, out CredentialProfile profile))
            {
                return new CredentialsRefreshState
                {
                    Credentials = profile.GetAWSCredentials(null).GetCredentials(),
                    Expiration = DateTime.UtcNow.AddSeconds(RefreshInterval)
                };
            }

            throw new CredentialsNotFoundException(string.Format(CREDENTIAL_NOT_FOUND_EXCEPTION_MESSAGE, this._profileName, this._profileFilePath));
        }
    }
}
