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
using System.IO;
using System.Text;

using Amazon.Runtime.CredentialManagement;
using Microsoft.Extensions.Logging;

using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.AWS.CredentialProvider
{
    /// <summary>
    /// Extend ProfileRefreshingAWSCredentials for profile expiration warning and logging.
    /// </summary>
    public class KinesisTapProfileRefreshingAWSCredentials : ProfileRefreshingAWSCredentials
    {
        protected readonly IPlugInContext _context;
        protected readonly int _warningIntervalSeconds = 0;

        public KinesisTapProfileRefreshingAWSCredentials(IPlugInContext context) : base(GetProfileConfiguration(context))
        {
            _context = context;
            var config = context?.Configuration;

            string refreshInterval = config?["refreshinterval"];
            if (!string.IsNullOrWhiteSpace(refreshInterval))
            {
                this.RefreshInterval = int.Parse(refreshInterval);
            }

            string warningIntervalSeconds = config?["warninginterval"];
            if (!string.IsNullOrWhiteSpace(warningIntervalSeconds))
            {
                _warningIntervalSeconds = int.Parse(warningIntervalSeconds);
            }
        }

        protected override CredentialsRefreshState GenerateNewCredentials()
        {
            if (_warningIntervalSeconds > 0 && File.GetLastWriteTimeUtc(this._profileFilePath).AddSeconds(_warningIntervalSeconds) < DateTime.UtcNow)
            {
                _context?.Logger?.LogWarning($"Credential file {this._profileFilePath} maybe have expired. Please check your credential rotator.");
            }

            return base.GenerateNewCredentials();
        }

        private static (string profile, string filePath) GetProfileConfiguration(IPlugInContext context)
        {
            var config = context?.Configuration;
            string profile = config?["profile"];
            if (string.IsNullOrWhiteSpace(profile)) profile = SharedCredentialsFile.DefaultProfileName;

            string filePath = config?["filepath"];
            if (string.IsNullOrWhiteSpace(filePath)) filePath = SharedCredentialsFile.DefaultFilePath;

            return (profile, filePath);
        }
    }
}
