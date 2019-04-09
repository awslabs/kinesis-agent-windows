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

using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.AWS.CredentialProvider
{
    /// <summary>
    /// Plug-in mechanism to make ProfileRefreshingAWSCredentialProvider as a plug-in.
    /// </summary>
    public class ProfileRefreshingCredentialsFactory : IFactory<ICredentialProvider>
    {
        private const string PROFILE_REFRESHING_AWS_CREDENTIAL_PROVIDER = "profilerefreshingawscredentialprovider";

        public ICredentialProvider CreateInstance(string entry, IPlugInContext context)
        {
            switch (entry.ToLower())
            {
                case PROFILE_REFRESHING_AWS_CREDENTIAL_PROVIDER:
                    return new ProfileRefreshingAWSCredentialProvider(context);
                default:
                    throw new NotImplementedException($"Credential type {entry} is not implemented by ProfileRefreshingCredentialsFactory.");
            }
        }

        public void RegisterFactory(IFactoryCatalog<ICredentialProvider> catalog)
        {
            catalog.RegisterFactory(PROFILE_REFRESHING_AWS_CREDENTIAL_PROVIDER, this);
        }
    }
}
