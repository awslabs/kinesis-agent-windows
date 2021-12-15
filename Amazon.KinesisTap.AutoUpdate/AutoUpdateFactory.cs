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

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Amazon.KinesisTap.Core;
using Microsoft.Extensions.DependencyInjection;
using System.Collections.Generic;

namespace Amazon.KinesisTap.AutoUpdate
{
    public class AutoUpdateFactory : IFactory<IGenericPlugin>
    {
        const string PACKAGE_UPDATE = "packageupdate";
        const string CONFIG_UPDATE = "configupdate";

        const string SKIP_SIGNATURE_VERIFICATION_SETTING = "SkipSignatureVerification";
        const string ALLOWED_PUBLISHERS_SETTING = "AllowedPublishers";

        public void RegisterFactory(IFactoryCatalog<IGenericPlugin> catalog)
        {
            catalog.RegisterFactory(PACKAGE_UPDATE, this);
            catalog.RegisterFactory(CONFIG_UPDATE, this);
        }

        public IGenericPlugin CreateInstance(string entry, IPlugInContext context)
        {
            IConfiguration config = context.Configuration;
            ILogger logger = context.Logger;

            switch (entry.ToLower())
            {
                case PACKAGE_UPDATE:
                    var allowedPublishers = new HashSet<string>();
                    var allowedPublishersConfig = config.GetSection(ALLOWED_PUBLISHERS_SETTING);
                    foreach (var val in allowedPublishersConfig.GetChildren())
                    {
                        allowedPublishers.Add(val.Value);
                    }
                    var appDataFileProvider = context.Services.GetService<IAppDataFileProvider>();
                    var skipSigVerification = bool.TryParse(config[SKIP_SIGNATURE_VERIFICATION_SETTING], out var ssv) && ssv;

                    return new PackageUpdater(context,
                        new AutoUpdateServiceHttpClient(), 
                        new PackageInstaller(context, appDataFileProvider, allowedPublishers.Count > 0 ? allowedPublishers : null, skipSigVerification));
                case CONFIG_UPDATE:
                    return new ConfigurationFileUpdater(context);
                default:
                    throw new ArgumentException($"Source {entry} not recognized.");
            }
        }
    }
}
