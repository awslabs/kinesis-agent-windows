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

namespace Amazon.KinesisTap.Uls
{
    //Factory for the Sharepoint Uls Log source
    public class UlsSourceFactory : IFactory<ISource>
    {
        const string ULSSOURCE = "ulssource";

        public void RegisterFactory(IFactoryCatalog<ISource> catalog)
        {
            catalog.RegisterFactory(ULSSOURCE, this);
        }

        public ISource CreateInstance(string entry, IPlugInContext context)
        {
            IConfiguration config = context.Configuration;
            ILogger logger = context.Logger;

            switch (entry.ToLower())
            {
                case ULSSOURCE:
                    UlsLogParser ulsParser = new UlsLogParser();
                    return DirectorySourceFactory.CreateEventSource(
                        context,
                        ulsParser);
                default:
                    throw new ArgumentException($"Source {entry} not recognized.");
            }
        }
    }
}
