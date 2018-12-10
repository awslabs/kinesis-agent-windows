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
using Microsoft.Extensions.Configuration;

namespace Amazon.KinesisTap.DiagnosticTool
{
    /// <summary>
    /// The configuration file loader
    /// </summary>
    public class ConfigFileLoader
    {
        /// <summary>
        /// Load the KinesisTap configuration file
        /// </summary>
        /// <param name="configBaseDirectory"></param>
        /// <param name="configFile"></param>
        /// <returns></returns>
        public static IConfigurationRoot LoadConfigFile(string configBaseDirectory, string configFile)
        {

            ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

            IConfigurationRoot config;

            config = configurationBuilder
                .SetBasePath(configBaseDirectory)
                .AddJsonFile(configFile, optional: false, reloadOnChange: true)
                .Build();

            return config;
        }
    }
}
