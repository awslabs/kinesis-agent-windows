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
using System.Runtime.Versioning;
using Amazon.KinesisTap.Core;
using Microsoft.Win32;

namespace Amazon.KinesisTap.Hosting
{
    [SupportedOSPlatform("windows")]
    public class RegistryParameterStore : IParameterStore
    {
        const string REG_ROOT = @"SOFTWARE\Amazon\KinesisTap";

        public string GetParameter(string name)
        {
            using RegistryKey regKey = GetKinesisTapRoot();
            return regKey.GetValue(name) as string;
        }

        public void SetParameter(string name, string value)
        {
            using RegistryKey regKey = GetKinesisTapRoot();
            regKey.SetValue(name, value, RegistryValueKind.String);
        }

        private static RegistryKey GetKinesisTapRoot()
        {
            return Registry.LocalMachine.CreateSubKey(REG_ROOT, true);
        }
    }
}
