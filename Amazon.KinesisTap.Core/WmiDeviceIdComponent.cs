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
using System.Collections.Generic;
using System.Management;
using System.Runtime.Versioning;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// An implementation of WmiDeviceIdComponent class that retrieves data from a WMI class.
    /// </summary>
    [SupportedOSPlatform("windows")]
    public class WmiDeviceIdComponent
    {
        /// <summary>
        /// The WMI class name.
        /// </summary>
        private readonly string _wmiClass;

        /// <summary>
        /// The WMI property name.
        /// </summary>
        private readonly string _wmiProperty;

        /// <summary>
        /// Initializes a new instance of the <see cref="WmiDeviceIdComponent"/> class.
        /// </summary>
        /// <param name="wmiClass">The WMI class name.</param>
        /// <param name="wmiProperty">The WMI property name.</param>
        public WmiDeviceIdComponent(string wmiClass, string wmiProperty)
        {
            _wmiClass = wmiClass;
            _wmiProperty = wmiProperty;
        }

        /// <summary>
        /// Gets the component value.
        /// </summary>
        /// <returns>The component value.</returns>
        public string GetValue()
        {
            var values = new List<string>();

            try
            {
                using var managementObjectSearcher = new ManagementObjectSearcher($"SELECT {_wmiProperty} FROM {_wmiClass}");
                using var managementObjectCollection = managementObjectSearcher.Get();
                foreach (var managementObject in managementObjectCollection)
                {
                    try
                    {
                        var value = managementObject[_wmiProperty] as string;
                        if (value != null)
                        {
                            values.Add(value);
                        }
                    }
                    finally
                    {
                        managementObject.Dispose();
                    }
                }
            }
            catch
            {

            }

            values.Sort();

            return (values != null && values.Count > 0)
                ? string.Join(",", values.ToArray())
                : string.Empty;
        }
    }
}
