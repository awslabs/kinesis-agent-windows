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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using Microsoft.DotNet.PlatformAbstractions;

namespace Amazon.KinesisTap.Core
{
    public static class ProgramInfo
    {
        private static readonly Process _process = Process.GetCurrentProcess();
        private static double _prevProcessorTime;
        private static DateTime _prevSampleTime;
        private static double _cpuUsage;
        private static readonly object _lockObject = new object();
        private static string _kinesisTapPath;

        /// <summary>
        /// Get Build Number of KinesisTap
        /// </summary>
        /// <returns></returns>
        public static int GetBuildNumber()
        {
            int build = GetKinesisTapVersion().FileBuildPart;
            return build;
        }

        /// <summary>
        /// Gets the full version number of KinesisTap.
        /// </summary>
        /// <returns>String representing KinesisTap's full version number.</returns>
        public static string GetVersionNumber()
        {
            return GetKinesisTapVersion().FileVersion;
        }

        /// <summary>
        /// Get the version of KinesisTap
        /// </summary>
        /// <returns></returns>
        public static FileVersionInfo GetKinesisTapVersion()
        {
            return FileVersionInfo.GetVersionInfo(KinesisTapPath);
        }

        /// <summary>
        /// If this code is invoked from KinesisTap.exe, it will resolve correctly.
        /// If it is invoke by dotnet or unit test, the host need to set the path.
        /// </summary>
        public static string KinesisTapPath
        {
            set
            {
                // Only allow code to set the property if the file actually exists.
                // Otherwise, force the use of the auto-discover code in the getter.
                if (File.Exists(value))
                    _kinesisTapPath = value;
            }
            get
            {
                if (string.IsNullOrWhiteSpace(_kinesisTapPath))
                {
                    string mainModulePath = _process.MainModule.FileName;
                    if (Path.GetFileName(mainModulePath).Equals(ConfigConstants.KINESISTAP_EXE_NAME, StringComparison.CurrentCultureIgnoreCase))
                    {
                        // This happens when running as normal.
                        _kinesisTapPath = mainModulePath;
                    }
                    else if (Utility.IsLinux || Utility.IsMacOs)
                    {
                        // This happens when running tests on a developer machine (Linux/Mac).
                        var netCorePath = Path.Combine(AppContext.BaseDirectory, ConfigConstants.KINESISTAP_CORE);
                        if (File.Exists(netCorePath))
                            _kinesisTapPath = netCorePath;
                    }
                    else if (File.Exists(ConfigConstants.KINESISTAP_STANDARD_PATH))
                    {
                        // This happens when running tests on a developer machine (Windows).
                        _kinesisTapPath = ConfigConstants.KINESISTAP_STANDARD_PATH;
                    }

                    // If none of the above worked...
                    if (string.IsNullOrWhiteSpace(_kinesisTapPath) || !File.Exists(_kinesisTapPath))
                    {
                        // This happens when running unit tests in CodeBuild (Windows container).
                        // We'll try using the version of the assembly that this code exists in.
                        var dllPath = Path.Combine(AppContext.BaseDirectory, "Amazon.KinesisTap.Core.dll");
                        if (File.Exists(dllPath))
                            _kinesisTapPath = dllPath;
                    }
                }
                return _kinesisTapPath;
            }
        }


        /// <summary>
        /// Get memory usage in MB
        /// </summary>
        /// <returns></returns>
        public static double GetMemoryUsage()
        {
            return _process.PrivateMemorySize64 / 1024D / 1024;
        }

        public static double GetCpuUsage()
        {
            double processorTime = _process.TotalProcessorTime.TotalMilliseconds;
            DateTime sampleTime = DateTime.Now;
            double processTimeUsed;
            double timeLapsed;
            lock (_lockObject)
            {
                if (_prevProcessorTime == 0)
                {
                    processTimeUsed = processorTime;
                    timeLapsed = (sampleTime - _process.StartTime).TotalMilliseconds;
                }
                else
                {
                    timeLapsed = (sampleTime - _prevSampleTime).TotalMilliseconds;
                    if (timeLapsed < 10)
                    {
                        return _cpuUsage; //Not long enough, return prev sample;
                    }
                    else
                    {
                        processTimeUsed = processorTime - _prevProcessorTime;
                    }
                }
                _prevProcessorTime = processorTime;
                _prevSampleTime = sampleTime;
                _cpuUsage = processTimeUsed / timeLapsed * 100 / Environment.ProcessorCount;
                return _cpuUsage;
            }
        }
    }
}
