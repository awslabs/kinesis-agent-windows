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
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text.RegularExpressions;
using Amazon.KinesisTap.Shared;

namespace Amazon.KinesisTap.Core
{
    public static class ProgramInfo
    {
        private static double _prevProcessorTime;
        private static DateTime _prevSampleTime;
        private static double _cpuUsage;
        private static readonly object _lockObject = new();
        private static string _kinesisTapPath;

        /// <summary>
        /// Gets the full version number of KinesisTap.
        /// </summary>
        /// <returns>String representing KinesisTap's full version number.</returns>
        public static string GetVersionNumber()
        {
            var fileVersionAttribute = Assembly.GetEntryAssembly().GetCustomAttribute<AssemblyFileVersionAttribute>();
            return fileVersionAttribute?.Version ?? string.Empty;
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
                    string mainModulePath = Process.GetCurrentProcess().MainModule.FileName;
                    if (Path.GetFileName(mainModulePath).Equals(ConfigConstants.KINESISTAP_EXE_NAME, StringComparison.OrdinalIgnoreCase))
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
        /// Get the memory and CPU utilization (in MB and %) of the process.
        /// </summary>
        public static (double memoryUsage, double cpuUsage) GetMemoryAndCpuUsage()
        {
            // Previously, a singleton Process object is used to query process information.
            // Since .NET 5 that approach no longer returns the correct momentary info, 
            // therefore a new Process object needs to be created every query.
            using var process = Process.GetCurrentProcess();
            return (GetMemoryUsage(process), GetCpuUsage(process));
        }

        /// <summary>
        /// Get the current memory usage in MB.
        /// </summary>
        public static double GetMemoryUsage() => GetMemoryUsage(Process.GetCurrentProcess());

        /// <summary>
        /// Get memory usage in MB
        /// </summary>
        private static double GetMemoryUsage(Process process) => Utility.IsMacOs
            ? GetMacOSMemoryUsage(process)
            : process.WorkingSet64 / 1024D / 1024;

        private static double GetCpuUsage(Process process)
        {
            double processorTime = process.TotalProcessorTime.TotalMilliseconds;
            DateTime sampleTime = DateTime.Now;
            double processTimeUsed;
            double timeLapsed;
            lock (_lockObject)
            {
                if (_prevProcessorTime == 0)
                {
                    processTimeUsed = processorTime;
                    timeLapsed = (sampleTime - process.StartTime).TotalMilliseconds;
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

        /// <summary>
        /// Gets the memory usage on macOS. Process memory information is still unimplemented in .NET Core for macOS, so
        /// we have to use the inbuilt `top` utility and parse the output manually to get the correct value.
        /// </summary>
        /// <returns>Double representing KinesisTap's current memory usage. -1 if we fail to get the right info.</returns>
        private static double GetMacOSMemoryUsage(Process process)
        {
            int pid = process.Id;
            string cmd = $"top -l 1 -pid {pid}";
            IBashShell cmdProcessor = new BashShell();
            string output = cmdProcessor.RunCommand(cmd);
            string[] outputLines = output.Split(new string[] { Environment.NewLine }, StringSplitOptions.None);

            string memoryUsagePattern = @"\d+M";
            string memoryUsageStr = "";

            // The `top` command prints some basic system information along with the specific process information, so we
            // have to iterate through the output lines to find the one we need.
            foreach (string line in outputLines)
            {
                // There should be only one output line starting with a number. The number should be the PID of
                // KinesisTap and the line should contain "amazon" (process name is "amazon-kinesistap").
                if (!line.StartsWith(pid.ToString()) || !line.Contains("amazon")) continue;

                Match m = Regex.Match(line, memoryUsagePattern);
                if (!m.Success) continue;

                // The only part of the line we care about is the first occurrence of the format XXM (XX is a number
                // representing the memory usage of KinesisTap). We're relying on `top` printing the memory usage first
                // among the various megabyte numerical values, but `top` output is fairly well-defined and reliable.
                memoryUsageStr = Regex.Replace(m.Value, "[^0-9]", "");
                break;
            }

            return Utility.ParseInteger(memoryUsageStr, -1);
        }
    }
}
