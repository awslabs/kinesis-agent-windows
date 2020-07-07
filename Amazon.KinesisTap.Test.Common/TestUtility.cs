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
using System.Linq;

using Microsoft.Extensions.Configuration;

using Amazon.KinesisTap.Core.Metrics;

namespace Amazon.KinesisTap.Core.Test
{
    public class TestUtility
    {
        public static string LINUX = "Linux";
        private const string WINDOWS_TEST_HOME = @"c:\temp";

        public static IConfigurationSection GetConfig(string parentSection, string id)
        {
            ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
            string basePath = AppContext.BaseDirectory;
            var config = configurationBuilder
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("testSettings.json", optional: false, reloadOnChange: false)
                .Build();

            var sections = config.GetSection(parentSection).GetChildren();

            foreach (var section in sections)
            {
                if (section[ConfigConstants.ID] == id)
                {
                    return section;
                }
            }

            return null;
        }

        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789,. ";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[Utility.Random.Next(s.Length)]).ToArray());
        }

        public static long GetMetricsCount(IDictionary<MetricKey, MetricValue> counters)
        {
            return counters.Values.Sum(v => v.Value);
        }

        public static string GetTestHome()
        {
            return Utility.IsWindows ? WINDOWS_TEST_HOME : Path.Combine(Environment.GetEnvironmentVariable("HOME"), "temp", "kinesistap");
        }

        /// <summary>
        /// Gets the path of the directory containing the root solution (.sln) file.
        /// </summary>
        public static string SolutionDir => FindSolutionDirRecursive(new DirectoryInfo(AppContext.BaseDirectory));

        private static string FindSolutionDirRecursive(DirectoryInfo thisDir)
        {
            if (thisDir.EnumerateFiles("*KinesisTap.sln").Any())
            {
                return thisDir.FullName;
            }

            return thisDir.Parent == null ? null : FindSolutionDirRecursive(thisDir.Parent);
        }
    }
}
