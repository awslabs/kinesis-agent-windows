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
            return Utility.IsWindow ? WINDOWS_TEST_HOME : Path.Combine(Environment.GetEnvironmentVariable("HOME"), "temp", "kinesistap");
        }
    }
}
