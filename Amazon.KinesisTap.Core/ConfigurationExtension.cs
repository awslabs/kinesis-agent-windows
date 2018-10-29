using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public static class ConfigurationExtension
    {
        public static string GetChildConfig(this IConfiguration config, string parentKey, string childKey)
        {
            return config[$"{parentKey}:{childKey}"];
        }
    }
}
