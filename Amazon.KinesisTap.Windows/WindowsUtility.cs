using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Windows
{
    public class WindowsUtility
    {
        /// <summary>
        /// Provide a search order on how we are going to resolve environment variables on Windows. Mac and Linux only have Process variables.
        /// </summary>
        /// <param name="variable">The name of the environment variable</param>
        /// <returns></returns>
        public static string ResolveEnvironmentVariable(string variable)
        {
            return Environment.GetEnvironmentVariable(variable, EnvironmentVariableTarget.Machine)
                ?? Environment.GetEnvironmentVariable(variable, EnvironmentVariableTarget.User)
                ?? Environment.GetEnvironmentVariable(variable, EnvironmentVariableTarget.Process);
        }
    }
}
