using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.Windows
{
    /// <summary>
    /// Windows specific startup code
    /// </summary>
    public class WindowsStartup
    {
        public static void Start()
        {
            Utility.ResolveEnvironmentVariable = WindowsUtility.ResolveEnvironmentVariable;
        }
    }
}
