using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.AutoUpdate
{
    /// <summary>
    /// This is the model class for PackageVersion.json file
    /// </summary>
    public class PackageVersionInfo
    {
        public string Name { get; set; }
        public string Version { get; set; }
        public string PackageUrl { get; set; }
    }
}
