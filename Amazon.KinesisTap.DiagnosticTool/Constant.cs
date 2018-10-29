using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.DiagnosticTool
{
    class Constant
    {
        public const string CONFIG_SCHEMA_FILE = "appsettingsSchema.json";
        public const string CONFIG_FILE = "appsettings.json";
        public const string PACKAGE_VERSION_SCHEMA_FILE = "PackageVersionSchema.json";

        public const int NORMAL = 0;
        public const int INVALID_ARGUMENT = 1;
        public const int INVALID_FORMAT = 2;
        public const int RUNTIME_ERROR = 3;
    }
}
