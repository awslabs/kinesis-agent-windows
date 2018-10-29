using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Schema;
using System.Collections.Generic;
using System.IO;

namespace Amazon.KinesisTap.DiagnosticTool
{
    public class PackageVersionValidator
    {

        private readonly JSchema _schema;

        public PackageVersionValidator(string schemaBaseDirectory)
        {

            using (StreamReader schemaReader = File.OpenText(Path.Combine(schemaBaseDirectory, Constant.PACKAGE_VERSION_SCHEMA_FILE)))
            using (JsonTextReader jsonReader = new JsonTextReader(schemaReader))
            {
                _schema = JSchema.Load(jsonReader);
            }
        }

        public bool ValidatePackageVersion (string filePath, out IList<string> messages)
        {
            using (StreamReader packageaVersionReader = File.OpenText(filePath))
            using (JsonTextReader jsonReader = new JsonTextReader(packageaVersionReader))
            {
                JToken token = JToken.ReadFrom(new JsonTextReader(packageaVersionReader));

                return token.IsValid(_schema, out messages);
            }

        }
    }
}
