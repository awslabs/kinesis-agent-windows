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
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Schema;
using System.Collections.Generic;
using System.IO;

namespace Amazon.KinesisTap.DiagnosticTool.Core
{
    /// <summary>
    /// The class for package version validator
    /// </summary>
    public class PackageVersionValidator
    {
        private readonly JSchema _schema;

        /// <summary>
        /// Package version validator constructor
        /// </summary>
        /// <param name="schemaBaseDirectory"></param>
        public PackageVersionValidator(string schemaBaseDirectory)
        {

            using (StreamReader schemaReader = File.OpenText(Path.Combine(schemaBaseDirectory, Constant.PACKAGE_VERSION_SCHEMA_FILE)))
            using (JsonTextReader jsonReader = new JsonTextReader(schemaReader))
            {
                _schema = JSchema.Load(jsonReader);
            }
        }

        /// <summary>
        /// Validate the package version
        /// </summary>
        /// <param name="filePath"></param>
        /// <param name="messages"></param>
        /// <returns></returns>
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
