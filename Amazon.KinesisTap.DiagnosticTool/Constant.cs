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
