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
namespace Amazon.KinesisTap.DiagnosticTool.Core
{
    /// <summary>
    /// The constant variables in the Diagnostic tool
    /// </summary>
    public static class Constant
    {
        public const string CONFIG_SCHEMA_FILE = "appsettingsSchema.json";
        public const string CONFIG_FILE = "appsettings.json";
        public const string PACKAGE_VERSION_SCHEMA_FILE = "packageVersionSchema.json";

        public const string KINESISTAP_DIAGNOSTIC_TOOL_EXE_NAME = "KTDiag.exe";

        public const int NORMAL = 0;
        public const int INVALID_ARGUMENT = 1;
        public const int INVALID_FORMAT = 2;
        public const int RUNTIME_ERROR = 3;

        //None-Windows
        public const string LINUX_DEFAULT_PROGRAM_DATA_PATH = "/opt/amazon-kinesistap/etc";
    }
}
