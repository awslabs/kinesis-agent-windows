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
using Amazon.KinesisTap.DiagnosticTool.Core;
using System;
using System.Collections.Generic;

namespace Amazon.KinesisTap.DiagnosticTool
{
    class Program
    {
        // Source validators in Windows platform
        private static readonly IDictionary<string, ISourceValidator> _sourceValidators = new Dictionary<string, ISourceValidator>();

        static void Main(string[] args)
        {
            _sourceValidators["DirectorySource"] = new DirectorySourceValidator();
            if (OperatingSystem.IsWindows())
            {
                _sourceValidators["WindowsEventLogSource"] = new EventLogValidator();
            }
            if (OperatingSystem.IsWindows())
            {
                _sourceValidators["WindowsPerformanceCounterSource"] = new PerformanceCounterValidator();
            }

            var exitCode = InvokeCommand(args);
            Environment.Exit(exitCode);
        }

        private static int InvokeCommand(string[] args)
        {
            if (args.Length == 0)
            {
                WriteUsage();
                return Constant.NORMAL;
            }

            switch (args[0])
            {
                case "/w":
                case "-w":
                    return new DirectoryWatcherCommand().ParseAndRunArgument(args);

                case "/log4net":
                    return new Log4NetSimulatorCommand().ParseAndRunArgument(args);

                case "/c":
                case "/config":

                    return new ConfigValidatorCommand(_sourceValidators, ConfigFileLoader.LoadConfigFile).ParseAndRunArgument(args);

                case "/r":
                    return new RecordParserValidatorCommand(_sourceValidators, ConfigFileLoader.LoadConfigFile).ParseAndRunArgument(args);

                case "/e":
                    if (!OperatingSystem.IsWindows())
                    {
                        throw new PlatformNotSupportedException();
                    }
                    return new WindowsEventLogSimulatorCommand().ParseAndRunArgument(args);

                case "/p":   // Validate the PackageVersion.json
                    return new PackageVersionValidatorCommand().ParseAndRunArgument(args);

                default:
                    WriteUsage();
                    return Constant.INVALID_ARGUMENT;
            }
        }

        /// <summary>
        /// List the functionalities of the Diagnostic tool
        /// </summary>
        private static void WriteUsage()
        {
            Log4NetSimulatorCommand.WriteUsage();
            if (OperatingSystem.IsWindows())
            {
                WindowsEventLogSimulatorCommand.WriteUsage();
            }

            DirectoryWatcherCommand.WriteUsage();
            ConfigValidatorCommand.WriteUsage();
            RecordParserValidatorCommand.WriteUsage();
            PackageVersionValidatorCommand.WriteUsage();
        }
    }
}
