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
        private static readonly IDictionary<String, ISourceValidator> sourceValidators = new Dictionary<String, ISourceValidator>()
        {
            { "DirectorySource", new DirectorySourceValidator() },
            { "WindowsEventLogSource", new EventLogValidator() },
            { "WindowsPerformanceCounterSource", new PerformanceCounterValidator() }
        };

        static void Main(string[] args)
        {

            int exitCode = InvokeCommand(args);

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

                    return new ConfigValidatorCommand(sourceValidators, ConfigFileLoader.LoadConfigFile).ParseAndRunArgument(args);

                case "/r":
                    return new RecordParserValidatorCommand(sourceValidators, ConfigFileLoader.LoadConfigFile).ParseAndRunArgument(args);

                case "/e":
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
            WindowsEventLogSimulatorCommand.WriteUsage();
            DirectoryWatcherCommand.WriteUsage();
            ConfigValidatorCommand.WriteUsage();
            RecordParserValidatorCommand.WriteUsage();
            PackageVersionValidatorCommand.WriteUsage();
        }
    }
}
