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
using System.IO;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.DiagnosticTool.Core
{
    /// <summary>
    /// The class for validating configuration file command
    /// </summary>
    public class ConfigValidatorCommand : ICommand
    {
        private readonly IDictionary<string, ISourceValidator> _sourceValidators;
        private readonly Func<string, string, IConfigurationRoot> _loadConfigFile;

        public ConfigValidatorCommand(IDictionary<string, ISourceValidator> sourceValidators, Func<string, string, IConfigurationRoot> loadConfigFile)
        {
            _sourceValidators = sourceValidators;
            _loadConfigFile = loadConfigFile;
        }

        public ConfigValidatorCommand(Func<string, string, IConfigurationRoot> loadConfigFile)
        {
            _sourceValidators = new Dictionary<string, ISourceValidator>();
            _loadConfigFile = loadConfigFile;
        }

        /// <summary>
        /// Parse and run the command
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public int ParseAndRunArgument(string[] args)
        {
            if (args.Length == 1 || args.Length == 2)
            {
                try
                {
                    string configPath = null;
                    IList<string> messages;

                    var ConfigFileValidator = new ConfigValidator(AppContext.BaseDirectory, _sourceValidators, _loadConfigFile);
                    var isValid = false;

                    if (args.Length == 2)
                    {
                        configPath = args[1];
                        isValid = ConfigFileValidator.ValidateSchema(configPath, out messages);
                    }
                    else
                    {
                        isValid = ConfigFileValidator.ValidateSchema(Utility.GetKinesisTapConfigPath(), Constant.CONFIG_FILE, out messages);
                    }

                    Console.WriteLine("Diagnostic Test #1: Pass! Configuration file is a valid JSON object.");

                    if (isValid)
                    {
                        Console.WriteLine("Diagnostic Test #2: Pass! Configuration file has the valid JSON schema!");
                    }
                    else
                    {
                        Console.WriteLine("Diagnostic Test #2: Fail! Configuration file doesn't have the valid JSON schema: ");
                        foreach (var message in messages)
                        {
                            Console.WriteLine(message);
                        }

                        Console.WriteLine("Please fix the Configuration file to match the JSON schema");
                    }

                    return Constant.NORMAL;
                }
                catch (FormatException ex)
                {
                    Console.WriteLine("Diagnostic Test #1: Fail! Configuration file is not a valid JSON object.");
                    Console.WriteLine(ex.Message);
                    return Constant.INVALID_FORMAT;
                }
                catch (FileNotFoundException ex)
                {
                    Console.WriteLine("Diagnostic Test #1: Fail! Configuration file is not found.");
                    Console.WriteLine(ex.ToString());
                    return Constant.RUNTIME_ERROR;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    return Constant.RUNTIME_ERROR;
                }
            }
            else
            {
                WriteUsage();
                return Constant.INVALID_ARGUMENT;
            }
        }

        /// <summary>
        /// Print configuration file validator usage
        /// </summary>
        public static void WriteUsage()
        {
            Console.WriteLine("Validate configuration file:");
            Console.WriteLine();
            Console.WriteLine("ktdiag /c [-configPath]");
            Console.WriteLine("\t -configPath: appSettings.json path.");
            Console.WriteLine();
        }
    }
}
