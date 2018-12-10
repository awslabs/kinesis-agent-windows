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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.DiagnosticTool
{
    class ConfigValidatorCommand : ICommand
    {

        public int ParseAndRunArgument(string[] args)
        {
            if (args.Length == 1 || args.Length == 2)
            {
                try
                {
                    string configPath = null;
                    IList<string> messages;

                    ConfigValidator ConfigFileValidator = new ConfigValidator(AppContext.BaseDirectory);
                    bool isValid = false;

                    if (args.Length == 2)
                    {
                        configPath = args[1];
                        isValid = ConfigFileValidator.ValidateSchema(configPath, out messages);
                    }
                    else
                    {
                        isValid = ConfigFileValidator.ValidateSchema(AppContext.BaseDirectory, Constant.CONFIG_FILE, out messages);
                    }

                    Console.WriteLine("Diagnostic Test #1: Pass! Configuration file is a valid JSON object.");

                    if (isValid)
                    {
                        Console.WriteLine("Diagnostic Test #2: Pass! Configuration file has the valid JSON schema!");
                    }
                    else
                    {
                        Console.WriteLine("Diagnostic Test #2: Fail! Configuration file doesn't have the valid JSON schema: ");
                        foreach (string message in messages)
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

        public void WriteUsage()
        {
            Console.WriteLine("Validate configuration file:");
            Console.WriteLine();
            Console.WriteLine("ktdiag /c [-configPath]");
            Console.WriteLine("\t -configPath: appSettings.json path.");
            Console.WriteLine();
        }
    }
}
