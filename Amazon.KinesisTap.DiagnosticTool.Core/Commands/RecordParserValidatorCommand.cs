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
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;

namespace Amazon.KinesisTap.DiagnosticTool.Core
{
    /// <summary>
    /// The class for record parser validator command
    /// </summary>
    public class RecordParserValidatorCommand : ICommand
    {
        private IDictionary<String, ISourceValidator> _sourceValidators;
        private Func<String, String, IConfigurationRoot> _loadConfigFile;

        public RecordParserValidatorCommand(IDictionary<String, ISourceValidator> sourceValidators, Func<string, string, IConfigurationRoot> loadConfigFile)
        {
            this._sourceValidators = sourceValidators;
            this._loadConfigFile = loadConfigFile;
        }

        /// <summary>
        /// Parse and run the command
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public int ParseAndRunArgument(string[] args)
        {
            if (args.Length == 2 || args.Length == 3)
            {
                RecordParserValidator validator = new RecordParserValidator(AppContext.BaseDirectory, this._sourceValidators, this._loadConfigFile);

                try
                {

                    string sourceID = args[1];

                    string LogName = null;
                    if (args.Length == 3)
                    {
                        LogName = args[2];
                    }

                    bool isValid = validator.ValidateRecordParser(sourceID, LogName, AppContext.BaseDirectory, Constant.CONFIG_FILE, out IList<string> messages);

                    if (isValid)
                    {
                        Console.WriteLine($"Record Parser is valid for Source ID: {sourceID}.");
                    }
                    else
                    {
                        foreach (string message in messages)
                        {
                            Console.WriteLine(message);
                        }
                    }
                    return Constant.NORMAL;
                }
                catch (FormatException ex)
                {
                    Console.WriteLine(ex.ToString());
                    return Constant.INVALID_FORMAT;
                }

            }
            else
            {
                WriteUsage();
                return Constant.INVALID_ARGUMENT;
            }
        }

        /// <summary>
        /// Print Record parser validator usage
        /// </summary>
        public static void WriteUsage()
        {
            Console.WriteLine("Validate RecordParser in configuration file:");
            Console.WriteLine();
            Console.WriteLine("ktdiag /r sourceID [-logName]");
            Console.WriteLine("\t -LogName: Log file name.");
            Console.WriteLine();
        }
    }
}
