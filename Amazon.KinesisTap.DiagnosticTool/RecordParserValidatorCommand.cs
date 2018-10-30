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
    class RecordParserValidatorCommand : ICommand
    {
        public int ParseAndRunArgument(string[] args)
        {
            if (args.Length == 2 || args.Length == 3)
            {
                RecordParserValidator validator = new RecordParserValidator(AppContext.BaseDirectory);

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

        public void WriteUsage()
        {
            Console.WriteLine("Validate RecordParser in configuration file:");
            Console.WriteLine();
            Console.WriteLine("ktdiag /r sourceID [-logName]");
            Console.WriteLine("\t -LogName: Log file name.");
            Console.WriteLine();
        }
    }
}
