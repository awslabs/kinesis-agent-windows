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

namespace Amazon.KinesisTap.DiagnosticTool.Core
{
    /// <summary>
    /// The class for Package version validator command
    /// </summary>
    public class PackageVersionValidatorCommand : ICommand
    {
        /// <summary>
        /// Parse and run the command
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public int ParseAndRunArgument(string[] args)
        {
            if (args.Length != 2)
            {
                WriteUsage();
                return Constant.INVALID_ARGUMENT;
            }

            try
            {
                var packageVersionValidator = new PackageVersionValidator(AppContext.BaseDirectory);

                bool isValid = packageVersionValidator.ValidatePackageVersion(args[1], out IList<string> messages);

                if (isValid)
                {
                    Console.WriteLine("Diagnostic Test: Pass! Your packageVersion.json is valid.");
                }
                else
                {
                    Console.WriteLine("Diagnostic Test: Fail! Your packageVersion.json is invalid: ");
                    foreach (string message in messages)
                    {
                        Console.WriteLine(message);
                    }

                    Console.WriteLine("Please fix the packageVersion.json to match the Json schema. You can follow the schema file named 'packageVersionSchema.json' to draft your packageVersion.json.");
                }

                return Constant.NORMAL;
            }
            catch (FormatException fex)
            {
                Console.WriteLine("Diagnostic Test: Fail! Your packageVersion.json is not a valid Json file.");
                Console.WriteLine(fex.Message);
                return Constant.INVALID_FORMAT;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                return Constant.RUNTIME_ERROR;
            }
        }

        /// <summary>
        /// Print Rackage version validator usage
        /// </summary>
        public static void WriteUsage()
        {
            Console.WriteLine("Validate packageVersion.json before it is uploaded to s3:");
            Console.WriteLine();
            Console.WriteLine("ktdiag /p filepath");
            Console.WriteLine();
        }
    }
}
