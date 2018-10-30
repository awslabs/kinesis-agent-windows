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
    class PackageVersionValidatorCommand : ICommand
    {
        public int ParseAndRunArgument(string[] args)
        {
            if (args.Length > 3)
            {
                WriteUsage();
                return Constant.INVALID_ARGUMENT;
            }

            try
            {
                var packageVersionValidator = new PackageVersionValidator(AppContext.BaseDirectory);

                bool isValid = packageVersionValidator.ValidatePackageVersion(args[1], out IList<string> messages);
                Console.WriteLine("Diagnostic Test #1: Pass! Configuration file is a valid Json object.");

                if (isValid)
                {
                    Console.WriteLine("Diagnostic Test #2: Pass! Configuration file has the valid Json schema!");
                }
                else
                {
                    Console.WriteLine("Diagnostic Test #2: Fail! Configuration file doesn't have the valid Json schema: ");
                    foreach (string message in messages)
                    {
                        Console.WriteLine(message);
                    }

                    Console.WriteLine("Please fix the Configuration file to match the Json schema.");
                }

                return Constant.NORMAL;
            }
            catch (FormatException fex)
            {
                Console.WriteLine("Diagnostic Test #1: Fail! Configuration file is not a valid Json object.");
                Console.WriteLine(fex.Message);
                return Constant.INVALID_FORMAT;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                return Constant.RUNTIME_ERROR;
            }
        }

        public void WriteUsage()
        {
            Console.WriteLine("Validate PackageVersion.json before it is uploaded to s3:");
            Console.WriteLine();
            Console.WriteLine("ktdiag /p filepath");
            Console.WriteLine();
        }
    }
}
