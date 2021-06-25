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
using System.Runtime.Versioning;

namespace Amazon.KinesisTap.DiagnosticTool
{
    /// <summary>
    /// Command for simulating Windows event logs
    /// </summary>
    [SupportedOSPlatform("windows")]
    public class WindowsEventLogSimulatorCommand : ICommand
    {
        /// <summary>
        /// Parse and run the command
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public int ParseAndRunArgument(string[] args)
        {
            if (args.Length < 2)
            {
                WriteUsage();
                return Constant.INVALID_ARGUMENT;
            }

            using (var eventLogSimulator = new WindowsEventLogSimulator(args))
            {
                eventLogSimulator.Start();
                Console.WriteLine("Type any key to exit this program...");
                Console.ReadKey();
                return Constant.NORMAL;
            }
        }

        /// <summary>
        /// Print the options
        /// </summary>
        public static void WriteUsage()
        {
            Console.WriteLine("Simulate Windows Event Log:");
            Console.WriteLine();
            Console.WriteLine("ktdiag /e logName [-tn] [-sm] [-bk]");
            Console.WriteLine("\t -tn:n is the interval between writing log records in millisecond. The default 1000 millisecond or 1 second.");
            Console.WriteLine("\t -sm:m is the size of each log record in bytes. The default 1000 bytes or 1 KB. The maximum is 32766.");
            Console.WriteLine("\t -bk:k is the batch size. The default is 1.");
            Console.WriteLine();
        }
    }
}
