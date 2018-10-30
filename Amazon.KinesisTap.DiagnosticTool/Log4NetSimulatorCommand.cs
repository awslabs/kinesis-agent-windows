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
    class Log4NetSimulatorCommand : ICommand
    {
 
        public int ParseAndRunArgument(string[] args)
        {
            if (args.Length < 2)
            {
                WriteUsage();
                return Constant.INVALID_ARGUMENT;
            }

            var l4nSimulator = new Log4NetSimulator(args);
            l4nSimulator.Start();
            Console.WriteLine("Type any key to exit this program...");
            Console.ReadKey();
            return Constant.NORMAL;
        }

        public void WriteUsage()
        {
            Console.WriteLine("Simulate log4net writing:");
            Console.WriteLine();
            Console.WriteLine("ktdiag /log4net path [-lm|-li|-le] [-tn] [-sm] [-bk]");
            Console.WriteLine("\t -lm:MinimumLock");
            Console.WriteLine("\t -li:InterProcessLock");
            Console.WriteLine("\t -le:ExclusiveLock. The default.");
            Console.WriteLine("\t -tn:n is the interval between writing log records in millisecond. The default 1000 millisecond or 1 second.");
            Console.WriteLine("\t -sm:m is the size of each log record in bytes. The default 1000 bytes or 1 KB.");
            Console.WriteLine("\t -bk:k is the batch size. The default is 1.");
            Console.WriteLine();
        }
    }
}
