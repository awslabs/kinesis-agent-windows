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
    class DirectoryWatcherCommand : ICommand
    {

        public int ParseAndRunArgument(string[] args)
        {
            if (args.Length % 2 == 0)
            {
                WriteUsage();
                return Constant.INVALID_ARGUMENT;
            }

            int directoryCount = args.Length / 2;
            DirectoryWatcher[] watchers = new DirectoryWatcher[directoryCount];

            try
            {
                for (int i = 0; i < directoryCount; i++)
                {
                    watchers[i] = new DirectoryWatcher(args[2 * i + 1], args[2 * i + 2], Console.Out);
                }
                Console.WriteLine("Type any key to exit this program...");
                Console.ReadKey();
                return Constant.NORMAL;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                return Constant.RUNTIME_ERROR;
            }
            finally
            {
                for (int i = 0; i < directoryCount; i++)
                {
                    watchers[i]?.Dispose();
                }
            }
        }

        public void WriteUsage()
        {
            Console.WriteLine("Watch a directory:");
            Console.WriteLine();
            Console.WriteLine("ktdiag /w directory1, filter1, [directory2], [filter2]...");
            Console.WriteLine();
        }
    }
}
