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
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Core
{
    public static class ProcessUtility
    {
        public static string ExecuteProcess(string filepath, string arguments, string input)
        {
            Process process = new Process();
            process.StartInfo.FileName = filepath;
            process.StartInfo.Arguments = arguments;
            process.StartInfo.CreateNoWindow = true;
            process.StartInfo.UseShellExecute = false;
            if (!string.IsNullOrWhiteSpace(input))
            {
                process.StartInfo.RedirectStandardInput = true;
            }
            process.StartInfo.RedirectStandardOutput = true;
            process.StartInfo.RedirectStandardError = true;
            process.Start();
            if (!string.IsNullOrWhiteSpace(input))
            {
                process.StandardInput.WriteLine(input);
                process.StandardInput.Flush();
            }
            string output = process.StandardOutput.ReadToEnd();
            string error = process.StandardError.ReadToEnd();
            process.WaitForExit();
            if (process.ExitCode > 0)
            {
                throw new Exception(string.IsNullOrWhiteSpace(error) ? output : error);
            }
            return output;
        }
    }
}
