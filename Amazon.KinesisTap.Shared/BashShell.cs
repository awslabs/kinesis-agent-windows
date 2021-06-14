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
using System.Diagnostics;

namespace Amazon.KinesisTap.Shared
{
    /// <summary>
    /// Default implementation of <see cref="IBashShell"/>.
    /// </summary>
    public class BashShell : IBashShell
    {
        /// <inheritdoc/>
        public string RunCommand(string cmd, int timeout)
        {
            string escaped = cmd.Replace("\"", "\\\"");
            using Process process = new Process()
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = "/bin/bash",
                    Arguments = $"-c \"{escaped}\"",
                    CreateNoWindow = true,
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                },
            };
            process.Start();
            string stdoutContent = process.StandardOutput.ReadToEnd();
            if (!process.WaitForExit(timeout))
            {
                throw new TimeoutException($"Process \"{cmd}\" timed out.");
            }

            int exitCode = process.ExitCode;
            if (exitCode != 0)
            {
                string stderrContent = process.StandardError.ReadToEnd();
                throw new Exception($"Command exited with unsuccessful error code: {exitCode}, stdout content: {stdoutContent}, and stderr content: {stderrContent}.");
            }

            return stdoutContent;
        }
    }
}