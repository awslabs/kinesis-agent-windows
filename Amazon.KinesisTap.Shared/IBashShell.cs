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
namespace Amazon.KinesisTap.Shared
{
    /// <summary>
    /// Interface for a Bash shell, only works on *nix systems.
    /// </summary>
    public interface IBashShell
    {
        /// <summary>
        /// Run a bash command.
        /// </summary>
        /// <param name="cmd">The command to run.</param>
        /// <param name="timeout">How long the system should wait (in milliseconds) before timing out.</param>
        /// <returns>The STDOUT output of the command.</returns>
        string RunCommand(string cmd, int timeout = 30000);
    }
}
