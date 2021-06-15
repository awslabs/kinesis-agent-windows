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
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Test.Common;
using System;
using Xunit;

namespace Amazon.KinesisTap.Windows.Test
{
    public class WindowsUtilityTest
    {
        [WindowsOnlyFact]
        public void TestResolveEnvironmentVariable()
        {
            var randomVariable = string.Format("TestVariable{0:yyyyMMddhhmmss}", DateTime.Now);
            string value = Utility.ResolveEnvironmentVariable(randomVariable);
            Assert.Null(value);

            Environment.SetEnvironmentVariable(randomVariable, randomVariable, EnvironmentVariableTarget.Machine);
            //By default, Utility.ResolveEnvironmentVariable use dotnet core GetEnvironmentVariable which resolves to process variable so this should not resolve
            value = Utility.ResolveEnvironmentVariable(randomVariable);
            Assert.Null(value);

            WindowsStartup.Start(); //This attach the Windows version of ResolveEnvironmentVariable
            value = Utility.ResolveEnvironmentVariable(randomVariable);
            Assert.NotEmpty(value);

            //Clean up
            Environment.SetEnvironmentVariable(randomVariable, null, EnvironmentVariableTarget.Machine);
        }
    }
}
