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
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Test.Common;
using Amazon.KinesisTap.Shared;
using Xunit;
using Moq;

namespace Amazon.KinesisTap.Core.Test
{
    public class UtilityTest
    {
        [Fact]
        public void TestStringToStream()
        {
            var testString = "test string";
            using (Stream stream = Utility.StringToStream(testString))
            using (var sr = new StreamReader(stream))
            {
                var stringFromStream = sr.ReadToEnd();
                Assert.Equal(testString, stringFromStream);
            }
        }

        [Fact]
        public void TestSplitPrefix()
        {
            var testString = "Key1=Value1";
            var (prefix, suffix) = Utility.SplitPrefix(testString, '=');
            Assert.Equal("Key1", prefix);
            Assert.Equal("Value1", suffix);
        }

        [Fact]
        public void TestSplitNoPrefix()
        {
            var testString = "ValueOnly";
            var (prefix, suffix) = Utility.SplitPrefix(testString, '=');
            Assert.Null(prefix);
            Assert.Equal(testString, suffix);
        }

        [WindowsOnlyFact]
        public void TestResolveWindowsVariable()
        {
            Assert.Equal(Environment.GetEnvironmentVariable("ProgramFiles"), Utility.ResolveVariable("{ProgramFiles}"));
            Assert.Equal(Environment.GetEnvironmentVariable("ProgramFiles(x86)"), Utility.ResolveVariable("{env:ProgramFiles(x86)}"));
            Assert.Throws<ArgumentException>(() => Utility.ResolveVariable(@"{ProgramFiles}\Amazon\KinesisTap"));
        }

        [Fact]
        public void TestResolveVariable()
        {
            Assert.Throws<ArgumentException>(() => Utility.ResolveVariable(null));
            Assert.Throws<ArgumentException>(() => Utility.ResolveVariable(string.Empty));
            Assert.Throws<ArgumentException>(() => Utility.ResolveVariable("{}"));

            var dateTimeVariable = "{DateTime:yyyy-MM-dd}";
            Assert.Equal(dateTimeVariable, Utility.ResolveVariable(dateTimeVariable));

            //Non-existing environment variable
            var notExistingVariable = "{instance_id}";
            Assert.Equal(notExistingVariable, Utility.ResolveVariable(notExistingVariable));

            // Test currentUser variable
            var bash = new Mock<IBashShell>();
            string usernameVariable = $"{{{ConfigConstants.CURRENT_USER}}}";
            
            // Test to verify if we get the expected username            
            string username = "testuser";
            bash.Setup(m => m.RunCommand(It.IsAny<string>(), It.IsAny<int>())).
             Returns(username);
            Utility.processor = bash.Object;
            
            // MacOS will resolve the correct username but not the other OS.
            if(OperatingSystem.IsMacOS())
                Assert.Equal(username, Utility.ResolveVariable(usernameVariable));
            else
                Assert.Equal(usernameVariable, Utility.ResolveVariable(usernameVariable));

            // Test to verify that variable is not resolved when username is root
            username = "root";
            bash.Setup(m => m.RunCommand(It.IsAny<string>(), It.IsAny<int>())).
             Returns(username);
            Utility.processor = bash.Object;
            
            Assert.Equal(usernameVariable, Utility.ResolveVariable(usernameVariable));

            // Test for unifortimestamp 
            // Mock the UniformServerTime class
            var uniformservertimeMockObj = new Mock<IUniformServerTime>();
            DateTime currentUTCtime = DateTime.UtcNow;

            uniformservertimeMockObj.Setup(m => m.GetNTPServerTime())
                .Returns(() => currentUTCtime);
            Utility.UniformTime = uniformservertimeMockObj.Object;

            var uniformtimestampvariable = "{uniformtimestamp}";

            var uniformtimestamp = Utility.ResolveVariable(uniformtimestampvariable);
            var baseTime = currentUTCtime.ToString("yyyy-MM-ddTHH:mm:ss.fff UTC");

            Assert.Equal(baseTime, uniformtimestamp);
        }

        [WindowsOnlyFact]
        public void TestResolveWindowsVariables()
        {
            Assert.Equal(Environment.GetEnvironmentVariable("ProgramFiles") + @"\Amazon\KinesisTap",
                Utility.ResolveVariables(@"{ProgramFiles}\Amazon\KinesisTap", Utility.ResolveVariable));
            Assert.Equal($"{Environment.GetEnvironmentVariable("USERDOMAIN")}\\{Environment.GetEnvironmentVariable("USERNAME")}",
                Utility.ResolveVariables(@"{USERDOMAIN}\{USERNAME}", Utility.ResolveVariable));
        }

        [Fact]
        public void TestFromEpochTime()
        {
            Assert.Equal(new DateTime(2018, 9, 21, 8, 38, 50, 972), Utility.FromEpochTime(1537519130972L));
        }

        [Theory]
        [InlineData(1000)]
        [InlineData(1500)]
        public void TestGetElapsedMilliseconds(int passedMillis)
        {
            var elapsed1 = Utility.GetElapsedMilliseconds();
            Thread.Sleep(passedMillis);

            var elapsed2 = Utility.GetElapsedMilliseconds();
            AssertAboutEqual(passedMillis, elapsed2 - elapsed1, 50);
        }

        [Fact]
        public async Task TestThreadSafeRandom()
        {
            const int taskCount = 1000;
            var semaphore = new SemaphoreSlim(0, taskCount);
            var cts = new CancellationTokenSource();
            var tasks = new List<Task>();
            for (var i = 0; i < taskCount; i++)
            {
                tasks.Add(MyThread(semaphore, cts.Token));
            }

            semaphore.Release(taskCount);

            await Task.Delay(20 * 1000);
            cts.Cancel();

            var newRandoms = new List<double>();
            for (var i = 0; i < 10; i++)
            {
                newRandoms.Add(Utility.Random.NextDouble());
            }

            // if the number of '0's is half the newly generated random, then we're failed
            Assert.True(newRandoms.Count(d => d == 0) < 5);
        }

        private static async Task MyThread(SemaphoreSlim semaphore, CancellationToken cancellationToken)
        {
            await semaphore.WaitAsync();

            while (!cancellationToken.IsCancellationRequested)
            {
                _ = Utility.Random.NextDouble();
                await Task.Delay(1);
            }
        }

        private static void AssertAboutEqual(long expected, long actual, long epsilon)
        {
            var diff = Math.Abs(expected - actual);
            Assert.True(diff < epsilon, $"Expected: {expected}, actual: {actual}, diff: {diff}");
        }
    }
}
