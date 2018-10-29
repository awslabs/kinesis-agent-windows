using Amazon.KinesisTap.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Amazon.KinesisTap.Windows.Test
{
    public class WindowsUtilityTest
    {
        [Fact]
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
