using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    public class UtilityTest
    {
        [Fact]
        public void TestStringToStream()
        {
            string testString = "test string";
            using (Stream stream = Utility.StringToStream(testString))
            using (StreamReader sr = new StreamReader(stream))
            {
                string stringFromStream = sr.ReadToEnd();
                Assert.Equal(testString, stringFromStream);
            }
        }

        [Fact]
        public void TestSplitPrefix()
        {
            string testString = "Key1=Value1";
            var (prefix, suffix) = Utility.SplitPrefix(testString, '=');
            Assert.Equal("Key1", prefix);
            Assert.Equal("Value1", suffix);
        }

        [Fact]
        public void TestSplitNoPrefix()
        {
            string testString = "ValueOnly";
            var (prefix, suffix) = Utility.SplitPrefix(testString, '=');
            Assert.Null(prefix);
            Assert.Equal(testString, suffix);
        }

        [Fact]
        [Trait("Category", "Windows")]
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

            string dateTimeVariable = "{DateTime:yyyy-MM-dd}";
            Assert.Equal(dateTimeVariable, Utility.ResolveVariable(dateTimeVariable));

            //Non-existing environment variable
            string notExistingVariable = "{instance_id}";
            Assert.Equal(notExistingVariable, Utility.ResolveVariable(notExistingVariable));
        }

        [Fact]
        [Trait("Category", "Windows")]
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
    }
}
