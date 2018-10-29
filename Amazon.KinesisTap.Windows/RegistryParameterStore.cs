using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Amazon.KinesisTap.Core;
using Microsoft.Win32;

namespace Amazon.KinesisTap.Windows
{
    public class RegistryParameterStore : IParameterStore
    {
        const string REG_ROOT = @"SOFTWARE\Amazon\AWSKinesisTap";

        public string GetParameter(string name)
        {
            RegistryKey regKey = GetKinesisTapRoot();
            return regKey.GetValue(name) as string;
        }

        public void SetParameter(string name, string value)
        {
            RegistryKey regKey = GetKinesisTapRoot();
            regKey.SetValue(name, value, RegistryValueKind.String);
        }

        private static RegistryKey GetKinesisTapRoot()
        {
            return Registry.LocalMachine.CreateSubKey(REG_ROOT, true);
        }
    }
}
