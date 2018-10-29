using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public interface IParameterStore
    {
        void SetParameter(string name, string value);
        string GetParameter(string name);
    }
}
