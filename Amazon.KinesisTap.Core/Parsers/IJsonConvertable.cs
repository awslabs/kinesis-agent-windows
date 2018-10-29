using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public interface IJsonConvertable
    {
        string ToJson();
    }
}
