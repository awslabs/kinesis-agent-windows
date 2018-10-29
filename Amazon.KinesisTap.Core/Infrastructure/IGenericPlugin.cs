using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    //For plugins that are not specialized, such as source, sink, crendential providers
    public interface IGenericPlugin : IPlugIn
    {
    }
}
