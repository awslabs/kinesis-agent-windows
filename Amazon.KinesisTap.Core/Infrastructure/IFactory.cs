using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public interface IFactory<T>
    {
        void RegisterFactory(IFactoryCatalog<T> catalog);
        T CreateInstance(string entry, IPlugInContext context);
    }
}
