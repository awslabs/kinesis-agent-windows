using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public interface IFactoryCatalog<T>
    {
        void RegisterFactory(string entry, IFactory<T> factory);
        IFactory<T> GetFactory(string entry);
    }
}
