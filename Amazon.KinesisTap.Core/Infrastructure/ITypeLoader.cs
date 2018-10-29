using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public interface ITypeLoader
    {
        IEnumerable<T> LoadTypes<T>();
    }
}
