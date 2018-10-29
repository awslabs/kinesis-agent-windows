using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public interface IDataSink<in T>
    {
        void RegisterDataSource(IDataSource<T> source);
    }
}
