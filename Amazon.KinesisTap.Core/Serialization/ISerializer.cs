using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public interface ISerializer<T>
    {
        void Serialize(Stream stream, T data);
        T Deserialize(Stream stream);
    }
}
