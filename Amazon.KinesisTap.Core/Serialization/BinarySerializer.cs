using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public class BinarySerializer<T> : ISerializer<T>
    {
        protected Action<BinaryWriter, T> _serialize;
        protected Func<BinaryReader, T> _deserialize;

        public BinarySerializer(Action<BinaryWriter, T> serialize, Func<BinaryReader, T> deserialize)
        {
            _serialize = serialize;
            _deserialize = deserialize;
        }

        public T Deserialize(Stream stream)
        {
            BinaryReader reader = new BinaryReader(stream);
            return _deserialize(reader);
        }

        public void Serialize(Stream stream, T data)
        {
            BinaryWriter writer = new BinaryWriter(stream);
            _serialize(writer, data);
        }
    }
}
