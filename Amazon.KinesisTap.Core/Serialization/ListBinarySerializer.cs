using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public class ListBinarySerializer<T>
    {
        protected Action<BinaryWriter, T> _serialize;
        protected Func<BinaryReader, T> _deserialize;

        public ListBinarySerializer(Action<BinaryWriter, T> serialize, Func<BinaryReader, T> deserialize)
        {
            _serialize = serialize;
            _deserialize = deserialize;
        }

        public List<T> Deserialize(BinaryReader reader)
        {
            List<T> entries = new List<T>();
            int count = reader.ReadInt32();
            for (int i = 0; i < count; i++)
            {
                T entry = _deserialize(reader);
                entries.Add(entry);
            }
            return entries;
        }

        public void Serialize(BinaryWriter writer, List<T> data)
        {
            writer.Write(data.Count);
            foreach (var entry in data)
            {
                _serialize(writer, entry);
            }
        }
    }
}
