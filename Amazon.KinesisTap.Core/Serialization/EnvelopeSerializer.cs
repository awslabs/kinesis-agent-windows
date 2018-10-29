using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public class EnvelopeSerializer<T>
    {
        protected Action<BinaryWriter, T> _serialize;
        protected Func<BinaryReader, T> _deserialize;

        public EnvelopeSerializer(Action<BinaryWriter, T> serialize, Func<BinaryReader, T> deserialize)
        {
            _serialize = serialize;
            _deserialize = deserialize;
        }

        public Envelope<T> Deserialize(BinaryReader reader)
        {
            DateTime timestamp = reader.ReadDateTime();
            T data = _deserialize(reader);
            return new Envelope<T>(data, timestamp);
        }

        public void Serialize(BinaryWriter writer, Envelope<T> data)
        {
            writer.WriteDateTime(data.Timestamp);
            _serialize(writer, data.Data);
        }
    }
}
