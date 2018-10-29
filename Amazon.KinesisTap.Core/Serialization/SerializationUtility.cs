using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public static class SerializationUtility
    {
        public static void WriteNullableString(this BinaryWriter writer, string value)
        {
            bool isnull = value == null;
            writer.Write(isnull);
            if (!isnull)
            {
                writer.Write(value);
            }
        }

        public static void WriteMemoryStream(this BinaryWriter writer, MemoryStream data)
        {
            writer.Write(data.Length);
            writer.Write(data.ToArray());
        }

        public static void WriteDateTime(this BinaryWriter writer, DateTime dateTime)
        {
            writer.Write(dateTime.Ticks);
        }

        public static string ReadNullableString(this BinaryReader reader)
        {
            bool isnull = reader.ReadBoolean();
            if (isnull)
                return null;
            else
                return reader.ReadString();
        }

        public static MemoryStream ReadMemoryStream(this BinaryReader reader)
        {
            int bufferLength = (int)reader.ReadInt64();
            byte[] data = reader.ReadBytes(bufferLength);
            return new MemoryStream(data);
        }

        public static DateTime ReadDateTime(this BinaryReader reader)
        {
            return new DateTime(reader.ReadInt64());
        }
    }
}
