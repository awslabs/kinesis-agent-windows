/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
namespace Amazon.KinesisTap.Core
{
    using System;
    using System.Collections.Concurrent;
    using System.IO;
    using System.Xml.Serialization;
    using Newtonsoft.Json;

    public static class SerializationUtility
    {
        /// <summary>
        /// Gets a static instance of the <see cref="JsonSerializer"/> class, using the default settings,
        /// with overrides for ReferenceLoopHandling (Ignore) and NullValueHandling (Ignore).
        /// </summary>
        public static readonly JsonSerializer Json = JsonSerializer.CreateDefault(new JsonSerializerSettings
        {
            ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
            NullValueHandling = NullValueHandling.Ignore
        });

        /// <summary>
        /// Gets a static dictionary of <see cref="XmlSerializer"/> instances used for converting objects to Xml.
        /// This allows multiple Envelopes to reuse serializers rather than creating new ones each time.
        /// </summary>
        public static readonly ConcurrentDictionary<Type, XmlSerializer> XmlSerializers = new ConcurrentDictionary<Type, XmlSerializer>();

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
