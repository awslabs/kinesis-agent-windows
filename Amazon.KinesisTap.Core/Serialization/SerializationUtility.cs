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
