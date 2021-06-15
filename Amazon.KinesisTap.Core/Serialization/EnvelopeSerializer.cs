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
using System.IO;

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
