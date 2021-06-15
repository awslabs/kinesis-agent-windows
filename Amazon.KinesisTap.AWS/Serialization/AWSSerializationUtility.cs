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
using System.Collections.Generic;
using System.IO;
using Amazon.CloudWatchLogs.Model;
using KFM = Amazon.KinesisFirehose.Model;
using KM = Amazon.Kinesis.Model;
using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.AWS
{
    public static class AWSSerializationUtility
    {
        #region CloudWatch Logs
        public static InputLogEvent ReadInputLogEvent(this BinaryReader reader)
        {
            var data = new InputLogEvent();
            data.Message = reader.ReadNullableString();
            data.Timestamp = reader.ReadDateTime();
            return data;
        }

        public static void WriteInputLogEvent(this BinaryWriter writer, InputLogEvent data)
        {
            writer.WriteNullableString(data.Message);
            writer.WriteDateTime(data.Timestamp);
        }

        private static BinarySerializer<List<Envelope<InputLogEvent>>> _inputLogEventListBinarySerializer;

        public static BinarySerializer<List<Envelope<InputLogEvent>>> InputLogEventListBinarySerializer
        {
            get
            {
                if (_inputLogEventListBinarySerializer == null)
                {
                    var envelopeSerializer = new EnvelopeSerializer<InputLogEvent>(
                        WriteInputLogEvent,
                        ReadInputLogEvent);

                    var listWriter = new ListBinarySerializer<Envelope<InputLogEvent>>(
                        envelopeSerializer.Serialize,
                        envelopeSerializer.Deserialize);

                    _inputLogEventListBinarySerializer = new BinarySerializer<List<Envelope<InputLogEvent>>>(
                        listWriter.Serialize,
                        listWriter.Deserialize
                        );
                }
                return _inputLogEventListBinarySerializer;
            }
        }
        #endregion

        #region Firehose
        public static KFM.Record ReadFirehoseRecord(this BinaryReader reader)
        {
            var record = new KFM.Record();
            record.Data = reader.ReadMemoryStream();
            return record;
        }

        public static void WriteFirehoseRecord(this BinaryWriter writer, KFM.Record data)
        {
            writer.WriteMemoryStream(data.Data);
        }

        private static BinarySerializer<List<Envelope<KFM.Record>>> _firehoseRecordListBinarySerializer;

        public static BinarySerializer<List<Envelope<KFM.Record>>> FirehoseRecordListBinarySerializer
        {
            get
            {
                if (_firehoseRecordListBinarySerializer == null)
                {
                    var envelopeSerializer = new EnvelopeSerializer<KFM.Record>(
                        WriteFirehoseRecord,
                        ReadFirehoseRecord);

                    var listWriter = new ListBinarySerializer<Envelope<KFM.Record>>(
                        envelopeSerializer.Serialize,
                        envelopeSerializer.Deserialize);

                    _firehoseRecordListBinarySerializer = new BinarySerializer<List<Envelope<KFM.Record>>>(
                        listWriter.Serialize,
                        listWriter.Deserialize
                        );
                }
                return _firehoseRecordListBinarySerializer;
            }
        }
        #endregion

        #region Kinesis Stream
        public static KM.PutRecordsRequestEntry ReadPutRecordsRequestEntry(this BinaryReader reader)
        {
            var entry = new KM.PutRecordsRequestEntry();
            entry.PartitionKey = reader.ReadNullableString();
            entry.ExplicitHashKey = reader.ReadNullableString();
            entry.Data = reader.ReadMemoryStream();
            return entry;
        }

        public static void WritePutRecordsRequestEntry(this BinaryWriter writer, KM.PutRecordsRequestEntry entry)
        {
            writer.WriteNullableString(entry.PartitionKey);
            writer.WriteNullableString(entry.ExplicitHashKey);
            writer.WriteMemoryStream(entry.Data);
        }

        private static BinarySerializer<List<Envelope<KM.PutRecordsRequestEntry>>> _putRecordsRequestEntryListBinarySerializer;

        public static BinarySerializer<List<Envelope<KM.PutRecordsRequestEntry>>> PutRecordsRequestEntryListBinarySerializer
        {
            get
            {
                if (_putRecordsRequestEntryListBinarySerializer == null)
                {
                    var envelopeSerializer = new EnvelopeSerializer<KM.PutRecordsRequestEntry>(
                        WritePutRecordsRequestEntry,
                        ReadPutRecordsRequestEntry);

                    var listWriter = new ListBinarySerializer<Envelope<KM.PutRecordsRequestEntry>>(
                        envelopeSerializer.Serialize,
                        envelopeSerializer.Deserialize);

                    _putRecordsRequestEntryListBinarySerializer = new BinarySerializer<List<Envelope<KM.PutRecordsRequestEntry>>>(
                        listWriter.Serialize,
                        listWriter.Deserialize
                        );
                }
                return _putRecordsRequestEntryListBinarySerializer;
            }
        }
        #endregion

        #region S3
        public static string ReadString(this BinaryReader reader)
        {
            return reader.ReadNullableString();
        }

        public static void WriteString(this BinaryWriter writer, string data)
        {
            writer.WriteNullableString(data);

        }

        private static BinarySerializer<List<Envelope<string>>> _stringListBinarySerializer;

        public static BinarySerializer<List<Envelope<string>>> StringListBinarySerializer
        {
            get
            {
                if (_stringListBinarySerializer == null)
                {
                    var envelopeSerializer = new EnvelopeSerializer<string>(
                        WriteString,
                        ReadString);

                    var listWriter = new ListBinarySerializer<Envelope<string>>(
                        envelopeSerializer.Serialize,
                        envelopeSerializer.Deserialize);

                    _stringListBinarySerializer = new BinarySerializer<List<Envelope<string>>>(
                        listWriter.Serialize,
                        listWriter.Deserialize
                        );
                }
                return _stringListBinarySerializer;
            }
        }
        #endregion
    }
}
