using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

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
                    var envelopeSerializer = new EnvelopeSerializer<KM.PutRecordsRequestEntry> (
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
    }
}
