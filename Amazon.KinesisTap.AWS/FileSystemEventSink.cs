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
using System.Threading;
using System.Threading.Tasks;
using Amazon.CloudWatchLogs.Model;
using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.AWS
{
    /// <summary>
    /// An implementation of the <see cref="AWSBufferedEventSink{TRecord}"/> that writes logs to the File System.
    /// By implementing the buffered sink, we're able to replicate a throttle event by creating a write lock on the target file.
    /// </summary>
    public class FileSystemEventSink : AWSBufferedEventSink<InputLogEvent>
    {
        private readonly SemaphoreSlim _fileLock = new SemaphoreSlim(1, 1);
        private bool _isStopped;

        public FileSystemEventSink(IPlugInContext context)
            : this(
                  context,
                  int.TryParse(context.Configuration[ConfigConstants.REQUESTS_PER_SECOND], out var rps) ? rps : 1000,
                  int.TryParse(context.Configuration[ConfigConstants.INTERVAL], out var interval) ? interval : 1,
                  int.TryParse(context.Configuration[ConfigConstants.RECORD_COUNT], out var recordCount) ? recordCount : 100,
                  long.TryParse(context.Configuration[ConfigConstants.MAX_BATCH_SIZE], out var maxBatchSize) ? maxBatchSize : 1024 * 1024)
        {
        }

        public FileSystemEventSink(IPlugInContext context, int requestRate, int defaultInterval, int defaultRecordCount, long maxBatchSize)
            : base(context, defaultInterval, defaultRecordCount, maxBatchSize)
        {
            // If the user hasn't specified a FilePath, generate one based on the Id of the sink, or the name if it doesn't have a value.
            FilePath = _context.Configuration["FilePath"] ?? Path.Combine(Path.GetTempPath(), (Id ?? nameof(FileSystemEventSink)) + ".txt");

            Throttle = new AdaptiveThrottle(
                new TokenBucket(1, requestRate),
                _backoffFactor,
                _recoveryFactor,
                _minRateAdjustmentFactor);
        }

        public Throttle Throttle { get; private set; }

        public string FilePath { get; private set; }

        /// <inheritdoc />
        public override void Start()
        {
            Start(true);
        }

        public void Start(bool deleteExisting)
        {
            var directory = Path.GetDirectoryName(FilePath);
            if (!Directory.Exists(directory))
                Directory.CreateDirectory(directory);
            else if (deleteExisting && File.Exists(FilePath))
                File.Delete(FilePath);

            _isStopped = false;
        }

        public override void Stop()
        {
            _isStopped = true;
            Throttle.SetSuccess();
            _buffer.Stop();
            base.Stop();
        }

        protected override long GetRecordSize(Envelope<InputLogEvent> record)
        {
            const long CLOUDWATCH_OVERHEAD = 26L;
            const long TWO_FIFTY_SIX_KILOBYTES = 256 * 1024;
            var recordSize = Encoding.UTF8.GetByteCount(record.Data.Message) + CLOUDWATCH_OVERHEAD;
            if (recordSize > TWO_FIFTY_SIX_KILOBYTES)
            {
                _recordsFailedNonrecoverable++;
                throw new ArgumentException("The maximum record size is 256KB. Record discarded.");
            }
            return recordSize;
        }

        protected override InputLogEvent CreateRecord(string record, IEnvelope envelope)
        {
            return new InputLogEvent
            {
                Timestamp = envelope.Timestamp,
                Message = record
            };
        }

        protected override long GetDelayMilliseconds(int recordCount, long batchBytes)
        {
            return Throttle.GetDelayMilliseconds(1);
        }

        protected override ISerializer<List<Envelope<InputLogEvent>>> GetSerializer()
        {
            return AWSSerializationUtility.InputLogEventListBinarySerializer;
        }

        protected override async Task OnNextAsync(List<Envelope<InputLogEvent>> records, long batchBytes)
        {
            // If the sink has been stopped, don't keep trying to write to the file.
            if (_isStopped) return;

            // Obtain the fileLock semaphore to ensure that only a single process can call this method at any given time.
            // This does not lock the file itself; we rely on the OS to manage file locking when we open the file stream.
            await _fileLock.WaitAsync();

            try
            {
                // Create a stream writer on a new FileStream object pointing to the target output file.
                // This method will fail if the OS already has a lock on the file, which will trigger throttling behavior.
                // This allows us to effectively reproduce throttling at the sink level, and also helps debug issues outside of the IDE.
                using (var sw = new StreamWriter(new FileStream(FilePath, FileMode.Append, FileAccess.Write, FileShare.Read)))
                {
                    foreach (var record in records)
                    {
                        await sw.WriteLineAsync(record.Data.Message);
                    }

                    await sw.FlushAsync();
                }

             await   SaveBookmarks(records);
                Throttle.SetSuccess();
            }
            catch (Exception)
            {
                // If the sink has been stopped, don't keep trying to write to the file.
                if (_isStopped) return;

                // This is the magic that reproduces the throttling behavior.
                // We call the SetError method on the throttle object and requeue the events.
                // This uses the same logic as the CloudWatchLogs sink.
                Throttle.SetError();
                _buffer.Requeue(records, Throttle.ConsecutiveErrorCount < _maxAttempts);
            }
            finally
            {
                // Release the semaphore. This doesn't release any locks on the file.
                _fileLock.Release();
            }
        }
    }
}
