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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// This abstract class provides batching behavior.
    /// It accepts individual event wrapped in envelopes.
    /// It then converts envelope to TRecord can be consumed by the downstream. Subclass override ConvertEnvelopToRecord to handle the conversion.
    /// It then batch TRecords to List<typeparamref name="TRecord"/> on time, record count and bytes of records, whichever occurs first.
    /// SubClass overrides GetRecordSize to calculate the size.
    /// Subclass overrides OnNextBatch to flush the batch.
    /// </summary>
    /// <typeparam name="TRecord"></typeparam>
    public abstract class BatchEventSink<TRecord> : EventSink
    {
        protected int _interval;
        protected int _count;

        protected long _queueSize;
        protected long _maxBatchSize;

        protected Batch<Envelope<TRecord>> _batch;
        protected Buffer<List<Envelope<TRecord>>> _buffer;

        protected CancellationToken _stopToken;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="defaultInterval">Number of seconds</param>
        /// <param name="defaultRecordCount">Record count of the buffer.</param>
        public BatchEventSink(
            IPlugInContext context,
            int defaultInterval,
            int defaultRecordCount,
            long maxBatchSize
        ) : base(context)
        {
            int.TryParse(_config[ConfigConstants.BUFFER_INTERVAL], out _interval);
            if (_interval == 0) _interval = defaultInterval;
            int.TryParse(_config[ConfigConstants.BUFFER_SIZE], out _count);
            if (_count == 0) _count = defaultRecordCount;
            _maxBatchSize = maxBatchSize;

            string queueType = _config[ConfigConstants.QUEUE_TYPE];
            int.TryParse(_config[ConfigConstants.QUEUE_MAX_BATCHES], out int maxBatches);
            ISimpleQueue<List<Envelope<TRecord>>> lowerPriorityQueue;
            if (!string.IsNullOrWhiteSpace(queueType) && queueType.Equals(ConfigConstants.QUEUE_TYPE_FILE, StringComparison.OrdinalIgnoreCase))
            {
                if (maxBatches == 0) maxBatches = 10000;
                string queuePath = _config[ConfigConstants.QUEUE_PATH];
                if (string.IsNullOrWhiteSpace(queuePath))
                    queuePath = Path.Combine(Utility.GetSessionQueuesDirectory(_context.SessionName), Id);
                lowerPriorityQueue = new FilePersistentQueue<List<Envelope<TRecord>>>(maxBatches, queuePath, GetSerializer(), _logger);
            }
            else //in memory
            {
                if (maxBatches == 0) maxBatches = 100;
                lowerPriorityQueue = new InMemoryQueue<List<Envelope<TRecord>>>(maxBatches);
            }

            if (!int.TryParse(_config["MaxInMemoryCacheSize"], out int inMemoryCacheSize) || inMemoryCacheSize < 1)
                inMemoryCacheSize = 10;
            if (inMemoryCacheSize > 100)
                throw new ConfigurationException("In-memory cache size cannot exceed 100 batches (500MB).");

            bool.TryParse(_config["PersistWhenCacheFull"], out bool persistWhenCacheFull);
            if (persistWhenCacheFull)
            {
                _logger?.LogDebug("Creating BatchEventSink with HighCapacityBuffer and max in-memory cache size: {0}", inMemoryCacheSize);
                _buffer = new HighCapacityBuffer<List<Envelope<TRecord>>>(inMemoryCacheSize, _context.Logger, OnNextBatch, lowerPriorityQueue);
            }
            else
            {
                _logger?.LogDebug("Creating BatchEventSink with HiLowBuffer and max in-memory cache size: {0}", inMemoryCacheSize);
                _buffer = new HiLowBuffer<List<Envelope<TRecord>>>(inMemoryCacheSize, _context.Logger, OnNextBatch, lowerPriorityQueue);
            }

            _batch = new Batch<Envelope<TRecord>>(TimeSpan.FromSeconds(_interval),
                    new long[] { _count, _maxBatchSize },
                    new Func<Envelope<TRecord>, long>[]
                    {
                        r => 1,
                        GetRecordSize
                    },
                    SendBatch
                );
        }

        public override void OnNext(IEnvelope value)
        {
            if (value == null) return;
            try
            {
                TRecord record = CreateRecord(value);
                if (record != null)
                {
                    _batch.Add(new Envelope<TRecord>(record, value.Timestamp, value.BookmarkData, value.Position));
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Batching error");
            }
            finally
            {
                value.Dispose();
            }
        }

        public override ValueTask StartAsync(CancellationToken stopToken)
        {
            // remember the token here so that all operations know when to exit
            _stopToken = stopToken;
            return base.StartAsync(stopToken);
        }

        public override void Stop()
        {
            _batch.Stop();
        }

        protected void SendBatch(List<Envelope<TRecord>> records, long[] metrics, FlushReason reason)
        {
            if (_stopToken.IsCancellationRequested)
            {
                return;
            }

            _logger?.LogDebug("Sink {0} sending {1} records {2} bytes for reason {3}.", Id, metrics[0], metrics[1], reason);
            if (_buffer == null)
            {
                _logger?.LogTrace("Sending new batch of {0} records directly to sink", records.Count);
                OnNextBatch(records);
            }
            else
            {
                _logger?.LogTrace("Adding new batch of {0} records to buffer", records.Count);
                _buffer.Add(records);
            }
        }

        protected abstract ISerializer<List<Envelope<TRecord>>> GetSerializer();

        protected abstract void OnNextBatch(List<Envelope<TRecord>> records);

        //Can throw if record too long
        protected abstract long GetRecordSize(Envelope<TRecord> record);

        //Can throw if record cannot be created
        protected abstract TRecord CreateRecord(IEnvelope value);
    }
}
