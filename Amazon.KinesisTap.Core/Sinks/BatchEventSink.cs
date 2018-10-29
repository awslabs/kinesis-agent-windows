using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Threading;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// This abstract class provides batching behavior.
    /// It accepts indivudal event wrapped in envelopes.
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
            if (!string.IsNullOrWhiteSpace(queueType) && queueType.Equals(ConfigConstants.QUEUE_TYPE_FILE, StringComparison.CurrentCultureIgnoreCase))
            {
                if (maxBatches == 0) maxBatches = 10000;
                string queuePath = _config[ConfigConstants.QUEUE_PATH];
                if (string.IsNullOrWhiteSpace(queuePath))
                {
                    queuePath = Path.Combine(Utility.GetKinesisTapProgramDataPath(), ConfigConstants.QUEUE, this.Id);
                }
                lowerPriorityQueue = new FilePersistentQueue<List<Envelope<TRecord>>>(maxBatches, queuePath, this.GetSerializer());
            }
            else //in memory
            {
                if (maxBatches == 0) maxBatches = 100;
                lowerPriorityQueue = new InMemoryQueue<List<Envelope<TRecord>>>(maxBatches);
            }
            _buffer = new HiLowBuffer<List<Envelope<TRecord>>>(1, OnNextBatch, lowerPriorityQueue);
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
                    _batch.Add(new Envelope<TRecord>(record, value.Timestamp));
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex.Message);
            }
        }

        public override void Start()
        {
        }

        public override void Stop()
        {
            _batch.Stop();
        }

        protected void SendBatch(List<Envelope<TRecord>> records, long[] metrics, FlushReason reason)
        {
            _logger?.LogDebug($"Sink {this.Id} sending {metrics[0]} records {metrics[1]} bytes for reason {reason}.");
            if (_buffer == null)
            {
                OnNextBatch(records);
            }
            else
            {
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
