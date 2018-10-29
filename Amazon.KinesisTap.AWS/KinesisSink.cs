using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.AWS
{
    public abstract class KinesisSink<TRecord> : AWSBufferedEventSink<TRecord>
    {
        protected int _maxRecordsPerSecond;
        protected long _maxBytesPerSecond;
        protected Throttle _throttle;

        public KinesisSink(
          IPlugInContext context,
          int defaultInterval,
          int defaultRecordCount,
          long maxBatchSize
        ) : base(context, defaultInterval, defaultRecordCount, maxBatchSize)
        {

        }

        protected override long GetDelayMilliseconds(int recordCount, long batchBytes)
        {
            long timeToWait = _throttle.GetDelayMilliseconds(new long[] { recordCount, batchBytes });
            return timeToWait;
        }
    }
}
