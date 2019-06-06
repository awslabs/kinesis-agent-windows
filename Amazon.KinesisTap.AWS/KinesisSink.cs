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
using System.Text;

using Amazon.KinesisTap.Core;

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
            long timeToWait = _throttle.GetDelayMilliseconds(new long[] { 1, recordCount, batchBytes }); //The 1st element indicates 1 API call.
            return timeToWait;
        }
    }
}
