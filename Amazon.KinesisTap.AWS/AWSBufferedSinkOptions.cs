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
using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.AWS
{
    /// <summary>
    /// Common options for AWS sinks
    /// </summary>
    public class AWSBufferedSinkOptions
    {
        public int BufferIntervalMs { get; set; } = 1000;

        public string Format { get; set; }

        public int QueueSizeItems { get; set; }

        public int MaxBatchBytes { get; set; }

        public int MaxBatchSize { get; set; }

        public string TextDecoration { get; set; }

        public string TextDecorationEx { get; set; }

        public string ObjectDecoration { get; set; }

        public string ObjectDecorationEx { get; set; }

        public string SecondaryQueueType { get; set; }

        public int QueueMaxBatches { get; set; } = 10000;

        public int MaxAttempts { get; set; } = ConfigConstants.DEFAULT_MAX_ATTEMPTS;

        public double JittingFactor { get; set; } = ConfigConstants.DEFAULT_JITTING_FACTOR;

        public double BackoffFactor { get; set; } = ConfigConstants.DEFAULT_BACKOFF_FACTOR;

        public double RecoveryFactor { get; set; } = ConfigConstants.DEFAULT_RECOVERY_FACTOR;

        public double MinRateAdjustmentFactor { get; set; } = ConfigConstants.DEFAULT_MIN_RATE_ADJUSTMENT_FACTOR;

        public int UploadNetworkPriority { get; set; } = ConfigConstants.DEFAULT_NETWORK_PRIORITY;
    }
}
