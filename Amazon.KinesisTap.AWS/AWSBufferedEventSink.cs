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
using System.Reactive.Linq;
using System.Threading.Tasks;
using System.Reactive.Threading.Tasks;
using System.Diagnostics;
using System.Linq;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.AWS
{
    public abstract class AWSBufferedEventSink<TRecord> : BatchEventSink<TRecord>
    {
        protected int _maxAttempts;
        protected double _jittingFactor;
        protected double _backoffFactor;
        protected double _recoveryFactor;
        protected double _minRateAdjustmentFactor;

        //metrics
        protected long _recoverableServiceErrors;
        protected long _nonrecoverableServiceErrors;
        protected long _recordsAttempted;
        protected long _bytesAttempted;
        protected long _recordsSuccess;
        protected long _recordsFailedRecoverable;
        protected long _recordsFailedNonrecoverable;
        protected long _latency;
        protected long _clientLatency;

        public AWSBufferedEventSink(
            IPlugInContext context,
            int defaultInterval, 
            int defaultRecordCount,
            long maxBatchSize
        ) : base(context, defaultInterval, defaultRecordCount, maxBatchSize)
        {
            if (!int.TryParse(_config["MaxAttempts"], out _maxAttempts))
            {
                _maxAttempts = ConfigConstants.DEFAULT_MAX_ATTEMPTS;
            }

            if (!double.TryParse(_config["JittingFactor"], out _jittingFactor))
            {
                _jittingFactor = ConfigConstants.DEFAULT_JITTING_FACTOR;
            }

            if (!double.TryParse(_config["BackoffFactor"], out _backoffFactor))
            {
                _backoffFactor = ConfigConstants.DEFAULT_BACKOFF_FACTOR;
            }

            if (!double.TryParse(_config["RecoveryFactor"], out _recoveryFactor))
            {
                _recoveryFactor = ConfigConstants.DEFAULT_RECOVERY_FACTOR;
            }

            if (!double.TryParse(_config["MinRateAdjustmentFactor"], out _minRateAdjustmentFactor))
            {
                _minRateAdjustmentFactor = ConfigConstants.DEFAULT_MIN_RATE_ADJUSTMENT_FACTOR;
            }
        }

        protected override void OnNextBatch(List<Envelope<TRecord>> records)
        {
            if (records?.Count > 0)
            {
                ThrottledOnNextAsync(records).Wait();
            }
        }

        protected virtual async Task ThrottledOnNextAsync(List<Envelope<TRecord>> records)
        {
            int recordCount = records.Count;
            long batchBytes = records.Select(r => GetRecordSize(r)).Sum();
            long delay = GetDelayMilliseconds(recordCount, batchBytes);
            if (delay > 0)
            {
                await Task.Delay((int)(delay * (1.0d 
                    + Utility.Random.NextDouble() * _jittingFactor))) ;
            }
            await OnNextAsync(records, batchBytes);
        }

        protected abstract long GetDelayMilliseconds(int recordCount, long batchBytes);

        protected abstract Task OnNextAsync(List<Envelope<TRecord>> records, long batchBytes);

        protected abstract TRecord CreateRecord(string record, IEnvelope envelope);

        protected override string EvaluateVariable(string value)
        {
            string evaluated = base.EvaluateVariable(value);
            if (string.IsNullOrEmpty(evaluated)) return evaluated;
            try
            {
                return AWSUtilities.EvaluateAWSVariable(evaluated);
            }
            catch(Exception ex)
            {
                _logger.LogError(ex.Message);
                throw;
            }
        }

        protected void ResetIncrementalCounters()
        {
            _recoverableServiceErrors = 0;
            _nonrecoverableServiceErrors = 0;
            _recordsAttempted = 0;
            _bytesAttempted = 0;
            _recordsSuccess = 0;
            _recordsFailedRecoverable = 0;
            _recordsFailedNonrecoverable = 0;
        }

        protected void PublishMetrics(string prefix)
        {
            _metrics?.PublishCounters(this.Id, MetricsConstants.CATEGORY_SINK, CounterTypeEnum.Increment, new Dictionary<string, MetricValue>()
            {
                { prefix + MetricsConstants.BYTES_ATTEMPTED, new MetricValue(_bytesAttempted, MetricUnit.Bytes) },
                { prefix + MetricsConstants.RECORDS_ATTEMPTED, new MetricValue(_recordsAttempted) },
                { prefix + MetricsConstants.RECORDS_FAILED_NONRECOVERABLE, new MetricValue(_recordsFailedNonrecoverable) },
                { prefix + MetricsConstants.RECORDS_FAILED_RECOVERABLE, new MetricValue(_recordsFailedRecoverable) },
                { prefix + MetricsConstants.RECORDS_SUCCESS, new MetricValue(_recordsSuccess) },
                { prefix + MetricsConstants.RECOVERABLE_SERVICE_ERRORS, new MetricValue(_recoverableServiceErrors) },
                { prefix + MetricsConstants.NONRECOVERABLE_SERVICE_ERRORS, new MetricValue(_nonrecoverableServiceErrors) }
            });

            _metrics?.PublishCounters(this.Id, MetricsConstants.CATEGORY_SINK, CounterTypeEnum.CurrentValue, new Dictionary<string, MetricValue>()
            {
                { prefix + MetricsConstants.LATENCY, new MetricValue(_latency, MetricUnit.Milliseconds) },
                { prefix + MetricsConstants.CLIENT_LATENCY, new MetricValue(_clientLatency, MetricUnit.Milliseconds) }
            });
            ResetIncrementalCounters();
        }

        protected override TRecord CreateRecord(IEnvelope envelope)
        {
            string record = base.GetRecord(envelope);
            if (string.IsNullOrEmpty(record))
            {
                return default(TRecord);
            }
            else
            {
                return CreateRecord(record, envelope);
            }
        }
    }
}
