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
using System.Threading.Tasks;
using System.Diagnostics;
using System.Threading;

using Microsoft.Extensions.Logging;

using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using System.Linq;

namespace Amazon.KinesisTap.AWS
{
    /// <summary>
    /// Base class for AWS Metrics Sink such as CloudWatch and Telemetrics. Not buffered
    /// </summary>
    public abstract class AWSMetricsSink<TRequest, TResponse, TMetricValue> : SimpleMetricsSink, IDataSink<object>
    {
        private const int RETRY_QUEUE_LIMIT = 1440; //If sending once per minute, allows losing network connectivity for 1 day. Use max 0.7 MB.

        protected long _serviceSuccess;
        protected long _recoverableServiceErrors;
        protected long _nonrecoverableServiceErrors;
        protected long _latency;

        private Queue<TRequest> _requestQueue = new Queue<TRequest>();
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1);

        public AWSMetricsSink(int defaultInterval, IPlugInContext context) : base(defaultInterval, context)
        {
        }

        #region IDataSink support
        protected IDictionary<string, IDataSource<object>> _dataSources =
            new Dictionary<string, IDataSource<object>>();

        public void RegisterDataSource(IDataSource<object> source)
        {
            if (source is IDataSource<ICollection<KeyValuePair<MetricKey, MetricValue>>>)
            {
                _dataSources.Add(source.Id, source);
            }
            else
            {
                throw new InvalidOperationException($"source {source.Id} is incompatible with the metrics sink.");
            }
        }
        #endregion

        #region protected members
        protected abstract int AttemptLimit { get; }

        protected abstract int FlushQueueDelay { get; }

        protected int RetryQueueLimit => RETRY_QUEUE_LIMIT;

        protected abstract bool IsRecoverable(Exception ex);

        protected abstract Task<TResponse> SendRequestAsync(TRequest putMetricDataRequest);

        protected void AddToRetryQueue(TRequest putMetricDataRequest)
        {
            if (_requestQueue.Count >= this.RetryQueueLimit)
            {
                _nonrecoverableServiceErrors++;
                _logger?.LogError($"{this.GetType().Name} AddToRetryQueue: the queue reached the limit. Oldest discarded");
                _requestQueue.Dequeue();
            }
            _requestQueue.Enqueue(putMetricDataRequest);
        }

        protected async Task PutMetricDataAsync(TRequest putMetricDataRequest)
        {
            int attempts = 0;
            while (attempts < this.AttemptLimit)
            {
                attempts++;
                long elapsedMilliseconds = Utility.GetElapsedMilliseconds();
                try
                {
                    var response = await SendRequestAsync(putMetricDataRequest);
                    _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                    _serviceSuccess++;
                    break;
                }
                catch (Exception ex)
                {
                    _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                    if (IsRecoverable(ex))
                    {
                        _recoverableServiceErrors++;
                        //If we get a recoverable error, we will retry up to the limit before putting it in the lower priority queue
                        if (attempts < this.AttemptLimit)
                        {
                            _logger?.LogDebug($"{this.GetType().Name} recoverable exception: {ex.ToMinimized()}");
                            int delay = Utility.Random.Next(_interval * attempts) * 100; //attempts * (_interval * 1000/10)
                            await Task.Delay(delay);
                        }
                        else
                        {
                            _logger?.LogDebug($"{this.GetType().Name} recoverable exception (added to queue): {ex.ToMinimized()}");
                            AddToRetryQueue(putMetricDataRequest);
                            break;
                        }
                    }
                    else
                    {
                        _nonrecoverableServiceErrors++;
                        _logger?.LogError($"{this.GetType().Name} nonrecoverable exception: {ex}");
                        break;
                    }
                }
            }
        }

        protected async Task FlushQueueAsync()
        {
            //Only one thread to flush at a time
            if (_semaphore.Wait(0))
            {
                try
                {
                    while (_requestQueue.Count > 0)
                    {
                        try
                        {
                            var request = _requestQueue.Dequeue();
                            var response = await this.SendRequestAsync(request);
                        }
                        catch (Exception ex)
                        {
                            //Don't process the rest. Wait for next opportunity
                            _logger?.LogDebug($"{this.GetType().Name} FlushQueueAsync exception: {ex.ToMinimized()}");
                            break;
                        }
                        await Task.Delay(this.FlushQueueDelay);
                    }
                }
                catch (Exception ex)
                {
                    _logger?.LogError($"{this.GetType().Name} FlushQueueAsync unknown exception: {ex.ToMinimized()}");
                }
                finally
                {
                    _semaphore.Release();
                }
            }
        }

        protected void AggregateMetrics(IDictionary<MetricKey, MetricValue> metrics, 
            Dictionary<string, object> data,
            Func<IEnumerable<MetricValue>, TMetricValue> aggregator)
        {
            foreach (var group in metrics.GroupBy(
                    kv => kv.Key.Name,
                    (k, g) => new KeyValuePair<string, TMetricValue>(k, aggregator(g.Select(kv => kv.Value)))
                )
            )
            {
                data[group.Key] = group.Value;
            }
        }
        #endregion
    }
}
