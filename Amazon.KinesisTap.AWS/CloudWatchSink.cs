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
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Timers;
using Amazon.CloudWatch;
using Amazon.CloudWatch.Model;
using Amazon.KinesisTap.AWS.Failover;
using Amazon.KinesisTap.AWS.Failover.Strategy;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Amazon.Runtime.Internal;
using Amazon.Util;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.AWS
{
    public class CloudWatchSink : AWSMetricsSink<PutMetricDataRequest, MetricValue>, IEventSink<List<MetricDatum>>, IFailoverSink<AmazonCloudWatchClient>, IDisposable
    {
        protected virtual IAmazonCloudWatch CloudWatchClient { get; set; }

        /// <summary>
        /// Adaptive throttle for client.
        /// </summary>
        protected readonly Throttle _throttle;

        /// <summary>
        /// Maximum wait interval between failback retry.
        /// </summary>
        protected readonly int _maxFailbackRetryIntervalInMinutes;

        /// <summary>
        /// Primary Region Failback Timer.
        /// </summary>
        protected readonly Timer _primaryRegionFailbackTimer;

        /// <summary>
        /// Failover Sink.
        /// </summary>
        protected readonly FailoverSink<AmazonCloudWatchClient> _failoverSink;

        /// <summary>
        /// Sink Regional Strategy.
        /// </summary>
        protected readonly FailoverStrategy<AmazonCloudWatchClient> _failoverSinkRegionStrategy;

        private readonly bool _failoverSinkEnabled = false;

        private readonly string _namespace;

        private readonly Dimension[] _dimensions;
        private readonly int _storageResolution;
        private readonly DefaultRetryPolicy _defaultRetryPolicy;

        private static Dimension[] _defaultDimensions;

        private static Dimension[] DefaultDimensions
        {
            get
            {
                if (_defaultDimensions == null)
                {
                    List<Dimension> dimensions = new List<Dimension>()
                    {
                        new Dimension() { Name = "ComputerName", Value = Utility.ComputerName }
                    };
                    if (!string.IsNullOrEmpty(EC2InstanceMetadata.InstanceId))
                    {
                        dimensions.Add(new Dimension() { Name = "InstanceID", Value = EC2InstanceMetadata.InstanceId });
                    }
                    _defaultDimensions = dimensions.ToArray();
                }
                return _defaultDimensions;
            }
        }

        private static readonly IDictionary<MetricUnit, StandardUnit> _unitMap;

        private const int ATTEMPT_LIMIT = 1;
        private const int FLUSH_QUEUE_DELAY = 100; //Throttle at about 10 TPS

        public CloudWatchSink(
            int defaultInterval,
            IPlugInContext context
            ) : base(defaultInterval, context)
        {
            //StorageResolution is used to specify standard or high-resolution metrics. Valid values are 1 and 60
            //It is different to interval.
            //See https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_MetricDatum.html for full details
            _storageResolution = base._interval < 60 ? 1 : 60;

            string dimensionsConfig = null;
            if (_config != null)
            {
                dimensionsConfig = _config["dimensions"];
                _namespace = _config["namespace"];
            }
            if (!string.IsNullOrEmpty(dimensionsConfig))
            {
                List<Dimension> dimensions = new List<Dimension>();
                string[] dimensionPairs = dimensionsConfig.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
                foreach (var dimensionPair in dimensionPairs)
                {
                    string[] keyValue = dimensionPair.Split('=');
                    string value = ResolveVariables(keyValue[1]);
                    dimensions.Add(new Dimension() { Name = keyValue[0], Value = value });
                }
                _dimensions = dimensions.ToArray();
            }
            else
            {
                _dimensions = DefaultDimensions;
            }

            if (string.IsNullOrEmpty(_namespace))
            {
                _namespace = "KinesisTap";
            }
            else
            {
                _namespace = ResolveVariables(_namespace);
            }

            // Set throttle at 150 requests per second
            // https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_limits.html
            _throttle = new Throttle(new Core.TokenBucket(1, 150));
        }

        public CloudWatchSink(
            int defaultInterval,
            IPlugInContext context,
            IAmazonCloudWatch cloudWatchClient
            ) : this(defaultInterval, context)
        {
            // Setup Client
            CloudWatchClient = cloudWatchClient;

            // Setup Default Retry Policy
            _defaultRetryPolicy = new DefaultRetryPolicy(CloudWatchClient.Config);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CloudWatchSink"/> class.
        /// </summary>
        /// <param name="defaultInterval">Number of seconds as interval before next upload.</param>
        /// <param name="context">The <see cref="IPlugInContext"/> that contains configuration info, logger, metrics etc.</param>
        /// <param name="failoverSink">The <see cref="FailoverSink{AmazonCloudWatchClient}"/> that defines failover sink class.</param>
        /// <param name="failoverSinkRegionStrategy">The <see cref="FailoverStrategy{AmazonCloudWatchClient}"/> that defines failover sink region selection strategy.</param>
        public CloudWatchSink(
            int defaultInterval,
            IPlugInContext context,
            FailoverSink<AmazonCloudWatchClient> failoverSink,
            FailoverStrategy<AmazonCloudWatchClient> failoverSinkRegionStrategy)
            : this(defaultInterval, context, failoverSinkRegionStrategy.GetPrimaryRegionClient()) // Setup CloudWatch Client with Primary Region
        {
            // Parse or default
            // Max wait interval between failback retry
            if (!int.TryParse(_config[ConfigConstants.MAX_FAILBACK_RETRY_INTERVAL_IN_MINUTES], out _maxFailbackRetryIntervalInMinutes))
            {
                _maxFailbackRetryIntervalInMinutes = ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_RETRY_IN_MINUTES;
            }
            else if (_maxFailbackRetryIntervalInMinutes < ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_RETRY_IN_MINUTES)
            {
                throw new ArgumentException(String.Format("Invalid \"{0}\" value, please provide positive integer greator than \"{1}\".",
                    ConfigConstants.MAX_FAILBACK_RETRY_INTERVAL_IN_MINUTES, ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_RETRY_IN_MINUTES));
            }

            // Failover Sink
            _failoverSink = failoverSink;
            _failoverSinkEnabled = true;
            // Failover Sink Region Strategy
            _failoverSinkRegionStrategy = failoverSinkRegionStrategy;

            // Setup Primary Region Failback Timer
            _primaryRegionFailbackTimer = new System.Timers.Timer(_maxFailbackRetryIntervalInMinutes * 60 * 1000);
            _primaryRegionFailbackTimer.Elapsed += new ElapsedEventHandler(FailbackToPrimaryRegion);
            _primaryRegionFailbackTimer.AutoReset = true;
            _primaryRegionFailbackTimer.Start();
        }

        #region public methods
        /// <inheritdoc/>
        public void Dispose()
        {
            _primaryRegionFailbackTimer.Stop();
        }

        static CloudWatchSink()
        {
            _unitMap = new Dictionary<MetricUnit, StandardUnit>();

            foreach (MetricUnit key in Enum.GetValues(typeof(MetricUnit)))
            {
                var standardUnitField = typeof(StandardUnit)
                    .GetTypeInfo()
                    .GetDeclaredField(key.ToString());
                if (standardUnitField != null)
                {
                    StandardUnit cloudWatchUnit = (StandardUnit)standardUnitField
                        .GetValue(null);
                    _unitMap.Add(key, cloudWatchUnit);
                }
            }
        }

        public override void Start()
        {
            base.Start();
            _metrics?.InitializeCounters(Id, MetricsConstants.CATEGORY_SINK, CounterTypeEnum.Increment,
                new Dictionary<string, MetricValue>()
            {
                { MetricsConstants.CLOUDWATCH_PREFIX + MetricsConstants.NONRECOVERABLE_SERVICE_ERRORS, MetricValue.ZeroCount },
                { MetricsConstants.CLOUDWATCH_PREFIX + MetricsConstants.RECOVERABLE_SERVICE_ERRORS, MetricValue.ZeroCount },
                { MetricsConstants.CLOUDWATCH_PREFIX + MetricsConstants.SERVICE_SUCCESS, MetricValue.ZeroCount }
            });
        }

        public void OnNext(IEnvelope<List<MetricDatum>> datums)
        {
            List<MetricDatum> records = new List<MetricDatum>();
            foreach (MetricDatum datum in datums.Data)
            {
                // Append the default dimensions if datum doesn't have dimensions
                if (datum.Dimensions == null || datum.Dimensions.Count == 0)
                {
                    datum.Dimensions = _dimensions.ToList();
                }
                records.Add(datum);
            }

            // Send Metric datums request
            PutMetricDataAsync(records).Wait();
        }

        /// <inheritdoc/>
        public AmazonCloudWatchClient FailbackToPrimaryRegion(Throttle throttle)
        {
            var _cloudWatchClient = _failoverSink.FailbackToPrimaryRegion(_throttle);
            if (_cloudWatchClient is not null)
            {
                // Jittered Delay
                var delay = _throttle.GetDelayMilliseconds(1);
                if (delay > 0)
                {
                    Task.Delay((int)(delay * (1.0d + Utility.Random.NextDouble() * ConfigConstants.DEFAULT_JITTING_FACTOR))).Wait();
                }
                // Dispose
                CloudWatchClient.Dispose();
                // Override client
                CloudWatchClient = _cloudWatchClient;
            }
            return null;
        }

        /// <inheritdoc/>
        public AmazonCloudWatchClient FailOverToSecondaryRegion(Throttle throttle)
        {
            var _cloudWatchClient = _failoverSink.FailOverToSecondaryRegion(_throttle);
            if (_cloudWatchClient is not null)
            {
                // Jittered Delay
                var delay = _throttle.GetDelayMilliseconds(1);
                if (delay > 0)
                {
                    Task.Delay((int)(delay * (1.0d + Utility.Random.NextDouble() * ConfigConstants.DEFAULT_JITTING_FACTOR))).Wait();
                }
                // Dispose
                CloudWatchClient.Dispose();
                // Override client
                CloudWatchClient = _cloudWatchClient;
            }
            return null;
        }

        /// <summary>
        /// Check service health.
        /// </summary>
        /// <param name="client">Instance of <see cref="AmazonCloudWatchClient"/> class.</param>
        /// <returns>Success, RountTripTime.</returns>
        public static async Task<(bool, double)> CheckServiceReachable(AmazonCloudWatchClient client)
        {
            var stopwatch = new Stopwatch();
            try
            {
                stopwatch.Start();
                await client.DescribeAlarmsAsync(new DescribeAlarmsRequest
                {
                    AlarmNamePrefix = "KinesisTap"
                });
                stopwatch.Stop();
            }
            catch (AmazonCloudWatchException)
            {
                stopwatch.Stop();
                // Any exception is fine, we are currently only looking to
                // check if the service is reachable and what is the RTT.
                return (true, stopwatch.ElapsedMilliseconds);
            }
            catch (Exception)
            {
                stopwatch.Stop();
                return (false, stopwatch.ElapsedMilliseconds);
            }

            return (true, stopwatch.ElapsedMilliseconds);
        }
        #endregion

        #region protected methods
        protected override int AttemptLimit => ATTEMPT_LIMIT;

        protected override int FlushQueueDelay => FLUSH_QUEUE_DELAY;

        protected override void OnFlush(IDictionary<MetricKey, MetricValue> accumlatedValues, IDictionary<MetricKey, MetricValue> lastValues)
        {
            QueryDataSources(accumlatedValues);

            List<MetricDatum> datums = new List<MetricDatum>();
            if (string.IsNullOrWhiteSpace(_metricsFilter))
            {
                PrepareMetricDatums(accumlatedValues, datums);
                PrepareMetricDatums(lastValues, datums);
            }
            else
            {
                FilterValues(accumlatedValues, lastValues, datums);
            }
            PutMetricDataAsync(datums).Wait();
            PublishMetrics(MetricsConstants.CLOUDWATCH_PREFIX);
            Task.Run(FlushQueueAsync)
                .ContinueWith(t =>
                {
                    if (t.IsFaulted && t.Exception is AggregateException aex)
                    {
                        aex.Handle(ex =>
                        {
                            _logger?.LogError($"FlushQueueAsync Exception {ex}");
                            return true;
                        });
                    }
                });
        }

        protected override string EvaluateVariable(string value)
        {
            string evaluated = base.EvaluateVariable(value);
            try
            {
                return AWSUtilities.EvaluateAWSVariable(evaluated);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.ToMinimized());
                throw;
            }
        }

        /// <inheritdoc/>
        protected override async Task<PutMetricDataResponse> SendRequestAsync(PutMetricDataRequest putMetricDataRequest)
        {
            // Failover
            if (_failoverSinkEnabled)
            {
                // Failover to Secondary Region
                _ = FailOverToSecondaryRegion(_throttle);
            }

            PutMetricDataResponse response;
            try
            {
                response = await CloudWatchClient.PutMetricDataAsync(putMetricDataRequest);
                _throttle.SetSuccess();
            }
            catch (Exception)
            {
                _throttle.SetError();
                throw;
            }

            return response;
        }

        protected override bool IsRecoverable(Exception ex)
        {
            return _defaultRetryPolicy.RetryForException(null, ex);
        }
        #endregion

        #region private methods
        private void FilterValues(IDictionary<MetricKey, MetricValue> accumlatedValues, IDictionary<MetricKey, MetricValue> lastValues, List<MetricDatum> datums)
        {
            var filteredAccumulatedValues = FilterValues(accumlatedValues);
            PrepareMetricDatums(filteredAccumulatedValues, datums);
            var filteredLastValues = FilterValues(lastValues);
            PrepareMetricDatums(filteredLastValues, datums);
            if (_aggregatedMetricsFilters.Count > 0)
            {
                var filteredAggregatedAccumulatedValues =
                    FilterAndAggregateValues(accumlatedValues,
                        values => new MetricValue(values.Sum(v => v.Value), values.First().Unit));
                PrepareMetricDatums(filteredAggregatedAccumulatedValues, datums);
                var filteredAggregatedLastValues = FilterAndAggregateValues(lastValues,
                    values => new MetricValue((long)values.Average(v => v.Value), values.First().Unit));
                PrepareMetricDatums(filteredAggregatedLastValues, datums);
            }
        }

        private void PublishMetrics(string prefix)
        {
            _metrics?.PublishCounters(Id, MetricsConstants.CATEGORY_SINK, CounterTypeEnum.Increment, new Dictionary<string, MetricValue>()
            {
                { prefix + MetricsConstants.SERVICE_SUCCESS, new MetricValue(_serviceSuccess) },
                { prefix + MetricsConstants.RECOVERABLE_SERVICE_ERRORS, new MetricValue(_recoverableServiceErrors) },
                { prefix + MetricsConstants.NONRECOVERABLE_SERVICE_ERRORS, new MetricValue(_nonrecoverableServiceErrors) }
            });
            _metrics?.PublishCounter(Id, MetricsConstants.CATEGORY_SINK, CounterTypeEnum.CurrentValue, prefix + MetricsConstants.LATENCY, _latency, MetricUnit.Milliseconds);
            ResetIncrementalCounters();
        }

        private void ResetIncrementalCounters()
        {
            _serviceSuccess = 0;
            _recoverableServiceErrors = 0;
            _nonrecoverableServiceErrors = 0;
        }

        private async Task PutMetricDataAsync(List<MetricDatum> datums)
        {
            _logger?.LogDebug($"CloudWatchSink {Id} sending a total of {datums.Count} datums.");
            //cloudwatch can only handle 20 datums at a time
            foreach (var subDatums in datums.Chunk(20))
            {
                var metricsToSend = subDatums as List<MetricDatum>;
                if (metricsToSend == null)
                {
                    _logger?.LogError($"CloudWatchSink {Id} trying to send a chunk with null datums");
                }
                else
                {
                    _logger?.LogDebug($"CloudWatchSink {Id} trying to send a chunk with {metricsToSend.Count} datums.");
                }
                var putMetricDataRequest = new PutMetricDataRequest()
                {
                    Namespace = _namespace,
                    MetricData = metricsToSend
                };
                await PutMetricDataAsync(putMetricDataRequest);
            }
        }

        private void PrepareMetricDatums(IDictionary<MetricKey, MetricValue> metrics, List<MetricDatum> datums)
        {
            foreach (var metric in metrics)
            {
                datums.Add(new MetricDatum()
                {
                    Dimensions = GetDimensions(metric.Key.Id, metric.Key.Category),
                    Value = metric.Value.Value,
                    MetricName = metric.Key.Name,
                    TimestampUtc = DateTime.UtcNow,
                    StorageResolution = _storageResolution,
                    Unit = _unitMap[metric.Value.Unit]
                });
            }
        }

        private List<Dimension> GetDimensions(string id, string category)
        {
            List<Dimension> dimensions = new List<Dimension>(_dimensions);
            if (!string.IsNullOrEmpty(id))
            {
                dimensions.Add(new Dimension() { Name = "Id", Value = id });
            }
            dimensions.Add(new Dimension() { Name = "Category", Value = category });
            return dimensions;
        }

        private void QueryDataSources(IDictionary<MetricKey, MetricValue> accumlatedValues)
        {
            foreach (var dataSource in _dataSources?.Values)
            {
                var resultEnvelope = dataSource.Query(null);
                if (resultEnvelope != null)
                {
                    var metrics = resultEnvelope.Data as ICollection<KeyValuePair<MetricKey, MetricValue>>;
                    foreach (var metric in metrics)
                    {
                        accumlatedValues[metric.Key] = metric.Value;
                    }
                }
            }
        }

        private void FailbackToPrimaryRegion(Object source, ElapsedEventArgs e)
        {
            // Failover
            if (_failoverSinkEnabled)
            {
                _ = FailbackToPrimaryRegion(_throttle);
            }
        }
        #endregion
    }
}
