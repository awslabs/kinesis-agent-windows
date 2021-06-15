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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Amazon.Util;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.AWS.Telemetrics
{
    /// <summary>
    /// Default implementation of <see cref="ITelemetricSink"/>.
    /// </summary>
    public class TelemetricsSink : AsyncTimingPlugin, IEventSink
    {
        /// <summary>
        /// Store current-value counters.
        /// </summary>
        private readonly ConcurrentDictionary<MetricKey, MetricValue> _currentMetrics = new();

        /// <summary>
        /// Store incremental counters.
        /// </summary>
        private readonly ConcurrentDictionary<MetricKey, MetricValue> _incrementalMetrics = new();

        /// <summary>
        /// Client used to send metrics.
        /// </summary>
        private readonly ITelemetricsClient _telemetricsClient;

        /// <summary>
        /// Initialize <see cref="TelemetricsSink"/>.
        /// </summary>
        /// <param name="id">Sink ID.</param>
        /// <param name="intervalMs">Reporting interval in milliseconds.</param>
        /// <param name="logger">Logger.</param>
        public TelemetricsSink(
            string id,
            int intervalMs,
            ITelemetricsClient telemetricsClient,
            ILogger logger) : base(id, intervalMs, true, logger)
        {
            _telemetricsClient = telemetricsClient;
        }

        public override async ValueTask StartAsync(CancellationToken stopToken)
        {
            await base.StartAsync(stopToken);

            _logger.LogInformation("Started");
        }

        public override async ValueTask StopAsync(CancellationToken stopToken)
        {
            await base.StopAsync(stopToken);
            _logger.LogInformation("Stopped");
        }

        /// <inheritdoc/>
        public void OnCompleted() => _logger.LogInformation("Completed");

        /// <inheritdoc/>
        public void OnError(Exception error) => _logger.LogCritical(error, "Telemetric sink error");

        /// <inheritdoc/>
        public void OnNext(IEnvelope value)
        {
            if (value is not MetricsEnvelope counterData)
            {
                _logger.LogError("Telemetric sink can process MetricsEnvelope.");
                return;
            }

            switch (counterData.CounterType)
            {
                case CounterTypeEnum.CurrentValue:
                    ProcessCurrentValue(counterData);
                    break;
                case CounterTypeEnum.Increment:
                    ProcessIncrementalValue(counterData);
                    break;
                default:
                    throw new NotImplementedException($"{counterData} not implemented.");
            }
        }

        private void ProcessIncrementalValue(MetricsEnvelope counterData)
        {
            foreach (var name in counterData.Data?.Keys)
            {
                var value = counterData.Data[name];
                var key = GetMetrickKey(counterData, name);
                _incrementalMetrics.AddOrUpdate(
                    key: key,
                    addValue: value,
                    updateValueFactory: (k, old) =>
                    {
                        old.Increment(value);
                        return old;
                    });
            }
        }

        private void ProcessCurrentValue(MetricsEnvelope counterData)
        {
            foreach (var name in counterData.Data?.Keys)
            {
                var value = counterData.Data[name];
                var key = GetMetrickKey(counterData, name);
                _currentMetrics[key] = value;
            }
        }

        private static MetricKey GetMetrickKey(MetricsEnvelope counterData, string name)
            => new MetricKey { Name = name, Id = counterData.Id, Category = counterData.Category };

        /// <inheritdoc/>
        protected async override ValueTask ExecuteActionAsync(CancellationToken stopToken)
        {
            var clientId = await _telemetricsClient.GetClientIdAsync(stopToken);
            var (memoryUsage, cpuUsage) = ProgramInfo.GetMemoryAndCpuUsage();

            var data = new Dictionary<string, object>
            {
                ["ClientId"] = clientId,
                ["ComputerName"] = Utility.ComputerName,
                ["ClientTimestamp"] = DateTime.UtcNow.Round(),
                ["OSDescription"] = $"{RuntimeInformation.OSDescription} {Environment.GetEnvironmentVariable("OS")}",
                ["DotnetFramework"] = RuntimeInformation.FrameworkDescription,
                ["MemoryUsage"] = memoryUsage,
                ["CPUUsage"] = cpuUsage,
                ["FQDN"] = Utility.HostName,
                ["KinesisTapVersionNumber"] = ProgramInfo.GetVersionNumber()
            };

            if (await AWSUtilities.GetIsEC2Instance(stopToken))
            {
                data["InstanceId"] = EC2InstanceMetadata.InstanceId;
                data["InstanctType"] = EC2InstanceMetadata.InstanceType;
            }

            if (!string.IsNullOrEmpty(Utility.AgentId))
            {
                data.Add("AgentId", Utility.AgentId);
            }

            if (!string.IsNullOrEmpty(Utility.UserId))
            {
                data.Add("UserId", Utility.UserId);
            }

            // aggregate the incremental metrics using their sum
            AggregateMetrics(_incrementalMetrics.ToArray(), data, list => list.Sum(l => l.Value));

            // aggregate the current-value metrics using the average of the metrics with the same name
            AggregateMetrics(_currentMetrics.ToArray(), data, list => (long)list.Average(l => l.Value));

            _logger.LogDebug("Sending {0} metrics", data.Count);
            await _telemetricsClient.PutMetricsAsync(data, stopToken);
        }

        protected void AggregateMetrics(KeyValuePair<MetricKey, MetricValue>[] sourceMetrics,
            Dictionary<string, object> destinationData,
            Func<IEnumerable<MetricValue>, object> aggregator)
        {
            foreach (var group in sourceMetrics.GroupBy(
                    kv => kv.Key.Name,
                    (k, g) => new KeyValuePair<string, object>(k, aggregator(g.Select(kv => kv.Value)))
                )
            )
            {
                destinationData[group.Key] = group.Value;
            }
        }
    }
}
