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
using Amazon.KinesisTap.Core.Metrics;
using Amazon.Util;
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.AWS.Telemetrics
{
    public class TelemetricsSink : AWSMetricsSink<IDictionary<string, object>, HttpResponseMessage, long>
    {
        private ITelemetricsClient<HttpResponseMessage> _telemetricsClient;
        private string _clientId;

        private const int ATTEMPT_LIMIT = 3;
        private const int FLUSH_QUEUE_DELAY = 100; //Throttle at about 10 TPS

        protected override int AttemptLimit => ATTEMPT_LIMIT;

        protected override int FlushQueueDelay => FLUSH_QUEUE_DELAY;

        public TelemetricsSink(int defaultInterval, IPlugInContext context, ITelemetricsClient<HttpResponseMessage> telemetricsClient) : base(defaultInterval, context)
        {
            _telemetricsClient = telemetricsClient;
        }

        public override void Start()
        {
            base.Start();
            _clientId = _context.ParameterStore.GetParameter(_telemetricsClient.ClientIdParameterName);
            if (string.IsNullOrWhiteSpace(_clientId))
            {
                _clientId = _telemetricsClient.CreateClientIdAsync().Result;
                _context.ParameterStore.SetParameter(_telemetricsClient.ClientIdParameterName, _clientId);
            }
            _telemetricsClient.ClientId = _clientId;
        }

        protected override void OnFlush(IDictionary<MetricKey, MetricValue> accumlatedValues, IDictionary<MetricKey, MetricValue> lastValues)
        {
            Dictionary<string, object> data = new Dictionary<string, object>
            {
                ["ClientId"] = _clientId,
                ["ComputerName"] = Utility.ComputerName,
                ["ClientTimestamp"] = DateTime.UtcNow.Round(),
                ["OSDescription"] = RuntimeInformation.OSDescription + " " + Environment.GetEnvironmentVariable("OS"),
                ["DotnetFramework"] = RuntimeInformation.FrameworkDescription,
                ["MemoryUsage"] = ProgramInfo.GetMemoryUsage(),
                ["CPUUsage"] = ProgramInfo.GetCpuUsage(),
                ["InstanceId"] = EC2InstanceMetadata.InstanceId,
                ["InstanctType"] = EC2InstanceMetadata.InstanceType,
                ["FQDN"] = Utility.HostName,
                ["IPAddress"] = EC2InstanceMetadata.PrivateIpAddress,
                ["KinesisTapVersionNumber"] = ProgramInfo.GetVersionNumber()
            };

            if (!string.IsNullOrEmpty(Utility.AgentId))
            {
                data.Add("AgentId", Utility.AgentId);
            }

            if (!string.IsNullOrEmpty(Utility.UserId))
            {
                data.Add("UserId", Utility.UserId);
            }

            if (accumlatedValues != null)
            {
                AggregateMetrics(accumlatedValues, data, list => list.Sum(l => l.Value));
            }

            if (lastValues != null)
            {
                AggregateMetrics(lastValues, data, list => (long)list.Average(l => l.Value));
            }
            PutMetricDataAsync(data).Wait();
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

        protected override bool IsRecoverable(Exception ex)
        {
            return !(ex is ArgumentException
                || ex is ArgumentNullException
                || ex is InvalidOperationException);
        }

        protected override async Task<HttpResponseMessage> SendRequestAsync(IDictionary<string, object> data)
        {
            var response = await _telemetricsClient.PutMetricsAsync(data);
            return response;
        }
    }
}
