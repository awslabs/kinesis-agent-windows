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
#if DEBUG
        const string CLIENT_ID = "ClientId_Debug";
#else
        const string CLIENT_ID = "ClientId";
#endif

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
            _clientId = _context.ParameterStore.GetParameter(CLIENT_ID);
            if (string.IsNullOrWhiteSpace(_clientId))
            {
                _clientId = TelemetricsClient.Default.GetClientIdAsync().Result;
                _context.ParameterStore.SetParameter(CLIENT_ID, _clientId);
            }
            _telemetricsClient.ClientId = _clientId;
        }

        protected override void OnFlush(IDictionary<MetricKey, MetricValue> accumlatedValues, IDictionary<MetricKey, MetricValue> lastValues)
        {
            Dictionary<string, object> data = new Dictionary<string, object>();
            data["ClientId"] = _clientId;
            data["ClientTimestamp"] = DateTime.UtcNow.Round();
            data["OSDescription"] =  RuntimeInformation.OSDescription + " " + Environment.GetEnvironmentVariable("OS");
            data["DotnetFramework"] = RuntimeInformation.FrameworkDescription;
            data["MemoryUsage"] = ProgramInfo.GetMemoryUsage();
            data["CPUUsage"] = ProgramInfo.GetCpuUsage();
            data["InstanceId"] = EC2InstanceMetadata.InstanceId;
            data["InstanctType"] = EC2InstanceMetadata.InstanceType;

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
            response.EnsureSuccessStatusCode();
            return response;
        }
    }
}
