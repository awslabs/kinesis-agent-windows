using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.AWS.Telemetrics
{
    public interface ITelemetricsClient<TResponse>
    {
        string ClientId { get; set; }

        Task<TResponse> PutMetricsAsync(IDictionary<string, object> data);
        Task<string> GetClientIdAsync();
    }
}