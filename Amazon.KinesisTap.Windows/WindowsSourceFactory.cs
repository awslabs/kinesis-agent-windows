using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Diagnostics.Eventing.Reader;
using Microsoft.Diagnostics.Tracing;

namespace Amazon.KinesisTap.Windows
{
    public class WindowsSourceFactory : IFactory<ISource>
    {
        const string WINDOWS_EVENT_LOG_SOURCE = "windowseventlogsource";
        const string WINDOWS_PERFORMANCE_COUNTER_SOURCE = "windowsperformancecountersource";
        const string WINDOWS_ETW_EVENT_SOURCE = "windowsetweventsource";

        public ISource CreateInstance(string entry, IPlugInContext context)
        {
            IConfiguration config = context.Configuration;
            ILogger logger = context.Logger;

            switch (entry.ToLowerInvariant())
            {
                case "windowseventlogsource":
                    string logName = config["LogName"];
                    string query = config["Query"];
                    EventLogSource source = new EventLogSource(logName, query, context);
                    EventSource<EventInfo>.LoadCommonSourceConfig(config, source);
                    return source;
                case "windowsperformancecountersource":
                    PerformanceCounterSource performanceCounterSource = new PerformanceCounterSource(context);
                    return performanceCounterSource;
                case "windowsetweventsource":
                    string providerName = config["ProviderName"];
                    string traceLevelString = DefaultMissingConfig(config["TraceLevel"], "Verbose");
                    string matchAnyKeywordString = DefaultMissingConfig(config["MatchAnyKeyword"], ulong.MaxValue.ToString());

                    if (string.IsNullOrWhiteSpace(providerName))
                    {
                        throw new Exception($"A provider name must be specified for the WindowsEtwEventSource.");
                    }
                   
                    TraceEventLevel traceLevel;
                    ulong matchAnyKeyword;

                    if (!Enum.TryParse<TraceEventLevel>(traceLevelString, out traceLevel))
                    {
                        string validNames = string.Join(", ", Enum.GetNames(typeof(TraceEventLevel)));
                        throw new Exception($"{traceLevelString} is not a valid trace level value ({validNames}) for the WindowsEtwEventSource.");
                    }

                    matchAnyKeyword = ParseMatchAnyKeyword(matchAnyKeywordString);

                    var eventSource = new EtwEventSource(providerName, traceLevel, matchAnyKeyword, context);
                    return eventSource;

                default:
                    throw new Exception($"Source type {entry} not recognized.");
            }
        }

        private ulong ParseMatchAnyKeyword(string matchAnyKeywordString)
        {
            ulong matchAnyKeyword = 0;
            if (matchAnyKeywordString.Contains(","))
            {
                foreach (string keyword in matchAnyKeywordString.Split(','))
                {
                    matchAnyKeyword |= ParseMatchAnyKeyword(keyword.Trim());
                }
            }
            else
            {
                if (matchAnyKeywordString.StartsWith("0x") || matchAnyKeywordString.StartsWith("0X"))
                {
                    matchAnyKeyword = Convert.ToUInt64(matchAnyKeywordString.Substring(2), 16);
                }
                else
                {
                    if (!ulong.TryParse(matchAnyKeywordString, out matchAnyKeyword))
                    {
                        throw new Exception($"{matchAnyKeywordString} is not a valid MatchAnyKeyword value (an unsigned long integer) for the WindowsEtwEventSource.");
                    }
                }
            }
            return matchAnyKeyword;
        }

        private string DefaultMissingConfig(string configValue, string defaultConfigValue)
        {
            return (string.IsNullOrWhiteSpace(configValue.Trim())) ? defaultConfigValue : configValue.Trim();
        }

        public void RegisterFactory(IFactoryCatalog<ISource> catalog)
        {
            catalog.RegisterFactory(WINDOWS_EVENT_LOG_SOURCE, this);
            catalog.RegisterFactory(WINDOWS_PERFORMANCE_COUNTER_SOURCE, this);
            catalog.RegisterFactory(WINDOWS_ETW_EVENT_SOURCE, this);
        }
    }
}
