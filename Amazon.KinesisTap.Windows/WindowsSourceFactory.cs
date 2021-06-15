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
using System.Linq;
using System.Runtime.Versioning;
using Amazon.KinesisTap.Core;
using Microsoft.Diagnostics.Tracing;
using Microsoft.Extensions.Configuration;

namespace Amazon.KinesisTap.Windows
{
    [SupportedOSPlatform("windows")]
    public class WindowsSourceFactory : IFactory<ISource>
    {
        const string WINDOWS_EVENT_LOG_SOURCE = "windowseventlogsource";
        const string WINDOWS_PERFORMANCE_COUNTER_SOURCE = "windowsperformancecountersource";
        const string WINDOWS_ETW_EVENT_SOURCE = "windowsetweventsource";
        const string WINDOWS_EVENT_LOG_POLLING_SOURCE = "windowseventlogpollingsource";

        public ISource CreateInstance(string entry, IPlugInContext context)
        {
            var config = context.Configuration;
            if (!OperatingSystem.IsWindows())
            {
                throw new PlatformNotSupportedException($"Source type '{entry}' is only supported on Windows");
            }

            switch (entry.ToLowerInvariant())
            {
                case WINDOWS_EVENT_LOG_POLLING_SOURCE:
                    var pollingOptions = new WindowsEventLogPollingSourceOptions();
                    ParseWindowsEventLogSourceOptions(config, pollingOptions);
                    ParseEventLogPollingSourceOptions(config, pollingOptions);
                    var weps = new WindowsEventPollingSource(config[ConfigConstants.ID],
                        config["LogName"], config["Query"], context.BookmarkManager, pollingOptions, context);
                    return weps;
                case WINDOWS_EVENT_LOG_SOURCE:
                    var eventOpts = new WindowsEventLogSourceOptions();
                    ParseWindowsEventLogSourceOptions(config, eventOpts);
                    var source = new EventLogSource(config[ConfigConstants.ID], config["LogName"], config["Query"],
                        context.BookmarkManager, eventOpts, context);
                    return source;
                case WINDOWS_PERFORMANCE_COUNTER_SOURCE:
                    var performanceCounterSource = new PerformanceCounterSource(context);
                    return performanceCounterSource;
                case WINDOWS_ETW_EVENT_SOURCE:
                    var providerName = config["ProviderName"];
                    var traceLevelString = DefaultMissingConfig(config["TraceLevel"], "Verbose");
                    var matchAnyKeywordString = DefaultMissingConfig(config["MatchAnyKeyword"], ulong.MaxValue.ToString());

                    if (string.IsNullOrWhiteSpace(providerName))
                    {
                        throw new Exception($"A provider name must be specified for the WindowsEtwEventSource.");
                    }

                    TraceEventLevel traceLevel;
                    ulong matchAnyKeyword;

                    if (!Enum.TryParse<TraceEventLevel>(traceLevelString, out traceLevel))
                    {
                        var validNames = string.Join(", ", Enum.GetNames(typeof(TraceEventLevel)));
                        throw new Exception($"{traceLevelString} is not a valid trace level value ({validNames}) for the WindowsEtwEventSource.");
                    }

                    matchAnyKeyword = ParseMatchAnyKeyword(matchAnyKeywordString);

                    var eventSource = new EtwEventSource(providerName, traceLevel, matchAnyKeyword, context);
                    return eventSource;

                default:
                    throw new Exception($"Source type {entry} not recognized.");
            }
        }

        private void ParseWindowsEventLogSourceOptions(IConfiguration config, WindowsEventLogSourceOptions options)
        {
            if (config["CustomFilters"] is not null)
            {
                options.CustomFilters = config["CustomFilters"].Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries);
                var badFilters = string.Join(",", options.CustomFilters.Where(i => EventInfoFilters.GetFilter(i) == null));
                if (!string.IsNullOrWhiteSpace(badFilters))
                {
                    throw new ConfigurationException($"Custom filter/s '{badFilters}' do not exist. Please check the filter names.");
                }
            }

            if (bool.TryParse(config["IncludeEventData"], out var ied))
            {
                options.IncludeEventData = ied;
            }

            if (bool.TryParse(config["BookmarkOnBufferFlush"], out var bookmarkOnBufferFlush))
            {
                options.BookmarkOnBufferFlush = bookmarkOnBufferFlush;
            }

            LoadInitialPositionSettings(config, options);
        }

        private void ParseEventLogPollingSourceOptions(IConfiguration config, WindowsEventLogPollingSourceOptions options)
        {
            if (int.TryParse(config["MaxReaderDelayMs"], out var maxReaderDelayMs))
            {
                options.MaxReaderDelayMs = maxReaderDelayMs;
            }
            if (int.TryParse(config["MinReaderDelayMs"], out var minReaderDelayMs))
            {
                options.MinReaderDelayMs = minReaderDelayMs;
            }
            if (int.TryParse(config["DelayThreshold"], out var delayThreshold))
            {
                options.DelayThreshold = delayThreshold;
            }
        }

        private void LoadInitialPositionSettings(IConfiguration config, WindowsEventLogSourceOptions options)
        {
            var initialPositionConfig = config["InitialPosition"];
            if (!string.IsNullOrEmpty(initialPositionConfig))
            {
                switch (initialPositionConfig.ToLower())
                {
                    case "eos":
                        options.InitialPosition = InitialPositionEnum.EOS;
                        break;
                    case "0":
                        options.InitialPosition = InitialPositionEnum.BOS;
                        break;
                    case "bookmark":
                        options.InitialPosition = InitialPositionEnum.Bookmark;
                        break;
                    case "timestamp":
                        options.InitialPosition = InitialPositionEnum.Timestamp;
                        string initialPositionTimeStamp = config["InitialPositionTimestamp"];
                        if (string.IsNullOrWhiteSpace(initialPositionTimeStamp))
                        {
                            throw new Exception("Missing initial position timestamp.");
                        }

                        try
                        {
                            var timeZone = Utility.ParseTimeZoneKind(config["TimeZoneKind"]);
                            var timestamp = DateTime.Parse(initialPositionTimeStamp, null, System.Globalization.DateTimeStyles.RoundtripKind);
                            options.InitialPositionTimestamp = timeZone == DateTimeKind.Utc
                                ? DateTime.SpecifyKind(timestamp, DateTimeKind.Utc)
                                : DateTime.SpecifyKind(timestamp, DateTimeKind.Local).ToUniversalTime();
                        }
                        catch
                        {
                            throw new Exception($"Invalid InitialPositionTimestamp {initialPositionTimeStamp}");
                        }

                        break;
                    default:
                        throw new Exception($"Invalid InitialPosition {initialPositionConfig}");
                }
            }
        }

        private ulong ParseMatchAnyKeyword(string matchAnyKeywordString)
        {
            if (string.IsNullOrEmpty(matchAnyKeywordString))
            {
                return ulong.MaxValue;
            }
            ulong matchAnyKeyword = 0;
            if (matchAnyKeywordString.Contains(","))
            {
                foreach (var keyword in matchAnyKeywordString.Split(','))
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
            return (string.IsNullOrWhiteSpace(configValue?.Trim())) ? defaultConfigValue : configValue.Trim();
        }

        public void RegisterFactory(IFactoryCatalog<ISource> catalog)
        {
            catalog.RegisterFactory(WINDOWS_EVENT_LOG_POLLING_SOURCE, this);
            catalog.RegisterFactory(WINDOWS_EVENT_LOG_SOURCE, this);
            catalog.RegisterFactory(WINDOWS_PERFORMANCE_COUNTER_SOURCE, this);
            catalog.RegisterFactory(WINDOWS_ETW_EVENT_SOURCE, this);
        }
    }
}
