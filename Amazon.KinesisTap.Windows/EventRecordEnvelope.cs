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
using System.Diagnostics.Eventing.Reader;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.Windows
{
    public class EventRecordEnvelope : Envelope<EventInfo>
    {
        public EventRecordEnvelope(EventRecord record, bool includeEventData, IPlugInContext context) : base(ConvertEventRecordToEventInfo(record, includeEventData))
        {
        }

        public override DateTime Timestamp => _data.TimeCreated ?? _timeStamp;

        public override string ToString()
        {
            return $"[{_data.LogName}] [{_data.LevelDisplayName}] [{_data.EventId}] [{_data.ProviderName}] [{ _data.MachineName}] [{_data.Description}]";
        }

        public override string GetMessage(string format)
        {
            if ("xml2".Equals(format, StringComparison.CurrentCultureIgnoreCase))
            {
                return _data.Xml;
            }

            return base.GetMessage(format);
        }

        private static EventInfo ConvertEventRecordToEventInfo(EventRecord record, bool includeEventData)
        {
            var eventInfo = new EventInfo()
            {
                EventId = record.Id,
                LevelDisplayName = GetLevelDisplayName(record.Level), //The LevelDisplayName sometime get EventLogNotFoundException exception
                LogName = record.LogName,
                MachineName = record.MachineName,
                ProviderName = record.ProviderName,
                TimeCreated = Utility.ToUniversalTime(record.TimeCreated),
                Description = record.FormatDescription(),
                Index = record.RecordId,
                UserName = record.UserId?.Value,
                Keywords = GetKeywords(record),
                Xml = record.ToXml(),
            };

            if (includeEventData)
            {
                eventInfo.EventData = GetEventData(record.Properties);
            }

            return eventInfo;
        }

        private static string GetLevelDisplayName(byte? level)
        {
            if (level.HasValue)
            {
                return ((StandardEventLevel)level).ToString();
            }
            return string.Empty;
        }

        private static string GetKeywords(EventRecord record)
        {
            try
            {
                IEnumerable<string> names = record.KeywordsDisplayNames;
                if (names != null)
                {
                    return string.Join(",", names.ToArray());
                }
            }
            catch(EventLogNotFoundException)
            {
                //Some but not all events with eventId 0 throws EventLogNotFoundException when accessing the KeywordsDisplayNames property
                PluginContext.ApplicationContext?.Logger?.LogDebug($"Unable to get KeywordsDisplayNames for event Id {record.Id} and provider {record.ProviderName}");
            }
            return string.Empty;
        }

        private static List<object> GetEventData(IList<EventProperty> properties)
        {
            if (properties == null) return null;

            return properties.Select(ep => ep.Value).ToList();
        }
    }
}
