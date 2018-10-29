using Amazon.KinesisTap.Core;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics.Eventing.Reader;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Windows
{
    public class EventRecordEnvelope : Envelope<EventInfo>
    {
        public EventRecordEnvelope(EventRecord record, bool includeEventData) : base(ConvertEventRecordToEventInfo(record, includeEventData))
        {
        }

        public override DateTime Timestamp => _data.TimeCreated ?? _timeStamp;

        public override string ToString()
        {
            return $"[{_data.LogName}] [{_data.LevelDisplayName}] [{_data.EventId}] [{_data.ProviderName}] [{ _data.MachineName}] [{_data.Description}]";
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
                Keywords = GetKeywords(record.KeywordsDisplayNames),
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

        private static string GetKeywords(IEnumerable<string> names)
        {
            if (names != null)
            {
                try
                {
                    return string.Join(",", names.ToArray());
                }
                catch { }
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
