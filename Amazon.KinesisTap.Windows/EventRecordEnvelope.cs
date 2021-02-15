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
namespace Amazon.KinesisTap.Windows
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.Eventing.Reader;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Xml;
    using System.Xml.Linq;
    using Amazon.KinesisTap.Core;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json.Linq;

    public class EventRecordEnvelope : Envelope<EventInfo>
    {
        public EventRecordEnvelope(EventRecord record, bool includeEventData, int bookmarkId) : base(ConvertEventRecordToEventInfo(record, includeEventData))
        {
            this.BookmarkId = bookmarkId;
            this.Position = record.RecordId ?? 0;
        }

        public override DateTime Timestamp => _data.TimeCreated ?? _timeStamp;

        public override string ToString()
        {
            return $"[{_data.LogName}] [{_data.LevelDisplayName}] [{_data.EventId}] [{_data.ProviderName}] [{ _data.MachineName}] [{_data.Description}]";
        }

        public override string GetMessage(string format)
        {
            if (ConfigConstants.FORMAT_XML_2.Equals(format, StringComparison.CurrentCultureIgnoreCase))
            {
                return _data.Xml;
            }
            else if (ConfigConstants.FORMAT_SUSHI.Equals(format, StringComparison.CurrentCultureIgnoreCase))
            {
                return FormatSushiMessage();
            }
            else if (ConfigConstants.FORMAT_RENDERED_XML.Equals(format, StringComparison.CurrentCultureIgnoreCase))
            {
                return FormatRenderedXml();
            }

            return base.GetMessage(format);
        }

        private string FormatRenderedXml()
        {
            try
            {
                var eventNode = XElement.Parse(_data.Xml);
                var renderingInfo = new XElement("RenderingInfo");
                renderingInfo.Add(new XAttribute("Culture", CultureInfo.CurrentCulture.Name));
                AddXElementWithoutNamespace(eventNode, renderingInfo);

                AddXElementWithoutNamespace(renderingInfo, new XElement("Message", _data.Description));
                AddXElementWithoutNamespace(renderingInfo, new XElement("Level", _data.LevelDisplayName));
                AddXElementWithoutNamespace(renderingInfo, new XElement("Task", _data.Task));
                AddXElementWithoutNamespace(renderingInfo, new XElement("Opcode", _data.Opcode));
                AddXElementWithoutNamespace(renderingInfo, new XElement("Channel", _data.LogName));
                AddXElementWithoutNamespace(renderingInfo, new XElement("Provider", _data.ProviderName));

                var keywordElement = new XElement("Keywords");
                AddXElementWithoutNamespace(renderingInfo, keywordElement);

                foreach (var keyword in _data.Keywords.Split(EventInfo.KeywordSeparator, StringSplitOptions.RemoveEmptyEntries))
                {
                    AddXElementWithoutNamespace(keywordElement, new XElement("Keyword", keyword));
                }

                using (var sw = new StringWriter())
                using (var xtw = new XmlTextWriter(sw)
                {
                    Formatting = Formatting.Indented,
                    QuoteChar = '\''
                })
                {
                    eventNode.WriteTo(xtw);
                    xtw.Flush();

                    return sw.ToString();
                }
            }
            catch (XmlException xmlException)
            {
                // fall back to default xml
                PluginContext.ServiceLogger?.LogError(0, xmlException, "Error encountered while formatting RenderedXml");
                return _data.Xml;
            }
        }

        /// <summary>
        /// Append <paramref name="child"/> to the list of child node of <paramref name="parent"/> without adding 'xmlns'.
        /// </summary>
        private static void AddXElementWithoutNamespace(XElement parent, XElement child)
        {
            parent.Add(child);
            child.Attributes("xmlns").Remove();
            // Inherit the parent namespace instead
            child.Name = child.Parent.Name.Namespace + child.Name.LocalName;
        }

        private string FormatSushiMessage()
        {
            JObject json =
                new JObject(
                    //Note that EventID is depreciated. See https://w.amazon.com/bin/view/Sushi/WindowsAgent
                    new JProperty("Id", _data.EventId),
                    new JProperty("EventCode", _data.EventId),
                    new JProperty("MachineName", _data.MachineName),
                    new JProperty("ProviderName", _data.ProviderName),
                    new JProperty("RecordId", _data.Index),
                    new JProperty("TimeCreated", _data.TimeCreated),
                    new JProperty("UserId", _data.UserName ?? "N\\A"),
                    new JProperty("Message", _data.Description));

            return json.ToString();
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
                    return string.Join(",", names);
                }
            }
            catch (EventLogNotFoundException)
            {
                //Some but not all events with eventId 0 throws EventLogNotFoundException when accessing the KeywordsDisplayNames property
                PluginContext.ServiceLogger?.LogDebug($"Unable to get KeywordsDisplayNames for event Id {record.Id} and provider {record.ProviderName}");
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
