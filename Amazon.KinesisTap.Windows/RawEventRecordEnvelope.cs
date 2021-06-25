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
using System.Globalization;
using System.IO;
using System.Runtime.Versioning;
using System.Xml;
using System.Xml.Linq;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Windows
{
    /// <summary>
    /// Unlike <see cref="EventRecordEnvelope"/>, this class defers converting Event log data until requested to format them.
    /// </summary>
    [SupportedOSPlatform("windows")]
    internal class RawEventRecordEnvelope : Envelope<EventRecord>
    {
        private readonly bool _includeEventData;
        private bool _disposed = false;

        public RawEventRecordEnvelope(EventRecord record, bool includeEventData, IntegerPositionRecordBookmark bookmarkData)
            : base(record, record.TimeCreated?.ToUniversalTime() ?? DateTime.UtcNow, bookmarkData, record.RecordId ?? 0)
        {
            _includeEventData = includeEventData;
        }

        public override string GetMessage(string format)
        {
            if (string.IsNullOrEmpty(format)) //plain text
            {
                return $"[{_data.LogName}] [{_data.LevelDisplayName}] [{_data.Id}] [{_data.ProviderName}] [{ _data.MachineName}] [{_data.FormatDescription()}]";
            }
            if (ConfigConstants.FORMAT_RENDERED_XML.Equals(format, StringComparison.CurrentCultureIgnoreCase))
            {
                return FormatRenderedXml();
            }
            if (ConfigConstants.FORMAT_XML_2.Equals(format, StringComparison.CurrentCultureIgnoreCase))
            {
                return _data.ToXml();
            }

            return GetFallbackEnvelopeFormat(format);
        }

        private string GetFallbackEnvelopeFormat(string format)
        {
            using var ere = new EventRecordEnvelope(_data, _includeEventData, BookmarkData as IntegerPositionRecordBookmark);
            var message = ere.GetMessage(format);
            return message;
        }

        private string FormatRenderedXml()
        {
            try
            {
                var eventNode = XElement.Parse(_data.ToXml());
                var renderingInfo = new XElement("RenderingInfo");
                renderingInfo.Add(new XAttribute("Culture", CultureInfo.CurrentCulture.Name));
                AddXElementWithoutNamespace(eventNode, renderingInfo);

                AddXElementWithoutNamespace(renderingInfo, new XElement("Message", _data.FormatDescription()));
                AddXElementWithoutNamespace(renderingInfo, new XElement("Level", GetLevelDisplayName(_data)));
                AddXElementWithoutNamespace(renderingInfo, new XElement("Task", _data.Task));
                AddXElementWithoutNamespace(renderingInfo, new XElement("Opcode", _data.Opcode));
                AddXElementWithoutNamespace(renderingInfo, new XElement("Channel", _data.LogName));
                AddXElementWithoutNamespace(renderingInfo, new XElement("Provider", _data.ProviderName));

                var keywordElement = new XElement("Keywords");
                AddXElementWithoutNamespace(renderingInfo, keywordElement);

                foreach (var keyword in GetKeywords(_data))
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
                return _data.ToXml();
            }
        }

        private static IEnumerable<string> GetKeywords(EventRecord eventRecord)
        {
            try
            {
                IEnumerable<string> names = eventRecord.KeywordsDisplayNames;
                if (names != null)
                {
                    return names;
                }
            }
            catch (EventLogNotFoundException)
            {
            }

            return Array.Empty<string>();
        }

        private static string GetLevelDisplayName(EventRecord eventRecord)
        {
            try
            {
                return eventRecord.LevelDisplayName;
            }
            catch (EventLogException)
            {
                if (eventRecord.Level.HasValue)
                {
                    return ((StandardEventLevel)eventRecord.Level).ToString();
                }
            }

            return string.Empty;
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

        protected override void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                _data.Dispose();
            }

            _disposed = true;

            // Call base class implementation.
            base.Dispose(disposing);
        }
    }
}
