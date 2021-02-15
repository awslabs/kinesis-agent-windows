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
    using System.Diagnostics.Eventing.Reader;
    using System.Globalization;
    using System.IO;
    using System.Xml;
    using System.Xml.Linq;
    using Amazon.KinesisTap.Core;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Unlike <see cref="EventRecordEnvelope"/>, this class defers converting Event log data until requested to format them.
    /// </summary>
    internal class RawEventRecordEnvelope : Envelope<EventRecord>
    {
        private readonly bool _includeEventData;

        public RawEventRecordEnvelope(EventRecord record, bool includeEventData, int bookmarkId)
            : base(record)
        {
            _includeEventData = includeEventData;
            BookmarkId = bookmarkId;
            Position = record.RecordId ?? 0;

            if (_data.TimeCreated.HasValue)
            {
                var time = _data.TimeCreated.Value.Kind == DateTimeKind.Unspecified
                    ? DateTime.SpecifyKind(_data.TimeCreated.Value, DateTimeKind.Local)
                    : _data.TimeCreated.Value;
                Timestamp = time.ToUniversalTime();
            }
            else
            {
                Timestamp = DateTime.UtcNow;
            }
        }

        public override DateTime Timestamp { get; }

        public override string GetMessage(string format)
        {
            if (string.IsNullOrEmpty(format)) //plain text
            {
                return _data.ToString();
                //return $"[{_data.LogName}] [{_data.LevelDisplayName}] [{_data.Id}] [{_data.ProviderName}] [{ _data.MachineName}] [{_data.FormatDescription()}]";
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
            => new EventRecordEnvelope(_data, _includeEventData, BookmarkId.Value).GetMessage(format);

        private string FormatRenderedXml()
        {
            try
            {
                var eventNode = XElement.Parse(_data.ToXml());
                var renderingInfo = new XElement("RenderingInfo");
                renderingInfo.Add(new XAttribute("Culture", CultureInfo.CurrentCulture.Name));
                AddXElementWithoutNamespace(eventNode, renderingInfo);

                AddXElementWithoutNamespace(renderingInfo, new XElement("Message", _data.FormatDescription()));
                AddXElementWithoutNamespace(renderingInfo, new XElement("Level", _data.LevelDisplayName));
                AddXElementWithoutNamespace(renderingInfo, new XElement("Task", _data.Task));
                AddXElementWithoutNamespace(renderingInfo, new XElement("Opcode", _data.Opcode));
                AddXElementWithoutNamespace(renderingInfo, new XElement("Channel", _data.LogName));
                AddXElementWithoutNamespace(renderingInfo, new XElement("Provider", _data.ProviderName));

                var keywordElement = new XElement("Keywords");
                AddXElementWithoutNamespace(renderingInfo, keywordElement);

                foreach (var keyword in _data.KeywordsDisplayNames)
                {
                    AddXElementWithoutNamespace(keywordElement, new XElement("Keyword", keyword));
                }

                using (var sw = new StringWriter())
                using (var xtw = new XmlTextWriter(sw)
                {
                    Formatting = System.Xml.Formatting.Indented,
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
    }
}
