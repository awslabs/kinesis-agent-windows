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
namespace Amazon.KinesisTap.Core.EMF
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Amazon.KinesisTap.Core;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// Implement the filter pipe semantics. The type is converted to JObject.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class EMFPipe<T> : Pipe<T, JObject>
    {
        private readonly CloudWatchMetric metric;
        private readonly JArray jArray;

        public EMFPipe(IPlugInContext context) : base(context)
        {
            this.Id = context.Configuration[ConfigConstants.ID];
            var config = context.Configuration.GetSection("MetricDefinition");
            this.metric = new CloudWatchMetric(config);
            this.jArray = new JArray(new[] { JObject.FromObject(this.metric) });
        }

        private static string CleanKeyName(string key)
        {
            if (!key.Contains("(")) return key;
            return key.Replace('(', '-').Replace(")", string.Empty);
        }

        private static bool KeyIsNotReservedProperty(string key)
        {
            return !string.Equals("timestamp", key, StringComparison.OrdinalIgnoreCase) && !string.Equals("version", key, StringComparison.OrdinalIgnoreCase);
        }

        public override void OnNext(IEnvelope<T> value)
        {
            var jo = new JObject();

            if (value.Data is IEnumerable<KeyValuePair<string, string>> dic)
            {
                foreach (var kvp in dic.Where(i => KeyIsNotReservedProperty(i.Key)))
                    jo[CleanKeyName(kvp.Key)] = kvp.Value;
            }
            else if (value.Data is IEnumerable<KeyValuePair<string, JToken>> jt)
            {
                foreach (var kvp in jt.Where(i => KeyIsNotReservedProperty(i.Key)))
                    jo[CleanKeyName(kvp.Key)] = kvp.Value;
            }
            else
            {
                _context.Logger.LogWarning("{0} only supports sources that output a type that inherits from IEnumerable<KeyValuePair<string, string>> or IEnumerable<KeyValuePair<string, JToken>>", nameof(EMFPipe<T>));
                return;
            }

            foreach (var m in this.metric.Metrics)
            {
                if (jo.TryGetValue(m.Name, StringComparison.CurrentCultureIgnoreCase, out JToken mVal))
                {
                    // The metric property MUST be a numeric, so if it has been parsed as a string,
                    // we need to convert it to a long so that CW can convert it properly.
                    if (mVal.Type == JTokenType.String && long.TryParse(mVal.ToObject<string>(), out long val))
                    {
                        jo.Remove(m.Name);
                        jo[m.Name] = val;
                    }

                    continue;
                }

                // If the object doesn't have the property specified in the MetricName, use a default value.
                jo[m.Name] = m.Value ?? 1;
            }

            jo["Timestamp"] = Utility.ToEpochMilliseconds(value.Timestamp);
            jo["Version"] = "0";
            jo["CloudWatchMetrics"] = this.jArray;

            this._subject.OnNext(new Envelope<JObject>(jo, value.Timestamp, value.BookmarkId, value.Position));
        }

        /// <inheritdoc />
        public override void Start()
        {
        }

        /// <inheritdoc />
        public override void Stop()
        {
        }
    }    
}
