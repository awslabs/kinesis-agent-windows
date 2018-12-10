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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Xml.Serialization;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Amazon.KinesisTap.Core
{
    public class Envelope<T> : IEnvelope<T>
    {
        protected DateTime _timeStamp;
        protected T _data;
        protected static readonly JsonSerializerSettings _jsonSettings = new JsonSerializerSettings
        {
            ReferenceLoopHandling = ReferenceLoopHandling.Ignore
        };

        public Envelope(T data) : this(data, DateTime.UtcNow)
        {
        }

        public Envelope(T data, DateTime timestamp)
        {
            _data = data;
            _timeStamp = timestamp;
        }

        public virtual DateTime Timestamp => _timeStamp;

        public T Data => _data;

        public virtual string GetMessage(string format)
        {
            switch ((format ?? string.Empty).ToLower())
            {
                case "json":
                    return this.ToJson();
                case "xml":
                    return this.ToXml();
                default:
                    return this.ToString();
            }
        }

        public override string ToString()
        {
            return _data?.ToString();
        }

        public virtual object ResolveLocalVariable(string variable)
        {
            if (_data == null) return null;

            variable = variable.Substring(1);
            if (_data is IDictionary dictionary) //Dictionary<,> and ReadOnlyDictionary<,> both implement this IDictionary
            {
                if (dictionary.Contains(variable))
                {
                    return dictionary[variable];
                }
            }
            else if (_data is IDictionary<string, JToken> jObject)
            {
                if (jObject.TryGetValue(variable, out JToken value))
                {
                    return value;
                }
            }
            else
            {
                var prop = _data.GetType().GetProperty(variable, BindingFlags.Public | BindingFlags.Instance);
                if (prop != null)
                {
                    return prop.GetValue(_data);
                }
            }
            return null;
        }

        protected virtual string ToJson()
        {
            if (_data == null) return null;

            if (Data is IJsonConvertable convertable)
            {
                return convertable.ToJson();
            }
            return JsonConvert.SerializeObject(_data, _jsonSettings);
        }

        protected virtual string ToXml()
        {
            if (_data == null) return null;

            var stringwriter = new System.IO.StringWriter();
            var serializer = new XmlSerializer(_data.GetType());
            serializer.Serialize(stringwriter, _data);
            return stringwriter.ToString();
        }

        private static XmlSerializer GetXmlSerializer(Type t)
        {
            //todo: cache the XmlSerializer instead of create one every time
            return new XmlSerializer(t);
        }
    }
}
