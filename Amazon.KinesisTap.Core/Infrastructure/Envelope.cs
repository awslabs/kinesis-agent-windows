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
namespace Amazon.KinesisTap.Core
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using System.Reflection;
    using System.Text;
    using System.Xml.Serialization;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    public class Envelope<T> : IEnvelope<T>
    {
        private static readonly JsonSerializerSettings jsonSettings = new JsonSerializerSettings
        {
            ReferenceLoopHandling = ReferenceLoopHandling.Ignore
        };

        protected readonly DateTime _timeStamp;
        protected readonly T _data;

        public Envelope(T data) : this(data, DateTime.UtcNow)
        {
        }

        public Envelope(T data, DateTime timestamp)
        {
            _data = data;
            _timeStamp = timestamp;
        }

        public Envelope(T data, DateTime timestamp, int? bookmarkId, long position)
        {
            _data = data;
            _timeStamp = timestamp;
            this.Position = position;
            this.BookmarkId = bookmarkId;
        }

        public virtual DateTime Timestamp => _timeStamp;

        public T Data => _data;

        public long Position { get; set; }

        public int? BookmarkId { get; set; }

        /// <inheritdoc />
        public virtual string GetMessage(string format)
        {
            // The previous code used a switch block to make formatting decisions, first doing a
            // null coalesce with a string.Empty constant, then a ToLower operation, and use the
            // default switch case to handle null values. This means that, when compiled to MSIL,
            // it will look like "if/elseif/else" where our most common use case falls into the
            // "else" block. That's two unnecessary comparisons when people use our defaults.
            // Knowing this, it's actually more efficient to break it down into what the MSIL will
            // compile it down to and work through it that way.

            // First we'll check to see if the format parameter is null or empty. Since this is the
            // default value it's also the most likely format, so it makes sense to try this first.
            // We also need to do a null/empty check before the rest of the cases anyway. It's unlikely
            // that the format is going to be whitespace, so we'll ignore that check to save resources.
            if (string.IsNullOrEmpty(format))
                return this.ToString();

            // JSON and XML are the other natively supported formats, so we'll try those next.
            // Previously the code would do a "ToLower" on the format parameter on every invocation,
            // but we can achieve the same thing, with the same cost, using a StringComparison flag.
            // In general (considering all languages and all compilers) a switch statement CAN SOMETIMES
            // be more efficient than an if/else statement, because it is easy for a compiler to generate
            // "jump tables" from switch statements. With a large number of strings, there is a significant
            // performance advantage to using a switch statement, because the compiler will use a hash table
            // to implement the jump. With a small number of strings, the performance between the two is
            // the same, because in these cases the C# compiler does not generate a jump table, but will
            // generate if/else blocks. Since switch can't use the StringComparison flag like string.Equals
            // can, we're better off using two if's here. If we implement more than 5 cases in the future,
            // going back to a switch statement may improve performance (slightly).
            if (string.Equals(ConfigConstants.FORMAT_JSON, format, StringComparison.CurrentCultureIgnoreCase))
                return this.ToJson();

            if (string.Equals(ConfigConstants.FORMAT_XML, format, StringComparison.CurrentCultureIgnoreCase))
                return this.ToXml();

            return this.ToString();
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return _data?.ToString();
        }

        /// <inheritdoc />
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
                if (jObject.TryGetValue(variable, out JToken jToken))
                {
                    if (jToken is JValue jValue)
                    {
                        return jValue.Value;
                    }
                    return jToken;
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

        /// <inheritdoc />
        public virtual object ResolveMetaVariable(string variable)
        {
            // Expanding from switch to if statements so we can use the StringComparison.
            // This is more efficient than a switch on a string that has been lowered using
            // ToLower(). See notes in the GetMessage section for more information.
            if (string.Equals("_record", variable, StringComparison.CurrentCultureIgnoreCase))
                return this.GetMessage(null);

            if (string.Equals("_timestamp", variable, StringComparison.CurrentCultureIgnoreCase))
                return this._timeStamp;

            return null;
        }

        /// <inheritdoc />
        public virtual MemoryStream GetMessageStream(string format)
        {
            var ms = new MemoryStream();
            this.WriteMessageToStream(format, ms);
            return ms;
        }

        /// <inheritdoc />
        public virtual void WriteMessageToStream(string format, MemoryStream memoryStream)
        {
            if (this._data == null) return;

            // Since there are multiple ways of converting JSON, we need to do some deeper
            // analysis on the data's raw type to determine how to serialize the message.
            if (string.Equals(ConfigConstants.FORMAT_JSON, format, StringComparison.CurrentCultureIgnoreCase))
            {
                using (var sw = new StreamWriter(memoryStream, Encoding.UTF8, 4096, true))
                {
                    if (this._data is IJsonConvertable convertable)
                        // IJsonConvertable is an interface that custom data types implement that indicate
                        // that they use a custom method to serialize the object when the target format is JSON.
                        // We can't use the Newtonsoft serializer for these object, so we have to use the ToJson()
                        // method on the object to convert to a string and then write that string to the stream.
                        sw.Write(convertable.ToJson());
                    else if (this._data is string s)
                        // If the data type is a string, it doesn't need to be serialized into JSON, so here
                        // we just write the string to the stream.
                        sw.Write(s);
                    else
                        // Use the static JsonSerializer in the SerializationUtility class to serialize the
                        // object directly to the stream. This saves us having to serialize to a string and
                        // then convert from string to stream.
                        SerializationUtility.Json.Serialize(sw, this._data);
                }

                return;
            }

            // For all other formats (including null/empty), call the GetMessage function to
            // turn it into a string, and write that string to the stream.
            using (var sw = new StreamWriter(memoryStream, Encoding.UTF8, 4096, true))
                sw.Write(this.GetMessage(format));
        }

        protected virtual string ToJson()
        {
            if (this._data == null) return null;

            // IJsonConvertable is an interface that custom data types implement that indicate
            // that they use a custom method to serialize the object when the target format is JSON.
            if (this._data is IJsonConvertable convertable) return convertable.ToJson();

            // If the data is a string, we'll return the raw string, which might already be in JSON format.
            // If it's not in JSON format, it'll be sent through to the destination as the raw string.
            // Current behavior is to return null if the object can't be serialized, which confuses customers.
            // We can't log a warning message, because if a customer is monitoring the KinesisTap logs using the
            // Timestamp parser, this would lead to an infinite log loop and be very expensive.
            if (this._data is string s) return s;

            return JsonConvert.SerializeObject(this._data, jsonSettings);
        }

        protected virtual string ToXml()
        {
            if (this._data == null) return null;

            // If the data is a string, we'll return the raw string, which might already be in XML format.
            // If it's not in XML format, it'll be sent through to the destination as the raw string.
            // Current behavior is to return null if the object can't be serialized, which confuses customers.
            // We can't log a warning message, because if a customer is monitoring the KinesisTap logs using the
            // Timestamp parser, this would lead to an infinite log loop and be very expensive.
            if (this._data is string s) return s;

            // Get a singleton serializer from the SerializationUtility cache.
            // This means we don't have to create a new serializer for each record.
            var serializer = SerializationUtility.XmlSerializers.GetOrAdd(this._data.GetType(), (key) => new XmlSerializer(key));

            using (var stringwriter = new StringWriter())
            {
                serializer.Serialize(stringwriter, this._data);
                return stringwriter.ToString();
            }
        }
    }
}