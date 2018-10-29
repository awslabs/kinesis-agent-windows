using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Text;
using System.Reflection;

namespace Amazon.KinesisTap.Core
{
    public class DelimitedLogRecordJsonConverter : JsonConverter
    {
        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            JToken t = JToken.FromObject(value);

            if (t.Type != JTokenType.Object)
            {
                t.WriteTo(writer);
            }
            else
            {
                JObject o = (JObject)t;
                DelimitedLogRecordBase record = value as DelimitedLogRecordBase;
                o.AddFirst(new JProperty("Timestamp", record.TimeStamp));
                o.WriteTo(writer);
            }
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            throw new NotImplementedException("Unnecessary because CanRead is false. The type will skip the converter.");
        }

        public override bool CanRead => false;

        public override bool CanConvert(Type objectType)
        {
            return objectType.GetTypeInfo().IsSubclassOf(typeof(DelimitedLogRecordBase));
        }
    }
}
