using System;
using System.Collections.Generic;
using System.Text;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Amazon.KinesisTap.Core
{
    public class JsonUtility
    {
        public static string DecorateJson(string json, IDictionary<string, string> attributes)
        {
            JObject jobject = JObject.Parse(json);
            return DecorateJson(jobject, attributes);
        }

        public static string DecorateJson(JObject jobject, IDictionary<string, string> attributes)
        {
            foreach (string key in attributes.Keys)
            {
                jobject.Add(key, attributes[key]);
            }
            return jobject.ToString(Formatting.None);
        }
    }
}
