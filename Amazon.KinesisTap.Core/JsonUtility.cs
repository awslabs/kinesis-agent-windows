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
using System.Collections.Generic;
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
