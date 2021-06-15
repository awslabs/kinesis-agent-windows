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
using Newtonsoft.Json;
using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Core
{
    public static class HttpClientExtensions
    {
        public static async Task<HttpResponseMessage> PutAsJsonAsync(this HttpClient httpClient, string requestUri, object data)
        {
            StringContent stringContent = GetStringContent(data);

            return await httpClient.PutAsync(requestUri, stringContent);
        }

        public static async Task<HttpResponseMessage> PostAsJsonAsync(this HttpClient httpClient, string requestUri, object data)
        {
            StringContent stringContent = GetStringContent(data);

            return await httpClient.PostAsync(requestUri, stringContent);
        }

        public static StringContent GetStringContent(object data)
        {
            if (data == null)
            {
                throw new ArgumentException("Cannot put null!");
            }

            var json = JsonConvert.SerializeObject(data) + ConfigConstants.NEWLINE;

            var stringContent = new StringContent(json,
                         Encoding.UTF8,
                         "application/json");
            return stringContent;
        }
    }
}
