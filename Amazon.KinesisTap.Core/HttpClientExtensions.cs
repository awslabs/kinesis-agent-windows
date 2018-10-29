using Newtonsoft.Json;
using System;
using System.Collections.Generic;
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
