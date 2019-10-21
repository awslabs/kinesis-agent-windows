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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.AutoUpdate
{
    internal class AutoUpdateServiceHttpClient : IAutoUpdateServiceHttpClient
    {
        private readonly HttpClient http;
        private bool disposed = false;

        public AutoUpdateServiceHttpClient()
        {
            // For generalization: Add proxy handling here
            var handler = new HttpClientHandler();

            // handler.Proxy
            this.http = new HttpClient(handler, true)
            {
                Timeout = TimeSpan.FromMinutes(60)
            };
        }

        // Public implementation of Dispose pattern callable by consumers.
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // Protected implementation of Dispose pattern.
        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
            {
                // Free any other managed objects here.
                this.http?.Dispose();
            }

            disposed = true;
        }

        public async Task<string> SendRequest(HttpRequestMessage request, CancellationToken ct)
        {
            using (var resp = await this.http.SendAsync(request, ct))
            {
                if (resp.IsSuccessStatusCode)
                {
                    return await resp.Content.ReadAsStringAsync();
                }
                else
                {
                    if ((resp.Content.Headers.ContentLength ?? 0) > 0)
                    {
                        var msg = await resp.Content.ReadAsStringAsync();
                        throw new Exception(msg);
                    }
                    else
                    {
                        throw new Exception(resp.ReasonPhrase);
                    }
                }
            }
        }
    }
}