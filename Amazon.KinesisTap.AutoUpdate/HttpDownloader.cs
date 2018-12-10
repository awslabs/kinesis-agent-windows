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
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.AutoUpdate
{
    //Download file using HTTP
    public class HttpDownloader : IFileDownloader
    {
        public HttpDownloader()
        {
        }

        public async Task DownloadFileAsync(string url, string toPath)
        {
            using (var streamTo = File.Open(toPath, FileMode.Create))
            using (HttpClient httpClient = new HttpClient())
            using (var response = await httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead))
            {
                response.EnsureSuccessStatusCode();
                using (var streamFrom = await response.Content.ReadAsStreamAsync())
                {
                    await streamFrom.CopyToAsync(streamTo);
                }
            }
        }

        public async Task<string> ReadFileAsStringAsync(string url)
        {
            using (HttpClient httpClient = new HttpClient())
            using (var response = await httpClient.GetAsync(url))
            {
                response.EnsureSuccessStatusCode();

                return await response.Content.ReadAsStringAsync();
            }
        }
    }
}
