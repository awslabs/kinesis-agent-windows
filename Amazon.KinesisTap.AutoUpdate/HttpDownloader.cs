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
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.AutoUpdate
{
    //Download file using HTTP
    public class HttpDownloader : IFileDownloader
    {
        private readonly IAppDataFileProvider _appDataFileProvider;

        public HttpDownloader(IAppDataFileProvider appDataFileProvider)
        {
            Guard.ArgumentNotNull(appDataFileProvider, nameof(appDataFileProvider));
            _appDataFileProvider = appDataFileProvider;
        }

        /// <inheritdoc/>
        public async Task DownloadFileAsync(string url, string toPath)
        {
            if (!_appDataFileProvider.IsWriteEnabled)
            {
                return;
            }

            using (HttpClient httpClient = new HttpClient())
            using (var response = await httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead))
            {
                response.EnsureSuccessStatusCode();
                using (var streamFrom = await response.Content.ReadAsStreamAsync())
                using (var streamTo = _appDataFileProvider.OpenFile(toPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
                {
                    await streamFrom.CopyToAsync(streamTo);
                }
            }
        }

        /// <inheritdoc/>
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
