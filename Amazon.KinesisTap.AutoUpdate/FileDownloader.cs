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
using System.IO;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.AutoUpdate
{
    /// <summary>
    /// Download File using File class.
    /// Use the Async semantic like other downloaders 
    /// </summary>
    public class FileDownloader : IFileDownloader
    {
        private readonly IAppDataFileProvider _appDataFileProvider;

        public FileDownloader(IAppDataFileProvider appDataFileProvider)
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

            string path = ConvertFileUrlToPath(url);
            using (var streamFrom = File.OpenRead(path))
            using (var streamTo = _appDataFileProvider.OpenFile(toPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
            {
                await streamFrom.CopyToAsync(streamTo);
            }
        }

        /// <inheritdoc/>
        public async Task<string> ReadFileAsStringAsync(string url)
        {
            string path = ConvertFileUrlToPath(url);
            using (var reader = File.OpenText(path))
            {
                return await reader.ReadToEndAsync();
            }
        }

        /// <summary>
        /// Convert the url to path. Don't convert if it is already path
        /// </summary>
        /// <param name="url">Url or Path</param>
        /// <returns>Path</returns>
        public static string ConvertFileUrlToPath(string url)
        {
            if (Uri.TryCreate(url, UriKind.Absolute, out Uri uri))
            {
                return uri.LocalPath;
            }
            else
            {
                return url;
            }
        }
    }
}
