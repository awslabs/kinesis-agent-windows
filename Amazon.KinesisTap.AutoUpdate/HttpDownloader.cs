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
