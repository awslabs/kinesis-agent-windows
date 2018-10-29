using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.AutoUpdate
{
    /// <summary>
    /// Download File using File class.
    /// Use the Async semantic like other downloaders 
    /// </summary>
    public class FileDownloader : IFileDownloader
    {
        public async Task DownloadFileAsync(string url, string toPath)
        {
            string path = ConvertFileUrlToPath(url);
            using (var streamTo = File.Open(toPath, FileMode.Create))
            using (var streamFrom = File.OpenRead(path))
            {
                await streamFrom.CopyToAsync(streamTo);
            }
        }

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
