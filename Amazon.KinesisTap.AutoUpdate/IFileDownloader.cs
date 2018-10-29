using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.AutoUpdate
{
    /// <summary>
    /// Interface for the file downloader
    /// </summary>
    public interface IFileDownloader
    {
        //Read a file from an url as a string
        Task<string> ReadFileAsStringAsync(string url);

        //Download a file to a path
        Task DownloadFileAsync(string url, string path);
    }
}
