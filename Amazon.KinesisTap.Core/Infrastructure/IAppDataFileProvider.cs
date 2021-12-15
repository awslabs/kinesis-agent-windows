using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Provide access to files in the AppData folder.
    /// </summary>
    public interface IAppDataFileProvider
    {
        /// <summary>
        /// Disable all writes to AppData folder.
        /// </summary>
        void DisableWrite();

        /// <summary>
        /// Determine whether write is being disabled.
        /// </summary>
        bool IsWriteEnabled { get; }

        /// <summary>
        /// Create a directory.
        /// </summary>
        /// <param name="path">Path to directory relative to AppData folder.</param>
        void CreateDirectory(string path);

        /// <summary>
        /// Creates a new file, write the contents to the file, and then closes the file.
        /// If the target file already exists, it is overwritten.
        /// </summary>
        /// <param name="path">The file to write to. MUST be relative to the AppData folder.</param>
        /// <param name="text">Text to write.</param>
        /// <param name="encoding">Encoding to use.</param>
        /// <remarks>
        /// This throws all the exception that <see cref="File.WriteAllText"/> throws.
        /// See https://docs.microsoft.com/en-us/dotnet/api/system.io.file.writealltext
        /// </remarks>
        void WriteAllText(string path, string text, Encoding encoding);

        /// <summary>
        /// Asynchronously creates a new file, writes the specified byte array to the file, and then closes the file.
        /// If the target file already exists, it is overwritten.
        /// </summary>
        /// <param name="path">File path relative to the AppData folder.</param>
        /// <param name="bytes">The bytes to write to the file.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A task that represents the asynchronous write operation.</returns>
        Task WriteAllBytesAsync(string path, byte[] bytes, CancellationToken cancellationToken);

        /// <summary>
        /// Open a file.
        /// </summary>
        /// <param name="path">The file to write to. MUST be relative to the AppData folder.</param>
        /// <param name="mode">Open mode.</param>
        /// <param name="access">File access mode.</param>
        /// <param name="share">File share mode.</param>
        /// <returns>Stream representing the file.</returns>
        Stream OpenFile(string path, FileMode mode, FileAccess access, FileShare share);

        /// <summary>
        /// Open a file.
        /// </summary>
        /// <param name="path">The file to write to. MUST be relative to the AppData folder.</param>
        /// <param name="mode">Open mode.</param>
        /// <param name="access">File access mode.</param>
        /// <param name="share">File share mode.</param>
        /// <param name="bufferSize">Buffer size.</param>
        /// <param name="fileOptions">File options.</param>
        /// <returns></returns>
        Stream OpenFile(string path, FileMode mode, FileAccess access, FileShare share, int bufferSize, FileOptions fileOptions);

        /// <summary>
        /// Opens a text file, reads all the text in the file, and then closes the file.
        /// </summary>
        /// <param name="path">File path relative to the AppData folder.</param>
        /// <returns>A string containing all the text in the file.</returns>
        string ReadAllText(string path);

        /// <summary>
        /// Asynchronously opens a binary file, reads the contents of the file into a byte array, and then closes the file.
        /// </summary>
        /// <param name="path">File path relative to the AppData folder.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>A task that represents the asynchronous read operation.</returns>
        Task<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken);

        /// <summary>
        /// Determines whether the specified file exists.
        /// </summary>
        /// <param name="path">File path relative to the AppData folder.</param>
        /// <returns>True iff the specified file exists.</returns>
        bool FileExists(string path);

        /// <summary>
        /// Delete the specified file.
        /// </summary>
        /// <param name="path">File path relative to AppData directory.</param>
        void DeleteFile(string path);

        /// <summary>
        /// Returns the names of files (including their AppData-RELATIVE paths) in the specified directory.
        /// </summary>
        /// <param name="directory">Directory path relative to the AppData directory.</param>
        /// <returns>Array of file names.</returns>
        string[] GetFilesInDirectory(string directory);

        /// <summary>
        /// Get full path to a directory or file.
        /// </summary>
        /// <param name="relativePath">Relative path to the AppData directory.</param>
        /// <returns>Full path of the file/folder.</returns>
        string GetFullPath(string relativePath);
    }
}
