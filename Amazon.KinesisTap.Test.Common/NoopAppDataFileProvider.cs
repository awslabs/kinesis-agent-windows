using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.Test.Common
{
    /// <summary>
    /// A file provider that does nothing.
    /// </summary>
    public class NoopAppDataFileProvider : IAppDataFileProvider
    {
        /// <summary>
        /// A file stream that does nothing.
        /// </summary>
        private class NoopStream : Stream
        {
            public override bool CanRead => false;

            public override bool CanSeek => false;

            public override bool CanWrite => false;

            public override long Length => default;

            public override long Position { get => default; set { } }

            public override void Flush() { }

            public override int Read(byte[] buffer, int offset, int count) => default;

            public override long Seek(long offset, SeekOrigin origin) => default;

            public override void SetLength(long value) { }

            public override void Write(byte[] buffer, int offset, int count) { }
        }

        public void CreateDirectory(string path) { }

        public void DisableWrite() { }

        public bool IsWriteEnabled => false;

        public Stream OpenFile(string path, FileMode mode, FileAccess access, FileShare share)
            => new NoopStream();

        public Stream OpenFile(string path, FileMode mode, FileAccess access, FileShare share, int bufferSize, FileOptions fileOptions)
            => new NoopStream();

        public void WriteAllText(string path, string text, Encoding encoding) { }

        public string ReadAllText(string path) => default;

        public bool FileExists(string path) => default;

        public string[] GetFilesInDirectory(string directory) => Array.Empty<string>();

        public void DeleteFile(string path) { }

        public Task WriteAllBytesAsync(string path, byte[] bytes, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken) => Task.FromResult(Array.Empty<byte>());

        public string GetFullPath(string relativePath) => default;
    }
}
