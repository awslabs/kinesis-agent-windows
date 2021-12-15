using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Provide controlled access to files in AppData folder.
    /// </summary>
    public class ProtectedAppDataFileProvider : IAppDataFileProvider
    {
        #region FileStreamWrapper
        /// <summary>
        /// Provide a wrapper class for <see cref="FileStream"/> to conditionally disable writes.
        /// </summary>
        private sealed class FileStreamWrapper : Stream
        {
            private readonly IAppDataFileProvider _fileProvider;
            private readonly FileStream _stream;

            public FileStreamWrapper(IAppDataFileProvider fileProvider,
                string path, FileMode mode, FileAccess access, FileShare share)
                : this(fileProvider, ShouldCreateFileStream(fileProvider, access, FileOptions.None)
                    ? new FileStream(path, mode, access, share)
                    : null)
            {
            }

            public FileStreamWrapper(IAppDataFileProvider fileProvider,
                string path, FileMode mode, FileAccess access, FileShare share, int bufferSize, FileOptions options)
                : this(fileProvider, ShouldCreateFileStream(fileProvider, access, options)
                      ? new FileStream(path, mode, access, share, bufferSize, options)
                      : null)
            {
            }

            private FileStreamWrapper(IAppDataFileProvider fileProvider, FileStream stream)
            {
                _fileProvider = fileProvider;
                _stream = stream;
            }

            private static bool ShouldCreateFileStream(IAppDataFileProvider fileProvider, FileAccess fileAccess, FileOptions options)
            {
                if (fileProvider.IsWriteEnabled)
                {
                    return true;
                }

                if (fileAccess.HasFlag(FileAccess.Write))
                {
                    return false;
                }

                if (options.HasFlag(FileOptions.DeleteOnClose))
                {
                    return false;
                }

                return true;
            }

            /// <inheritdoc/>
            public override bool CanRead => _stream?.CanRead ?? false;

            /// <inheritdoc/>
            public override bool CanSeek => _stream?.CanSeek ?? false;

            /// <inheritdoc/>
            /// <remarks>
            /// The reason why this is always true is because KT is always supposed to be able to write to AppData directory.
            /// If write is disabled because we detect a junction, all write operations will fail silently to avoid breaking any other components.
            /// </remarks>
            public override bool CanWrite => true;

            /// <inheritdoc/>
            public override long Length => _stream?.Length ?? default;

            /// <inheritdoc/>
            public override long Position
            {
                get => _stream?.Position ?? default;
                set
                {
                    if (_stream is not null)
                    {
                        _stream.Position = value;
                    }
                }
            }

            /// <inheritdoc/>
            public override void Flush()
            {
                if (_fileProvider.IsWriteEnabled)
                {
                    _stream?.Flush();
                }
            }

            /// <inheritdoc/>
            public override int Read(byte[] buffer, int offset, int count) => _stream?.Read(buffer, offset, count) ?? default;

            /// <inheritdoc/>
            public override long Seek(long offset, SeekOrigin origin) => _stream?.Seek(offset, origin) ?? default;

            /// <inheritdoc/>
            public override void SetLength(long value)
            {
                if (_fileProvider.IsWriteEnabled)
                {
                    _stream?.SetLength(value);
                }
            }

            /// <inheritdoc/>
            public override void Write(byte[] buffer, int offset, int count)
            {
                if (_fileProvider.IsWriteEnabled)
                {
                    _stream?.Write(buffer, offset, count);
                }
            }

            // Implement disposable pattern
            private bool _disposed;
            protected override void Dispose(bool disposing)
            {
                if (!_disposed)
                {
                    if (disposing)
                    {
                        _stream?.Dispose();
                    }

                    _disposed = true;
                }

                base.Dispose(disposing);
            }
        }
        #endregion

        /// <summary>
        /// 1 if write is disabled, 0 otherwise.
        /// </summary>
        private int _writeDisabled = 0;
        private readonly string _appDataDir;

        public ProtectedAppDataFileProvider(string appDataDir)
        {
            if (appDataDir is null)
            {
                throw new ArgumentNullException(nameof(appDataDir));
            }
            _appDataDir = appDataDir;
        }

        /// <inheritdoc/>
        public void DisableWrite() => Interlocked.Exchange(ref _writeDisabled, 1);

        /// <inheritdoc/>
        public Stream OpenFile(string path, FileMode mode, FileAccess access, FileShare share)
            => new FileStreamWrapper(this, Path.Combine(_appDataDir, path), mode, access, share);

        public Stream OpenFile(string path, FileMode mode, FileAccess access, FileShare share, int bufferSize, FileOptions fileOptions)
            => new FileStreamWrapper(this, Path.Combine(_appDataDir, path), mode, access, share, bufferSize, fileOptions);

        /// <inheritdoc/>
        public void CreateDirectory(string path)
        {
            if (!IsWriteEnabled)
            {
                return;
            }

            Directory.CreateDirectory(Path.Combine(_appDataDir, path));
        }

        /// <inheritdoc/>
        public void WriteAllText(string path, string text, Encoding encoding)
        {
            if (!IsWriteEnabled)
            {
                return;
            }

            File.WriteAllText(Path.Combine(_appDataDir, path), text, encoding);
        }

        /// <inheritdoc/>
        public string ReadAllText(string path) => IsWriteEnabled
                ? File.ReadAllText(Path.Combine(_appDataDir, path))
                : throw new FileNotFoundException("Cannot find suitable file to read", path);

        /// <inheritdoc/>
        public bool FileExists(string path) => IsWriteEnabled && File.Exists(Path.Combine(_appDataDir, path));

        /// <inheritdoc/>
        public string[] GetFilesInDirectory(string directory)
        {
            if (!IsWriteEnabled)
            {
                return Array.Empty<string>();
            }

            return Directory
                .GetFiles(Path.Combine(_appDataDir, directory))
                .Select(f => Path.GetRelativePath(_appDataDir, f))
                .ToArray();
        }

        /// <inheritdoc/>
        public void DeleteFile(string path)
        {
            if (!IsWriteEnabled)
            {
                return;
            }

            File.Delete(Path.Combine(_appDataDir, path));
        }

        /// <inheritdoc/>
        public Task WriteAllBytesAsync(string path, byte[] bytes, CancellationToken cancellationToken)
            => IsWriteEnabled
            ? File.WriteAllBytesAsync(Path.Combine(_appDataDir, path), bytes, cancellationToken)
            : Task.CompletedTask;

        /// <inheritdoc/>
        public Task<byte[]> ReadAllBytesAsync(string path, CancellationToken cancellationToken)
            => IsWriteEnabled
            ? File.ReadAllBytesAsync(Path.Combine(_appDataDir, path), cancellationToken)
            : Task.FromResult(Array.Empty<byte>());

        /// <inheritdoc/>
        public string GetFullPath(string relativePath) => Path.Combine(_appDataDir, relativePath);

        /// <inheritdoc/>
        public bool IsWriteEnabled => _writeDisabled == 0;
    }
}
