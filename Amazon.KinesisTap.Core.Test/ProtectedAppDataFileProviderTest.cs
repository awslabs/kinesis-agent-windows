using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Amazon.KinesisTap.Core.Test
{
    [Collection(nameof(ProtectedAppDataFileProviderTest))]
    public class ProtectedAppDataFileProviderTest : IDisposable
    {
        protected readonly ITestOutputHelper _output;
        protected readonly string _testDir = Path.Combine(TestUtility.GetTestHome(), Guid.NewGuid().ToString());

        public ProtectedAppDataFileProviderTest(ITestOutputHelper output)
        {
            _output = output;
            Directory.CreateDirectory(_testDir);
        }

        [Fact]
        public void WriteEnabled_WriteAllText_ShouldWriteToFile()
        {
            const string relativePath = "file.txt";
            const string content = "file content";
            var path = Path.Combine(_testDir, relativePath);

            var provider = new ProtectedAppDataFileProvider(_testDir);
            provider.WriteAllText(relativePath, content, Encoding.UTF8);

            var readContent = File.ReadAllText(path);
            Assert.Equal(content, readContent);
        }

        [Fact]
        public void WriteEnabled_OpenStream_ShouldWriteToFile()
        {
            const string relativePath = "file.txt";
            const string content = "file content";

            var provider = new ProtectedAppDataFileProvider(_testDir);
            using (var stream = provider.OpenFile(relativePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
            {
                WriteContentToStream(stream, content);
            }

            var readContent = File.ReadAllText(Path.Combine(_testDir, relativePath));
            Assert.Equal(content, readContent);
        }

        [Theory]
        [InlineData("dir")]
        [InlineData("dir1/dir2")]
        [InlineData("dir1/dir2/dir3")]
        public void WriteEnabled_CreateDirectory_ShouldCreateDirectory(string relativePath)
        {
            var provider = new ProtectedAppDataFileProvider(_testDir);
            provider.CreateDirectory(relativePath);

            Assert.True(Directory.Exists(Path.Combine(_testDir, relativePath)));
        }

        [Fact]
        public void DisableWrite_ReturnWriteDisabled()
        {
            var provider = new ProtectedAppDataFileProvider(_testDir);
            Assert.True(provider.IsWriteEnabled);

            provider.DisableWrite();
            Assert.False(provider.IsWriteEnabled);
        }

        [Fact]
        public void WriteDisabled_CreateDirectory_ShouldNotCreateDirectory()
        {
            const string relativePath = "dir";
            var provider = new ProtectedAppDataFileProvider(_testDir);

            provider.DisableWrite();

            provider.CreateDirectory(relativePath);
            Assert.False(Directory.Exists(Path.Combine(_testDir, relativePath)));
        }

        [Fact]
        public void WriteDisabled_WriteAllText_ShouldNotWriteToFile()
        {
            const string relativePath = "file.txt";
            const string content = "file content";
            var path = Path.Combine(_testDir, relativePath);

            // create the provider and disable write
            var provider = new ProtectedAppDataFileProvider(_testDir);
            provider.DisableWrite();

            // call WriteAllText and make sure file is not created
            provider.WriteAllText(relativePath, content, Encoding.UTF8);
            Assert.False(File.Exists(path));

            // create an empty file
            File.Create(path).Dispose();

            // call WriteAllText and make sure file nothing is written
            provider.WriteAllText(relativePath, content, Encoding.UTF8);
            Assert.True(string.IsNullOrEmpty(File.ReadAllText(path)));
        }

        [Fact]
        public async Task WriteDisabled_WriteAllBytesAsync_ShouldNotWriteToFile()
        {
            const string relativePath = "file";
            var path = Path.Combine(_testDir, relativePath);

            // randomize content
            var content = new byte[512];
            new Random().NextBytes(content);

            // create the provider and disable write
            var provider = new ProtectedAppDataFileProvider(_testDir);
            provider.DisableWrite();

            // call WriteAllBytesAsync and make sure file is not created
            await provider.WriteAllBytesAsync(relativePath, content, default);
            Assert.False(File.Exists(path));

            // create an empty file
            File.Create(path).Dispose();

            // call WriteAllBytesAsync and make sure file nothing is written
            await provider.WriteAllBytesAsync(relativePath, content, default);
            Assert.True(string.IsNullOrEmpty(File.ReadAllText(path)));
        }

        [Fact]
        public void WriteDisabledWhileStreamOpen_ShouldNotWrite()
        {
            const string relativePath = "file.txt";
            const string content = "file content";
            var path = Path.Combine(_testDir, relativePath);
            var provider = new ProtectedAppDataFileProvider(_testDir);

            // open the file stream
            using var stream = provider.OpenFile(relativePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);

            // write something
            WriteContentToStream(stream, content);

            // disable write while the stream is still open
            provider.DisableWrite();

            // write more content
            WriteContentToStream(stream, "more content");

            // assert that the new content is not written
            var readContent = File.ReadAllText(Path.Combine(_testDir, relativePath));
            Assert.Equal(content, readContent);
        }

        [Theory]
        [InlineData(0, 1)]
        [InlineData(0, 5)]
        [InlineData(2, 0)]
        [InlineData(2, 5)]
        public void WriteEnabled_GetFilesInDirectory_ShouldReturnDirectories(int depth, int fileCount)
        {
            // Get the directory that will contain all the files
            var relativePath = GetDirectoryPathForDepth(depth);
            var containerDir = Path.Combine(_testDir, relativePath);
            Directory.CreateDirectory(containerDir);

            // Get the 'relativeFileNames' list that contains all the relative path of the files
            var relativeFileNames = Enumerable.Range(0, fileCount).Select(i => Path.Combine(relativePath, i.ToString())).ToArray();
            foreach (var relativeFileName in relativeFileNames)
            {
                File.Open(Path.Combine(_testDir, relativeFileName), FileMode.Create).Dispose();
            }

            // Call 'GetFilesInDirectory'
            var provider = new ProtectedAppDataFileProvider(_testDir);
            var files = provider.GetFilesInDirectory(relativePath).OrderBy(f => int.Parse(Path.GetFileName(f))).ToList();

            // Assert that returned list is the same as 'relativeFileNames'
            Assert.Equal(relativeFileNames, files);
        }

        [InlineData(0, 1)]
        [InlineData(0, 5)]
        [InlineData(2, 0)]
        [InlineData(2, 5)]
        [Theory]
        public void WriteDisabled_GetFilesInDirectory_ShouldReturnEmpty(int depth, int fileCount)
        {
            // Get the directory that will contain all the files
            var relativePath = GetDirectoryPathForDepth(depth);
            var containerDir = Path.Combine(_testDir, relativePath);
            Directory.CreateDirectory(containerDir);

            // Get the 'relativeFileNames' list that contains all the relative path of the files
            var relativeFileNames = Enumerable.Range(0, fileCount).Select(i => Path.Combine(relativePath, i.ToString())).ToArray();
            foreach (var relativeFileName in relativeFileNames)
            {
                File.Open(Path.Combine(_testDir, relativeFileName), FileMode.Create).Dispose();
            }

            // Disable writes and call 'GetFilesInDirectory'
            var provider = new ProtectedAppDataFileProvider(_testDir);
            provider.DisableWrite();
            var files = provider.GetFilesInDirectory(relativePath);

            // assert
            Assert.Empty(files);
        }

        [Theory]
        [InlineData(0, true)]
        [InlineData(2, true)]
        [InlineData(0, false)]
        [InlineData(2, false)]
        public void WriteDisabled_FileExists_ShouldReturnFalse(int depth, bool fileActuallyExists)
        {
            // Setup
            var containerDir = GetDirectoryPathForDepth(depth);
            Directory.CreateDirectory(Path.Combine(_testDir, containerDir));
            var filePath = Path.Combine(containerDir, "file");
            if (fileActuallyExists)
            {
                File.Open(Path.Combine(_testDir, filePath), FileMode.Create).Dispose();
            }

            // Call 
            var provider = new ProtectedAppDataFileProvider(_testDir);
            provider.DisableWrite();

            // Assert
            Assert.False(provider.FileExists(filePath));
        }

        [Theory]
        [InlineData(0, true)]
        [InlineData(2, true)]
        [InlineData(0, false)]
        [InlineData(2, false)]
        public void WriteEnabled_FileExists_ShouldReturnFileExistence(int depth, bool fileActuallyExists)
        {
            // Setup
            var containerDir = GetDirectoryPathForDepth(depth);
            Directory.CreateDirectory(Path.Combine(_testDir, containerDir));
            var filePath = Path.Combine(containerDir, "file");
            if (fileActuallyExists)
            {
                File.Open(Path.Combine(_testDir, filePath), FileMode.Create).Dispose();
            }

            // Call 
            var provider = new ProtectedAppDataFileProvider(_testDir);

            // Assert
            Assert.Equal(fileActuallyExists, provider.FileExists(filePath));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(2)]
        public void WriteEnabled_ReadAllText_ShouldReturnText(int depth)
        {
            // setup
            var fileText = Guid.NewGuid().ToString();
            var containerDir = GetDirectoryPathForDepth(depth);
            Directory.CreateDirectory(Path.Combine(_testDir, containerDir));
            var filePath = Path.Combine(containerDir, "file");
            File.WriteAllText(Path.Combine(_testDir, filePath), fileText);

            // call
            var provider = new ProtectedAppDataFileProvider(_testDir);
            var readText = provider.ReadAllText(filePath);

            // assert
            Assert.Equal(fileText, readText);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(2)]
        public void WriteEnabled_ReadAllText_ShouldThrowException(int depth)
        {
            // setup
            var fileText = Guid.NewGuid().ToString();
            var containerDir = GetDirectoryPathForDepth(depth);
            Directory.CreateDirectory(Path.Combine(_testDir, containerDir));
            var filePath = Path.Combine(containerDir, "file");
            File.WriteAllText(Path.Combine(_testDir, filePath), fileText);

            // call
            var provider = new ProtectedAppDataFileProvider(_testDir);
            provider.DisableWrite();

            // assert
            Assert.Throws<FileNotFoundException>(() => provider.ReadAllText(filePath));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void DeleteFile_ShouldDeleteOnlyIfWriteEnabled(bool writeEnabled)
        {
            // setup: create the file
            var fileName = "file";
            File.Open(Path.Combine(_testDir, fileName), FileMode.Create).Dispose();

            // call 'DeleteFile'
            var provider = new ProtectedAppDataFileProvider(_testDir);
            if (!writeEnabled)
            {
                provider.DisableWrite();
            }
            provider.DeleteFile(fileName);

            // assert
            Assert.Equal(!writeEnabled, File.Exists(Path.Combine(_testDir, fileName)));
        }

        private string GetDirectoryPathForDepth(int depth)
        {
            var relativePath = string.Empty;
            for (var i = 0; i < depth; i++)
            {
                relativePath = Path.Combine(relativePath, $"p{i}");
            }

            return relativePath;
        }

        private void WriteContentToStream(Stream stream, string content)
        {
            using (var writer = new StreamWriter(stream))
            {
                writer.Write(content);
                writer.Flush();
            }
        }

        public void Dispose()
        {
            if (Directory.Exists(_testDir))
            {
                Directory.Delete(_testDir, true);
            }
        }
    }
}
