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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Amazon.KinesisTap.Filesystem.Test
{
    public class LineReaderTest : LineProcessorTestBase, IDisposable
    {
        private readonly string _testFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());

        public void Dispose()
        {
            if (File.Exists(_testFile))
            {
                File.Delete(_testFile);
            }
        }

        /// <summary>
        /// Encode the content in 'utf8samples.txt' with an encoding, then use <see cref="LineReader"/> to read it.
        /// Encoding should be detected automatically from BOM.
        /// </summary>
        [Theory]
        [MemberData(nameof(DetectableEncodings))]
        public async Task SampleWithImplicitEncoding(Encoding encoding)
        {
            var lines = File.ReadAllLines("Samples/utf8samples.txt");

            // write the content with the encoding to a byte stream
            var bytes = WriteToMemory(lines, encoding);

            // use LineReader to read the stream
            var output = new List<string>();
            using (var stream = new MemoryStream(bytes))
            {
                using var reader = new LineReader(stream, null, bytes.Length);
                string line;
                int bytesRead;

                do
                {
                    (line, bytesRead) = await reader.ReadAsync();
                    if (line is null)
                    {
                        break;
                    }
                    output.Add(line);
                } while (line is not null);
            }

            // confirm content match
            Assert.Equal(lines, output);
        }

        /// <summary>
        /// Encode the content in 'utf8samples.txt' with an encoding, then use <see cref="LineReader"/> to read it.
        /// BOM is not present, Encoding is specified by user.
        /// </summary>
        [Theory]
        [MemberData(nameof(AllEncodings))]
        public async Task SampleWithExplicitEncoding(Encoding encoding)
        {
            // write the content with the encoding to a byte stream
            var lines = File.ReadAllLines("Samples/utf8samples.txt");
            var bytes = WriteToMemory(lines, encoding);

            // use LineReader to read the stream
            var output = new List<string>();
            using (var stream = new MemoryStream(bytes))
            {
                using var reader = new LineReader(stream, encoding, bytes.Length);
                string line;
                int bytesRead;

                do
                {
                    (line, bytesRead) = await reader.ReadAsync();
                    if (line is null)
                    {
                        break;
                    }
                    output.Add(line);
                } while (line is not null);
            }

            // confirm content match
            Assert.Equal(lines, output);
        }

        [Theory]
        [MemberData(nameof(AllEncodings))]
        public async Task SampleWithPagedReads(Encoding encoding)
        {
            // write the content with the encoding to a byte stream
            var lines = File.ReadAllLines("Samples/utf8samples.txt");
            var bytes = WriteToMemory(lines, encoding);

            // use LineReader to read the stream
            var output = new List<string>();

            string line;
            int consumed = 0;
            do
            {
                int lineSize;
                using (var stream = new MemoryStream(bytes))
                {
                    stream.Position = consumed;
                    using (var reader = new LineReader(stream, encoding, 1024))
                    {
                        (line, lineSize) = await reader.ReadAsync();
                        if (line != null)
                        {
                            output.Add(line);
                        }
                        consumed += lineSize;
                    }
                }
            } while (line != null);

            // confirm content match
            Assert.Equal(lines, output);
        }

        [MemberData(nameof(TestLines))]
        [Theory]
        public async Task ReturnsCorrectConsumedBytes(string line)
        {
            foreach (var obj in DetectableEncodings)
            {
                var encoding = (Encoding)obj.First();
                var lineSize = encoding.GetBytes(line + Environment.NewLine).Length + encoding.Preamble.Length;

                var bytes = WriteToMemory(new string[] { line }, encoding);
                using var memStream = new MemoryStream(bytes);
                using var reader = new LineReader(memStream);
                var (_, consumed) = await reader.ReadAsync();

                Assert.Equal(lineSize, consumed);
                Assert.Equal(lineSize, memStream.Position);
            }
        }

        /// <summary>
        /// Test for reading lines longer than the initial buffer size of 1024
        /// </summary>
        [Theory]
        [InlineData(10024, 1)]
        [InlineData(10024, 10)]
        [InlineData(10024, 100)]
        public async Task LongLines(int size, int repeats)
        {
            using (var readStream = new FileStream(_testFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
            using (var reader = new LineReader(readStream))
            {
                var text = new string('0', size);
                string line = null;
                var consumed = 0;
                for (var i = 0; i < repeats; i++)
                {
                    File.AppendAllText(_testFile, text);
                    (line, consumed) = await reader.ReadAsync();
                    Assert.Null(line);

                    File.AppendAllText(_testFile, Environment.NewLine);
                    (line, consumed) = await reader.ReadAsync();
                    Assert.Equal(text, line);
                }

                // make sure the internal buffer don't grow indefinitely
                Assert.True(reader.CurrentBufferSize < size * 3);
            }
        }

        [Theory]
        [InlineData(1022, 1024)]
        [InlineData(1023, 1024)]
        [InlineData(1024, 1024)]
        [InlineData(1025, 1025)]
        [InlineData(1022, 1024 * 2)]
        [InlineData(1023, 1024 * 2)]
        [InlineData(1024, 1024 * 2)]
        [InlineData(1025, 1025 * 2)]
        [InlineData(1022, 1024 * 4)]
        [InlineData(1023, 1024 * 4)]
        [InlineData(1024, 1024 * 4)]
        [InlineData(1025, 1025 * 4)]
        public Task ReadEmptyLine_WindowsSequence(int length, int bufSize)
            => ReadEmptyLineWithNewLineSequence(length, bufSize, "\r\n");

        [Theory]
        [InlineData(1022, 1024)]
        [InlineData(1023, 1024)]
        [InlineData(1024, 1024)]
        [InlineData(1025, 1025)]
        [InlineData(1022, 1024 * 2)]
        [InlineData(1023, 1024 * 2)]
        [InlineData(1024, 1024 * 2)]
        [InlineData(1025, 1025 * 2)]
        [InlineData(1022, 1024 * 4)]
        [InlineData(1023, 1024 * 4)]
        [InlineData(1024, 1024 * 4)]
        [InlineData(1025, 1025 * 4)]
        public Task ReadEmptyLine_MacSequence(int length, int bufSize)
           => ReadEmptyLineWithNewLineSequence(length, bufSize, "\r");

        [Theory]
        [InlineData(1022, 1024)]
        [InlineData(1023, 1024)]
        [InlineData(1024, 1024)]
        [InlineData(1025, 1025)]
        [InlineData(1022, 1024 * 2)]
        [InlineData(1023, 1024 * 2)]
        [InlineData(1024, 1024 * 2)]
        [InlineData(1025, 1025 * 2)]
        [InlineData(1022, 1024 * 4)]
        [InlineData(1023, 1024 * 4)]
        [InlineData(1024, 1024 * 4)]
        [InlineData(1025, 1025 * 4)]
        public Task ReadEmptyLine_LinuxSequence(int length, int bufSize)
            => ReadEmptyLineWithNewLineSequence(length, bufSize, "\n");

        [Theory]
        [MemberData(nameof(TestLines))]
        public async Task ReadLineWithInterleavedWrites(string line)
        {
            foreach (var obj in DetectableEncodings)
            {
                var encoding = (Encoding)obj.First();
                await TestReadLineWithInterleavedWrites(line, encoding);
            }
        }

        private async Task TestReadLineWithInterleavedWrites(string line, Encoding encoding)
        {
            if (File.Exists(_testFile))
            {
                File.Delete(_testFile);
            }

            var output = new List<string>();
            using (var readStream = new FileStream(_testFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
            {
                using var reader = new LineReader(readStream);
                var (lineRead, consumed) = await reader.ReadAsync();
                Assert.Null(lineRead);

                // write some text without new line sequence
                File.AppendAllText(_testFile, line, encoding);

                // assert that no line is read yet
                (lineRead, consumed) = await reader.ReadAsync();
                Assert.Null(lineRead);

                // write line feed
                File.AppendAllText(_testFile, Environment.NewLine, encoding);

                // assert line is read
                (lineRead, consumed) = await reader.ReadAsync();
                Assert.True(consumed > 0);
                Assert.Equal(line, lineRead);
            }
        }

        private async Task ReadEmptyLineWithNewLineSequence(int length, int bufSize, string sequence)
        {
            var lines = new string[] {
                new string('a',length),
                string.Empty,
                new string('b',length)
            };

            foreach (var obj in DetectableEncodings)
            {
                var encoding = (Encoding)obj.First();
                var bytes = WriteToMemory(lines, encoding, sequence);

                using var memStream = new MemoryStream(bytes);
                using var reader = new LineReader(memStream, null, bufSize);
                var output = new List<string>();
                for (var i = 0; i < lines.Length; i++)
                {
                    var (l, c) = await reader.ReadAsync();
                    output.Add(l);
                }
                Assert.Equal(lines, output);
            }
        }

        [Fact]
        public void ObjectDisposedReadLine()
        {
            var baseInfo = GetCharArrayStream(Encoding.UTF8);
            var sr = baseInfo.Item2;

            sr.Dispose();
            Assert.ThrowsAsync<ObjectDisposedException>(() => sr.ReadAsync().AsTask());
        }


        [Theory]
        [MemberData(nameof(AllEncodings))]
        public async Task VanillaReadLines(Encoding encoding)
        {
            var baseInfo = GetCharArrayStream(encoding);
            var reader = baseInfo.Item2;
            var valueString = new string(baseInfo.Item1);

            var data = await reader.ReadAsync();
            Assert.Equal(valueString.Substring(0, valueString.IndexOf('\r')), data.Item1);

            data = await reader.ReadAsync();
            Assert.Equal(valueString.Substring(valueString.IndexOf('\r') + 1, 3), data.Item1);

            data = await reader.ReadAsync();
            Assert.Equal(valueString.Substring(valueString.IndexOf('\n') + 1, 2), data.Item1);

            data = await reader.ReadAsync();
            Assert.Null(data.Item1);
        }

        [Fact]
        public async Task VanillaReadLines2()
        {
            var baseInfo = GetCharArrayStream(new UTF8Encoding(false));
            var reader = baseInfo.Item2;
            var valueString = new string(baseInfo.Item1);

            reader.BaseStream.Position = 1;
            var data = await reader.ReadAsync();
            Assert.Equal(valueString.Substring(1, valueString.IndexOf('\r') - 1), data.Item1);
        }

        [Theory]
        [MemberData(nameof(DetectableEncodings))]
        public async Task ContinuousNewLinesAndTabsAsync(Encoding encoding)
        {
            var ms = new MemoryStream();
            var sw = new StreamWriter(ms, encoding);
            sw.Write("\n\n\r\r\n");
            sw.Flush();

            ms.Position = 0;

            var reader = new LineReader(ms, encoding);

            for (var i = 0; i < 4; i++)
            {
                var data = await reader.ReadAsync();
                Assert.Equal(string.Empty, data.Item1);
            }

            var eol = await reader.ReadAsync();
            Assert.Null(eol.Item1);
        }

        protected static (char[], LineReader) GetCharArrayStream(Encoding encoding)
        {
            var chArr = LineReaderTestData.CharData;
            var ms = new MemoryStream();
            var sw = new StreamWriter(ms, encoding);

            for (var i = 0; i < chArr.Length; i++)
            {
                sw.Write(chArr[i]);
            }

            sw.Flush();
            ms.Position = 0;

            return new(chArr, new LineReader(ms, encoding));
        }
    }
}
