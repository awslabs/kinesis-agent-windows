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
using System.Text;
using Xunit;

namespace Amazon.KinesisTap.Core.Test
{
    public class FileLineReaderTest
    {
        private readonly Encoding _encoding = Encoding.UTF8;

        /// <summary>
        /// Test situations where a line is not written completely at first.
        /// </summary>
        [Fact]
        public void InterleavedWrites()
        {
            var testFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            try
            {
                using (var readStream = new FileStream(testFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
                {
                    var reader = new FileLineReader();
                    Assert.Null(reader.ReadLine(readStream, _encoding));

                    // write some text without new line sequence
                    File.AppendAllText(testFile, new string('*', FileLineReader.MinimumBufferSize - 1));

                    // assert that no line is read yet
                    Assert.Null(reader.ReadLine(readStream, _encoding));

                    // write line feed
                    File.AppendAllText(testFile, "\n");

                    // assert that the line is read
                    Assert.Equal(FileLineReader.MinimumBufferSize - 1, reader.ReadLine(readStream, _encoding).Length);
                }
            }
            finally
            {
                File.Delete(testFile);
            }
        }

        /// <summary>
        /// Test that the reader recognizes new line sequences
        /// </summary>
        /// <param name="newlineSequence"></param>
        [Theory]
        [InlineData("\r")]
        [InlineData("\n")]
        [InlineData("\r\n")]
        public void EmptyLines(string newlineSequence)
        {
            var testFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            var startString = new string('a', FileLineReader.MinimumBufferSize);
            var endString = new string('b', FileLineReader.MinimumBufferSize);
            File.AppendAllText(testFile, startString);
            try
            {
                using (var readStream = new FileStream(testFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
                {
                    var reader = new FileLineReader();
                    Assert.Null(reader.ReadLine(readStream, _encoding));
                    File.AppendAllText(testFile, newlineSequence);
                    Assert.Equal(startString, reader.ReadLine(readStream, _encoding));

                    File.AppendAllText(testFile, newlineSequence);
                    Assert.Equal(string.Empty, reader.ReadLine(readStream, _encoding));

                    File.AppendAllText(testFile, newlineSequence);
                    Assert.Equal(string.Empty, reader.ReadLine(readStream, _encoding));

                    File.AppendAllText(testFile, endString);
                    File.AppendAllText(testFile, newlineSequence);
                    Assert.Equal(endString, reader.ReadLine(readStream, _encoding));
                }
            }
            finally
            {
                File.Delete(testFile);
            }
        }

        /// <summary>
        /// Reading multiple consecutive lines in a file.
        /// </summary>
        [Theory]
        [InlineData('a', 2, 10)]
        [InlineData('b', FileLineReader.MinimumBufferSize - 1, 10)]
        [InlineData('c', FileLineReader.MinimumBufferSize, 10)]
        [InlineData('d', FileLineReader.MinimumBufferSize + 1, 200)]
        [InlineData('d', FileLineReader.MinimumBufferSize * 8, 1000)]
        public void MultipleLines(char c, int size, int count)
        {
            var testFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());

            // write all lines to the file
            for (var i = 0; i < count; i++)
            {
                File.AppendAllText(testFile, new string(c, size));
                File.AppendAllText(testFile, Environment.NewLine);
            }
            try
            {
                using (var readStream = new FileStream(testFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
                {
                    var reader = new FileLineReader();
                    for (var i = 0; i < count; i++)
                    {
                        var line = reader.ReadLine(readStream, _encoding);
                        if (string.Empty == line)
                        {
                            // in the first test case (a,1023,10), due to the internal buffer size, the sequence \r\n might be broken up
                            // so we just ignore the return values with an empty line
                            continue;
                        }
                        Assert.Equal(new string(c, size), line);
                    }
                }
            }
            finally
            {
                File.Delete(testFile);
            }
        }

        /// <summary>
        /// Test for reading lines longer than the initial buffer size of 1024
        /// </summary>
        [Theory]
        [InlineData(1023)]
        [InlineData(1024)]
        [InlineData(1025)]
        [InlineData(10024)]
        public void LongLines(int size)
        {
            var testFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            var reader = new FileLineReader();

            try
            {
                using (var readStream = new FileStream(testFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
                {
                    for (var i = 0; i < 10; i++)
                    {
                        File.AppendAllText(testFile, new string((char)(i + '0'), size));
                        Assert.Null(reader.ReadLine(readStream, _encoding));

                        File.AppendAllText(testFile, Environment.NewLine);
                        var line = reader.ReadLine(readStream, _encoding);
                        Assert.Equal(size, line.Length);
                    }
                }
            }
            finally
            {
                File.Delete(testFile);
            }
        }

        /// <summary>
        /// Make sure we don't just grow the internal buffer indefinitely as data is read.
        /// </summary>
        [Theory]
        [InlineData(FileLineReader.MinimumBufferSize - 2, 10)]
        [InlineData(FileLineReader.MinimumBufferSize + 2, 10)]
        [InlineData(FileLineReader.MinimumBufferSize * 2, 1000)]
        public void BufferIsRealigned(int lineSize, int count)
        {
            var random = new Random();
            var testFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            var reader = new FileLineReader();

            try
            {
                using (var readStream = new FileStream(testFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
                {
                    for (var i = 0; i < count; i++)
                    {
                        // write a random string that contains an ASCII
                        var s = new string((char)(33 + random.Next(90)), lineSize);
                        File.AppendAllText(testFile, s);
                        File.AppendAllText(testFile, Environment.NewLine);

                        // read the line
                        Assert.Equal(s, reader.ReadLine(readStream, _encoding));

                        // make sure that the internal buffer grows as much as twice the record size
                        Assert.True(reader.InternalBufferSize <= lineSize * 3,
                            $"Internal buffer size is too large: {reader.InternalBufferSize}");
                    }
                }
            }
            finally
            {
                File.Delete(testFile);
            }
        }

        /// <summary>
        /// Resetting the reader should refresh the state.
        /// </summary>
        [Fact]
        public void ResetReader()
        {
            const string testLine = nameof(testLine);
            var testFile = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            var reader = new FileLineReader();

            try
            {
                using (var readStream = new FileStream(testFile, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite))
                {
                    reader.ReadLine(readStream, _encoding);

                    // write a test line without new line to make the buffer contain data
                    File.AppendAllText(testFile, testLine);
                    Assert.Null(reader.ReadLine(readStream, _encoding));

                    // reset
                    readStream.Seek(0, SeekOrigin.Begin);
                    reader.Reset();

                    // finish the line and assert
                    File.AppendAllText(testFile, Environment.NewLine);
                    Assert.Equal(testLine, reader.ReadLine(readStream, _encoding));
                }
            }
            finally
            {
                File.Delete(testFile);
            }
        }
    }
}
