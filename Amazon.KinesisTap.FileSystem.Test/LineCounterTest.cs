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
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Amazon.KinesisTap.Filesystem.Test
{
    public class LineCounterTest : LineProcessorTestBase
    {
        /// <summary>
        /// Encode the content in 'utf8samples.txt' with an encoding, then use <see cref="LineCounter"/> to count it.
        /// Encoding should be detected automatically from BOM.
        /// </summary>
        [Theory]
        [MemberData(nameof(DetectableEncodings))]
        public async Task SampleWithImplicitEncoding(Encoding encoding)
        {
            var lines = File.ReadAllLines("Samples/utf8samples.txt");

            // write the content with the encoding to a byte stream
            var bytes = WriteToMemory(lines, encoding);
            using var stream = new MemoryStream(bytes);

            // create the counter with encoding set to 'null' for auto-detection
            using var counter = new LineCounter(stream, null);
            var count = await counter.CountAllLinesAsync();

            // confirm line count match
            Assert.Equal(lines.Length, count);
        }

        /// <summary>
        /// Encode the content in 'utf8samples.txt' with an encoding, then use <see cref="LineCounter"/> to count it.
        /// BOM is not present, Encoding is specified by user.
        /// </summary>
        [Theory]
        [MemberData(nameof(AllEncodings))]
        public async Task SampleWithExplicitEncoding(Encoding encoding)
        {
            var lines = File.ReadAllLines("Samples/utf8samples.txt");

            // write the content with the encoding to a byte stream
            var bytes = WriteToMemory(lines, encoding);
            using var stream = new MemoryStream(bytes);

            // create the counter with the specified encoding
            using var counter = new LineCounter(stream, encoding);
            var count = await counter.CountAllLinesAsync();

            // confirm content match
            Assert.Equal(lines.Length, count);
        }

        /// <summary>
        /// Test counting lines up to a position.
        /// </summary>
        [Theory]
        [InlineData(0, 5)]
        [InlineData(5, 0)]
        [InlineData(10, 11)]
        public async Task CountToPosition(int linesBefore, int linesAfter)
        {
            var texts = new string[]
            {
                LineReaderTestData.FormatStringOneObject,
                LineReaderTestData.FormatStringTwoObjects,
                LineReaderTestData.FormatStringThreeObjects,
                LineReaderTestData.FormatStringMultipleObjects
            };
            long position = 0;
            var j = 0;
            byte[] data;

            using (var memStream = new MemoryStream())
            using (var writer = new StreamWriter(memStream))
            {
                for (var i = 0; i < linesBefore; i++)
                {
                    await writer.WriteLineAsync(texts[j++ % texts.Length]);
                }

                // remember the stream position at this point
                await writer.FlushAsync();
                position = memStream.Position;

                for (var i = 0; i < linesAfter; i++)
                {
                    await writer.WriteLineAsync(texts[j++ % texts.Length]);
                }
                await writer.FlushAsync();

                data = memStream.ToArray();
            }

            using var readStream = new MemoryStream(data);
            using var counter = new LineCounter(readStream, null);

            var count = await counter.CountLinesAsync(position);
            Assert.Equal(linesBefore, count);
        }
    }
}
