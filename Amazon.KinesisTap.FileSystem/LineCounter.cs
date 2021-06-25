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
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Filesystem
{
    /// <summary>
    /// Count text lines in text streams.
    /// </summary>
    public class LineCounter : AbstractLineProcessor
    {
        private const int ONE_KILO_BYTE = 1024;
        private const int ONE_MEGA_BYTE = 1024 * 1024;

        /// <summary>
        /// Initialize a <see cref="LineCounter"/> instance.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="encoding"></param>
        public LineCounter(Stream stream, Encoding encoding)
            : base(stream, encoding, GetBlockSize(stream.Length - stream.Position))
        {
        }

        /// <summary>
        /// Count the number of lines in the stream.
        /// </summary>
        /// <param name="stopToken">Stop the async operation.</param>
        /// <returns>Number of lines.</returns>
        public ValueTask<int> CountAllLinesAsync(CancellationToken stopToken = default)
        {
            return CountLinesAsync(long.MaxValue, stopToken);
        }

        /// <summary>
        /// Count the number of lines in the stream up to a position.
        /// </summary>
        /// <param name="endPosition">The end position.</param>
        /// <param name="stopToken">Stop the async operation.</param>
        /// <returns>Number of lines.</returns>
        public async ValueTask<int> CountLinesAsync(long endPosition, CancellationToken stopToken = default)
        {
            var count = 0;
            int lineSize, consumed;
            var position = BaseStream.Position;
            do
            {
                (_, lineSize, consumed) = await ParseAsyncInternal(stopToken);
                position += consumed;
                if (lineSize >= 0 && position <= endPosition)
                {
                    count++;
                }
            } while (lineSize >= 0 && position <= endPosition);

            return count;
        }

        /// <inheritdoc/>
        protected override void DisposeBuffer() => _buffer = null;

        /// <inheritdoc/>
        /// <remarks>
        /// 
        /// </remarks>
        protected override void ResizeBuffer(int size) => Array.Resize(ref _buffer, size);

        /// <summary>
        /// Figure out what is the most efficient block size to use for buffer.
        /// </summary>
        private static int GetBlockSize(long bytesToRead)
        {
            if (bytesToRead < 0)
            {
                return 0;
            }

            // if stream is 1MB or less, use 512KB blocks
            if (bytesToRead <= ONE_MEGA_BYTE)
            {
                return 512 * ONE_KILO_BYTE;
            }

            // else if stream is 128MB or less, use 1MB blocks
            if (bytesToRead <= 128 * ONE_MEGA_BYTE)
            {
                return ONE_MEGA_BYTE;
            }

            // if the stream is larger than 128MB, use 4MB blocks
            return 4 * ONE_MEGA_BYTE;
        }
    }
}
