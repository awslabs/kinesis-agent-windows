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
using System.Buffers;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Filesystem
{
    /// <summary>
    /// Used for reading complete text lines.
    /// </summary>
    public class LineReader : AbstractLineProcessor
    {
        /// <summary>
        /// Initialize new LineReader instance.
        /// </summary>
        /// <param name="stream">Stream to read data from.</param>
        public LineReader(Stream stream) : this(stream, null)
        {
        }

        /// <summary>
        /// Initialize new LineReader instance.
        /// </summary>
        /// <param name="stream">Stream to read data from.</param>
        /// <param name="encoding">Text encoding of the stream.</param>
        public LineReader(Stream stream, Encoding encoding) : this(stream, encoding, 1024)
        {
        }

        /// <summary>
        /// Initialize new LineReader instance.
        /// </summary>
        /// <param name="stream">Stream to read data from.</param>
        /// <param name="encoding">Text encoding of the stream.</param>
        /// <param name="blockSize">Size used to initialize internal buffer.</param>
        public LineReader(Stream stream, Encoding encoding, int blockSize) : base(stream, encoding, blockSize)
        {
        }

        /// <summary>
        /// Read the next complete line in a stream.
        /// </summary>
        /// <param name="stopToken">Used to stop the read operation.</param>
        /// <returns>Tuple containing the parsed line and the amount of bytes consumed from the stream.</returns>
        public async ValueTask<(string, int)> ReadAsync(CancellationToken stopToken = default)
        {
            var (lineStartIdx, lineSize, consumed) = await ParseAsyncInternal(stopToken);

            if (lineSize < 0)
            {
                return (null, consumed);
            }

            var line = lineSize == 0
                ? string.Empty
                : (CurrentEncoding ?? Encoding.UTF8).GetString(_buffer, lineStartIdx, lineSize);
            return (line, consumed);
        }

        /// <inheritdoc/>
        protected override void ResizeBuffer(int size)
        {
            var newBuffer = ArrayPool<byte>.Shared.Rent(size);

            if (_buffer is not null)
            {
                _buffer.AsSpan().CopyTo(newBuffer);
                ArrayPool<byte>.Shared.Return(_buffer);
            }

            _buffer = newBuffer;
        }

        /// <inheritdoc/>
        protected override void DisposeBuffer()
        {
            ArrayPool<byte>.Shared.Return(_buffer);
        }
    }
}
