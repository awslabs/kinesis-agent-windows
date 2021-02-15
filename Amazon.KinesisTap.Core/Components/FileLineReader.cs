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
using System.Diagnostics;
using System.IO;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Class used to read lines from text files.
    /// </summary>
    internal class FileLineReader
    {
        private const byte LineFeedByte = (byte)'\n';
        private const byte CarriageReturnByte = (byte)'\r';

        // make this internal so we can unit-test cases with buffer size boundaries
        internal const int MinimumBufferSize = 1024;

        private byte[] _buffer = new byte[MinimumBufferSize * 2];

        private int _pos = 0;
        private int _len = 0;

        // for testing purpose
        internal int InternalBufferSize => _buffer.Length;

        /// <summary>
        /// Read the next text line from the stream.
        /// </summary>
        /// <param name="stream">The source stream.</param>
        /// <param name="encoding">The stream's encoding.</param>
        /// <returns>The next line in the stream, or 'null' if the end of the stream is reached.</returns>
        /// <remarks>
        /// Unlike <see cref="StreamReader.ReadLine"/>, this method returns a string only if it is terminated with a new line sequence.
        /// The class maintains internal buffer, and the <paramref name="stream"/>'s position will be updated as the class reads data.
        /// Therefore it is important that the class's user maintains the position of the <paramref name="stream"/> between calls to
        /// <see cref="ReadLine"/> to avoid corrupting the state of the reader.
        /// </remarks>
        public string ReadLine(Stream stream, Encoding encoding)
        {
            // return any line still in the buffer
            var line = ParseLineFromBuffer(encoding);
            if (line != null)
            {
                return line;
            }

            // read the stream into the buffer until we find a new line 
            int bytesRead;
            do
            {
                var startIdx = _pos + _len;
                // resize the buffer if neccessary
                if (_buffer.Length - startIdx < MinimumBufferSize)
                {
                    Array.Resize(ref _buffer, _buffer.Length * 2);
                }

                bytesRead = stream.Read(_buffer, startIdx, MinimumBufferSize);
                _len += bytesRead;

                line = ParseLineFromBuffer(encoding);
                if (line != null)
                {
                    return line;
                }
            } while (bytesRead != 0);

            return null;
        }

        /// <summary>
        /// Discard the internal buffer and reset the reader to the initial state.
        /// </summary>
        public void Reset()
        {
            _pos = 0;
            _len = 0;
        }

        /// <summary>
        /// Look at the internal buffer [_pos, _pos+ _len), parse a complete line, then update the state.
        /// </summary>
        /// <returns>A complete line (excluding the new line sequence) or 'null' if no line is detected.</returns>
        private string ParseLineFromBuffer(Encoding encoding)
        {
            if (_len == 0)
            {
                return null;
            }

            var newLineIdx = Array.FindIndex(_buffer, _pos, _len, c => c == LineFeedByte || c == CarriageReturnByte);

            if (newLineIdx < 0)
            {
                return null;
            }

            // found a new line character
            // form the string
            var str = newLineIdx == _pos
            ? string.Empty
            : encoding.GetString(_buffer, _pos, newLineIdx - _pos);

            // skip over all possible newline sequences
            switch (_buffer[newLineIdx])
            {
                case CarriageReturnByte:
                    if (newLineIdx + 1 < _pos + _len && _buffer[newLineIdx + 1] == LineFeedByte)
                    {
                        // DOS
                        newLineIdx++;
                    }
                    // OSX
                    break;
                default:
                    // UNIX ('\n')
                    break;
            }

            _len -= newLineIdx - _pos + 1;
            _pos = newLineIdx + 1;
            RealignBuffer();
            return str;
        }

        /// <summary>
        /// If _pos is at the second half of the buffer, move the current data to the buffer's beginning.
        /// </summary>
        private void RealignBuffer()
        {
            if (_pos < _buffer.Length / 2)
            {
                return;
            }

            Debug.Assert(_len < _buffer.Length / 2);
            Array.Copy(_buffer, _pos, _buffer, 0, _len);
            _pos = 0;
        }
    }
}
