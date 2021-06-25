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
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Filesystem
{
    /// <summary>
    /// Base class for processing lines (i.e. text that ends with new-line sequence) from text streams.
    /// </summary>
    public abstract class AbstractLineProcessor : IDisposable
    {
        private const byte LineFeedByte = (byte)'\n';
        private const byte CarriageReturnByte = (byte)'\r';
        private static readonly byte[] _utf8LineSeparators = new byte[] { LineFeedByte, CarriageReturnByte };

        private static readonly int _utf32BeLF = BitConverter.ToInt32(new byte[] { 0x00, 0x00, 0x00, 0x0a });
        private static readonly int _utf32BeCR = BitConverter.ToInt32(new byte[] { 0x00, 0x00, 0x00, 0x0d });
        private static readonly int[] _utf32BeLineSeparators = new int[] { _utf32BeLF, _utf32BeCR };

        private static readonly int _utf32LeLF = BitConverter.ToInt32(new byte[] { 0x0a, 0x00, 0x00, 0x00 });
        private static readonly int _utf32LeCR = BitConverter.ToInt32(new byte[] { 0x0d, 0x00, 0x00, 0x00 });
        private static readonly int[] _utf32LeLineSeparators = new int[] { _utf32LeLF, _utf32LeCR };

        private static readonly short _utf16BeLF = BitConverter.ToInt16(new byte[] { 0x00, 0x0a });
        private static readonly short _utf16BeCR = BitConverter.ToInt16(new byte[] { 0x00, 0x0d });
        private static readonly short[] _utf16BeLineSeparators = new short[] { _utf16BeLF, _utf16BeCR };

        private static readonly short _utf16LeLF = BitConverter.ToInt16(new byte[] { 0x0a, 0x00 });
        private static readonly short _utf16LeCR = BitConverter.ToInt16(new byte[] { 0x0d, 0x00 });
        private static readonly short[] _utf16LeLineSeparators = new short[] { _utf16LeLF, _utf16LeCR };

        private readonly Stream _stream;
        private readonly int _blockSize;
        protected byte[] _buffer;

        private int _startPos;
        private int _len;

        private bool _isBigEndian = false;
        private bool _disposed;

        /// <summary>
        /// Initialize new <see cref="AbstractLineProcessor"/> instance.
        /// </summary>
        /// <param name="stream">Stream to read data from.</param>
        /// <param name="encoding">Text encoding of the stream.</param>
        /// <param name="blockSize">Size used to initialize internal buffer.</param>
        protected AbstractLineProcessor(Stream stream, Encoding encoding, int blockSize)
        {
            if (stream is null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            if (!stream.CanRead || !stream.CanSeek)
            {
                throw new ArgumentException("Stream must be seek-able and read-able", nameof(stream));
            }

            CurrentEncoding = encoding;
            _stream = stream;
            _blockSize = blockSize;
            ResizeBuffer(_blockSize * 2);

            DetermineUnitSizeAndEndianess();
        }

        /// <summary>
        /// Get the current encoding of the stream.
        /// </summary>
        public Encoding CurrentEncoding { get; private set; } = null;

        /// <summary>
        /// Get the current buffer size.
        /// </summary>
        public int CurrentBufferSize => _buffer.Length;

        /// <summary>
        /// Get the Encoding unit size.
        /// </summary>
        public int UnitSize { get; private set; } = 1;

        /// <summary>
        /// Get the stream being processed.
        /// </summary>
        public Stream BaseStream => _stream;

        /// <summary>
        /// Parse the next line in the stream and move the current position forward.
        /// </summary>
        /// <param name="stopToken">Stop the async operation.</param>
        /// <returns>
        /// 'lineStartIdx' points to the starting index of the line in <see cref="_buffer"/>,
        /// 'lineSize' is the size of the line in bytes, or -1 when EOF is reached,
        /// 'consumed' is the number of bytes the method has consumed.
        /// </returns>
        protected async ValueTask<(int lineStartIdx, int lineSize, int consumed)> ParseAsyncInternal(CancellationToken stopToken = default)
        {
            var initialStreamPosition = _stream.Position;
            var readNextResult = ReadNextLine(out var consumed);
            _startPos += consumed;
            _len -= consumed;

            if (readNextResult.lineSize >= 0)
            {
                return (readNextResult.lineStartIdx, readNextResult.lineSize, consumed);
            }

            int bytesRead;
            do
            {
                AdjustBuffer();
                bytesRead = await _stream.ReadAsync(_buffer.AsMemory(_startPos + _len, _blockSize), stopToken);
                _len += bytesRead;

                if (bytesRead == 0)
                {
                    break;
                }

                int consumedBytes;
                if (initialStreamPosition == 0)
                {
                    if (CurrentEncoding is null)
                    {
                        CurrentEncoding = DetectEncoding(out consumedBytes);
                        // continue even if encoding is not detected, in this case we assume UTF-8
                        consumed += consumedBytes;
                        _startPos += consumedBytes;
                        _len -= consumedBytes;
                        DetermineUnitSizeAndEndianess();
                    }
                    else
                    {
                        // this means encoding is dictated by user
                        var detectedEncoding = DetectEncoding(out consumedBytes);
                        if (detectedEncoding == null)
                        {
                            // no encoding found yet, try reading later
                            break;
                        }
                        if (consumedBytes > 0)
                        {
                            // this means we have found the preamble,
                            // override the user-specified encoding
                            CurrentEncoding = detectedEncoding;
                            consumed += consumedBytes;
                            _startPos += consumedBytes;
                            _len -= consumedBytes;
                            DetermineUnitSizeAndEndianess();
                        }
                    }
                }

                readNextResult = ReadNextLine(out consumedBytes);
                _startPos += consumedBytes;
                _len -= consumedBytes;
                consumed += consumedBytes;

                if (readNextResult.lineSize >= 0)
                {
                    return (readNextResult.lineStartIdx, readNextResult.lineSize, consumed);
                }

            } while (bytesRead > 0);

            return (_startPos, -1, consumed);
        }

        /// <summary>
        /// When implemented, resize the <see cref="_buffer"/> to specified size.
        /// After the method returns, the data in the old buffer must be copied over to the beginning of the new buffer.
        /// </summary>
        /// <param name="size">New buffer size.</param>
        protected abstract void ResizeBuffer(int size);

        /// <summary>
        /// When implemented, dispose the <see cref="_buffer"/>.
        /// </summary>
        protected abstract void DisposeBuffer();

        private void DetermineUnitSizeAndEndianess()
        {
            Span<byte> span = stackalloc byte[4];
            if (CurrentEncoding is null)
            {
                // unknown encoding, assume UTF-8
                UnitSize = 1;
                _isBigEndian = false;
                return;
            }

            switch (CurrentEncoding)
            {
                default:
                    throw new Exception($"Unsupported encoding type {CurrentEncoding.GetType()}");
                case UTF7Encoding utf7Encoding:
                case UTF8Encoding utf8Encoding:
                case ASCIIEncoding aSCIIEncoding:
                    _isBigEndian = false;
                    UnitSize = 1;
                    break;
                case UTF32Encoding utf32Encoding:
                    CurrentEncoding.GetBytes("a", span);
                    _isBigEndian = span[0] == 0;
                    UnitSize = 4;
                    break;
                case UnicodeEncoding unicodeEncoding:
                    CurrentEncoding.GetBytes("a", span);
                    _isBigEndian = span[0] == 0;
                    UnitSize = 2;
                    break;
            }
        }

        private (int lineStartIdx, int lineSize) ReadNextLine(out int consumed)
        {
            switch (UnitSize)
            {
                default:
                    throw new Exception($"Invalid unit size {UnitSize}");
                case 1:
                    return ReadNextLineUtf8(out consumed);
                case 2:
                    return ReadNextLineUtf16(out consumed);
                case 4:
                    return ReadNextLineUtf32(out consumed);
            }
        }

        private (int, int) ReadNextLineUtf32(out int consumed)
        {
            consumed = 0;
            var lf = _isBigEndian ? _utf32BeLF : _utf32LeLF;
            var cr = _isBigEndian ? _utf32BeCR : _utf32LeCR;
            ReadOnlySpan<int> separators = _isBigEndian ? _utf32BeLineSeparators : _utf32LeLineSeparators;
            Span<byte> peek = stackalloc byte[4];
            Span<int> asInts = MemoryMarshal.Cast<byte, int>(new Span<byte>(_buffer, _startPos, (_len / 2) * 2));
            var newLineIdx = asInts.IndexOfAny(separators);
            if (newLineIdx < 0)
            {
                return (_startPos, -1);
            }

            var lineByteLength = newLineIdx * 4;

            if (asInts[newLineIdx] == cr)
            {
                if (PeekBuffer((newLineIdx + 1) * 4, peek) && BitConverter.ToInt32(peek) == lf)
                {
                    newLineIdx++;
                }
            }
            consumed = (newLineIdx + 1) * 4;
            return (_startPos, lineByteLength);
        }

        private (int, int) ReadNextLineUtf16(out int consumed)
        {
            consumed = 0;
            var lf = _isBigEndian ? _utf16BeLF : _utf16LeLF;
            var cr = _isBigEndian ? _utf16BeCR : _utf16LeCR;
            ReadOnlySpan<short> separators = _isBigEndian ? _utf16BeLineSeparators : _utf16LeLineSeparators;
            Span<byte> peek = stackalloc byte[2];
            Span<short> asShorts = MemoryMarshal.Cast<byte, short>(new Span<byte>(_buffer, _startPos, (_len / 2) * 2));
            var newLineIdx = asShorts.IndexOfAny(separators);
            if (newLineIdx < 0)
            {
                return (_startPos, -1);
            }

            var lineByteLength = newLineIdx * 2;

            if (asShorts[newLineIdx] == cr)
            {
                if (PeekBuffer((newLineIdx + 1) * 2, peek) && BitConverter.ToInt16(peek) == lf)
                {
                    newLineIdx++;
                }
            }
            consumed = (newLineIdx + 1) * 2;
            return (_startPos, lineByteLength);
        }

        private (int, int) ReadNextLineUtf8(out int consumed)
        {
            consumed = 0;

            var byteSpan = _buffer.AsSpan().Slice(_startPos, _len);
            var newLineIdx = byteSpan.IndexOfAny(_utf8LineSeparators);

            if (newLineIdx < 0)
            {
                return (_startPos, -1);
            }
            Span<byte> peek = stackalloc byte[1];
            consumed = newLineIdx + 1;

            // determine if this is \r\n on Windows
            if (byteSpan[newLineIdx] == CarriageReturnByte)
            {
                if (PeekBuffer(newLineIdx + 1, peek) && peek[0] == LineFeedByte)
                {
                    consumed++;
                }
            }
            return (_startPos, newLineIdx);
        }

        private bool PeekBuffer(int offset, Span<byte> span)
        {
            if (offset + span.Length <= _len)
            {
                goto docopy;
            }

            var bytesToRead = offset + span.Length - _len;
            if (_startPos + _len + bytesToRead > _buffer.Length)
            {
                DoubleBufferSize();
            }

            var bytesRead = _stream.Read(_buffer, _startPos + _len, bytesToRead);
            _len += bytesRead;
            if (bytesRead == 0)
            {
                return false;
            }
            if (bytesRead != bytesToRead)
            {
                _len -= bytesRead;
                _stream.Seek(-bytesRead, SeekOrigin.Current);
            }
docopy:
            _buffer.AsSpan().Slice(_startPos + offset, span.Length).CopyTo(span);
            return true;
        }

        private void AdjustBuffer()
        {
            if (_len <= _startPos && _startPos + _len >= _buffer.Length / 2)
            {
                Debug.Assert(_len <= _buffer.Length / 2);
                _buffer.AsSpan().Slice(_startPos, _len).CopyTo(_buffer);
                _startPos = 0;
            }

            if (_buffer.Length - (_startPos + _len) < _blockSize)
            {
                DoubleBufferSize();
            }
        }

        private void DoubleBufferSize() => ResizeBuffer(_buffer.Length * 2);

        private Encoding DetectEncoding(out int consumed)
        {
            consumed = 0;
            if (_len < 2)
            {
                return null;
            }

            var byteBuffer = new Span<byte>(_buffer, _startPos, _len);

            if (byteBuffer[0] == 0xFE && byteBuffer[1] == 0xFF)
            {
                consumed = 2;
                // Big Endian Unicode
                return new UnicodeEncoding(true, true);
            }
            else if (byteBuffer[0] == 0xFF && byteBuffer[1] == 0xFE)
            {
                // Little Endian Unicode, or possibly little endian UTF32
                if (_len < 4 || byteBuffer[2] != 0 || byteBuffer[3] != 0)
                {
                    consumed = 2;
                    return new UnicodeEncoding(false, true);
                }
                if (CurrentEncoding is UnicodeEncoding)
                {
                    // now, there is a case here when the intended encoding is actually UTF16,
                    // but the stream starts with 0x00 0x00 so there's really no way for the parser to differ it from UTF-32
                    // in this case we need to rely on the user-provided encoding
                    consumed = 2;
                    return CurrentEncoding;
                }

                consumed = 4;
                return new UTF32Encoding(false, true);
            }
            else if (_len >= 3 && byteBuffer[0] == 0xEF && byteBuffer[1] == 0xBB && byteBuffer[2] == 0xBF)
            {
                // UTF-8
                consumed = 3;
                return Encoding.UTF8;
            }
            else if (_len >= 4 && byteBuffer[0] == 0 && byteBuffer[1] == 0 && byteBuffer[2] == 0xFE && byteBuffer[3] == 0xFF)
            {
                // Big Endian UTF32
                consumed = 4;
                return new UTF32Encoding(true, true);
            }
            else if (_len == 2)
            {
                return null;
            }

            // sufficient bytes have been read but no BOM found, we assume UTF8
            return Encoding.UTF8;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    DisposeBuffer();
                }

                _disposed = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
