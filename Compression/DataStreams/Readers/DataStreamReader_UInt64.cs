using System;
using System.Runtime.CompilerServices;
using System.Collections.Generic;
using System.IO;


namespace TimeSeriesDB.DataStreams.Readers
{
    using BaseClasses;

    /// <summary>
    ///     Stores efficiently a stream of ulong values.
    ///     Will store either the top-most or bottom-most bytes, depending on the side having the most zeroes.
    ///     If you know you are storing small values, use the UInt64_LSB encoders instead for a far better compression ratio.
    ///     Does automatic RLE on zero values.
    /// </summary>
    /// <remarks>
    ///     Encoding format explained in Constants_UInt64Encoding.
    /// </remarks>
    public sealed class DataStreamReader_UInt64 : StreamReaderBase, IDataStreamReader<ulong> {
        private ulong m_decompressedBuffer;

        /// <summary>
        ///     Returns the number of read items.
        ///     This might be smaller than expected due to the way data is encoded.
        ///     This method is about 75% faster than using the enumerator version.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Read(ulong[] buffer, int offset, int count) {
            var startOffset = offset;

            // written this way to avoid overflowing
            var endOffset = count < buffer.Length - offset ? offset + count : buffer.Length;

            // begin by emptying the decompress buffer that wasnt returned in previous call
            var decompressedCount = m_decompressedCount; // use a local variable for faster access
            if(decompressedCount > 0) { // can only be = 1 (1 value encoded)
                if(offset < endOffset) {
                    buffer[offset++] = m_decompressedBuffer;
                    m_decompressedCount = 0;
                }
            } else {
                while(decompressedCount != 0 && offset < endOffset) {
                    buffer[offset++] = 0;
                    decompressedCount++;
                }
                m_decompressedCount = decompressedCount;
            }

            while(offset < endOffset){
                this.EnsureBuffer();
                var flags = this.ReadByte();

                // if readbyte() sets m_bufferSize to zero, it means we reached the end of the stream
                if(m_bufferSize == 0)
                    break;

                if(Constants_UInt64Encoding.IsNonRLE(flags)){
                    byte flag1 = unchecked((byte)(flags >> 4));
                    byte flag2 = unchecked((byte)(flags & 0x0F));

                    buffer[offset++] = this.ReadValue(flag1);

                    if(flag2 != Constants_UInt64Encoding.SIGNAL_SINGLE_ITEM) {
                        var value = this.ReadValue(flag2);

                        if(offset < endOffset)
                            buffer[offset++] = value;
                        else {
                            // if we don't have the space to store the decompressed value, then you must store it for next call
                            m_decompressedCount = 1;
                            m_decompressedBuffer = value;
                            break; // technically not needed but potential speedup
                        }
                    }
                } else {
                    byte flag2 = unchecked((byte)(flags & 0x0F));

                    // RLE zeroes coding
                    var zeroes = flag2 + 1;
                    //Array.Clear(); // wont be much of a speedup on long[] anyway
                    while(offset < endOffset && zeroes-- > 0)
                        buffer[offset++] = 0;

                    // if there's remaining decompressed zeroes that could not be written to output, then store for next call
                    if(zeroes > 0) {
                        //m_decompressedBuffer = 0;
                        m_decompressedCount = (sbyte)-zeroes;
                        break; // technically not needed but potential speedup
                    }
                }
            }
            return offset - startOffset;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong ReadOne() {
            // begin by emptying the decompress buffer that wasnt returned in previous call
            if(m_decompressedCount > 0) { // can only be = 1 (1 value encoded)
                m_decompressedCount = 0;
                return m_decompressedBuffer;
            }
            if(m_decompressedCount != 0) {
                m_decompressedCount++;
                return 0;
            }

            this.EnsureBuffer();
            var flags = this.ReadByte();

            // if readbyte() sets m_bufferSize to zero, it means we reached the end of the stream
            if(m_bufferSize == 0)
                return 0; // throw new ApplicationException();

            if(Constants_UInt64Encoding.IsNonRLE(flags)) {
                byte flag1 = unchecked((byte)(flags >> 4));
                byte flag2 = unchecked((byte)(flags & 0x0F));

                var res = this.ReadValue(flag1);

                if(flag2 != Constants_UInt64Encoding.SIGNAL_SINGLE_ITEM) {
                    m_decompressedCount = 1;
                    m_decompressedBuffer = this.ReadValue(flag2);
                }

                return res;
            } else {
                byte flag2 = unchecked((byte)(flags & 0x0F));

                // RLE zeroes coding
                var zeroes = flag2 + 1;
                
                if(--zeroes > 0) {
                    //m_decompressedBuffer = 0;
                    m_decompressedCount = (sbyte)-zeroes;
                }

                return 0;
            }
        }

        #region Skip()
        public void Skip(int count) {
            if(count <= 0)
                return;

            // begin by emptying the decompress buffer that wasnt returned in previous call
            if(m_decompressedCount > 0) { // can only be = 1 (1 value encoded)
                m_decompressedCount = 0;
                count--;
            } else {
                var temp = Math.Min(-m_decompressedCount, count);

                m_decompressedCount += unchecked((sbyte)temp);
                count -= temp;
            }

            while(count > 0) {
                this.EnsureBuffer();
                var flags = this.ReadByte();

                // if readbyte() sets m_bufferSize to zero, it means we reached the end of the stream
                if(m_bufferSize == 0)
                    break;

                if(Constants_UInt64Encoding.IsNonRLE(flags)) {
                    byte flag1 = unchecked((byte)(flags >> 4));
                    byte flag2 = unchecked((byte)(flags & 0x0F));

                    this.ReadValue(flag1);
                    count--;

                    if(flag2 != Constants_UInt64Encoding.SIGNAL_SINGLE_ITEM) {
                        var value = this.ReadValue(flag2);

                        if(count > 0)
                            count--;
                        else {
                            // if we don't have the space to store the decompressed value, then you must store it for next call
                            m_decompressedCount = 1;
                            m_decompressedBuffer = value;
                            break; // technically not needed but potential speedup
                        }
                    }
                } else {
                    // RLE zeroes coding
                    byte flag2 = unchecked((byte)(flags & 0x0F));

                    // RLE zeroes coding
                    var zeroes = flag2 + 1;

                    var temp = Math.Min(zeroes, count);
                    count -= temp;
                    zeroes -= temp;

                    // if there's remaining decompressed zeroes that could not be written to output, then store for next call
                    if(zeroes > 0) {
                        //m_decompressedBuffer = 0;
                        m_decompressedCount = (sbyte)-zeroes;
                        break; // technically not needed but potential speedup
                    }
                }
            }
        }
        #endregion

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Read(Array items, int offset, int count) {
            return this.Read((ulong[])items, offset, count);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        object IDataStreamReader.ReadOne() {
            return this.ReadOne();
        }

        #region ReadAll()
        /// <summary>
        ///     Returns all decoded values.
        ///     This method is significantly slower than using Read().
        /// </summary>
        public IEnumerable<ulong> ReadAll() {
            // begin by emptying the decompress buffer that wasnt returned in previous call
            if(m_decompressedCount > 0) { // can only be = 1 (1 value encoded)
                m_decompressedCount = 0;
                yield return m_decompressedBuffer;
            } else {
                while(m_decompressedCount != 0) {
                    m_decompressedCount++;
                    yield return 0;
                }
            }

            while(true) {
                this.EnsureBuffer();
                var flags = this.ReadByte();

                // if readbyte() sets m_bufferSize to zero, it means we reached the end of the stream
                if(m_bufferSize == 0)
                    yield break;

                byte flag1 = unchecked( (byte)(flags >> 4) );
                byte flag2 = unchecked( (byte)(flags & 0x0F) );

                if(flag1 != Constants_UInt64Encoding.SIGNAL_REPEATING_ZEROES) {
                    yield return this.ReadValue(flag1);

                    if(flag2 != Constants_UInt64Encoding.SIGNAL_SINGLE_ITEM)
                        yield return this.ReadValue(flag2);
                } else {
                    // successive zeroes coding
                    var zeroes = flag2 + 1;
                    m_decompressedCount = (sbyte)unchecked( -(sbyte)zeroes );

                    do {
                        m_decompressedCount++;
                        yield return 0;
                    } while(m_decompressedCount != 0);
                }
            }
        }
        #endregion

        #region private ReadValue()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ulong ReadValue(byte flag) {
            var nbytes = (flag & Constants_UInt64Encoding.NBYTES_BIT_MASK) + 1;

            var value = base.ReadUInt64(nbytes);

            // if leading
            if((flag & Constants_UInt64Encoding.LEADING_BIT_MASK) != 0)
                value <<= (8 - nbytes) * 8;

            return value;
        }
        #endregion

        #region protected EnsureBuffer()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureBuffer() {
            this.EnsureBufferContains(Constants_UInt64Encoding.MAX_ENCODE_FRAME_SIZE);
        }
        #endregion

        #region CreateWriter()
        public Writers.IDataStreamWriter CreateWriter() {
            return new Writers.DataStreamWriter_UInt64();
        }
        #endregion
    }
}
