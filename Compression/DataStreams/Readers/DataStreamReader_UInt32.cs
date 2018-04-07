using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.IO;


namespace TimeSeriesDB.DataStreams.Readers
{
    using BaseClasses;

    /// <summary>
    ///     Stores efficiently a stream of uint values.
    ///     Will store either the top-most or bottom-most bytes, depending on the side having the most zeroes.
    ///     Does automatic RLE on zero values.
    /// </summary>
    /// <remarks>
    ///     Encoding format explained in Constants_UInt32Encoding.
    /// </remarks>
    public sealed class DataStreamReader_UInt32 : StreamReaderBase, IDataStreamReader<uint> {
        private uint m_decompressedBuffer;

        /// <summary>
        ///     Returns the number of read items.
        ///     This method is about 75% faster than using the enumerator version.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Read(uint[] buffer, int offset, int count) {
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

            // if the output is full, then you must not attempt to read the next pair as we cannot store a decompress buffer of 2 items
            while(offset != endOffset) {
                this.EnsureBuffer();
                var flags = this.ReadByte();

                // if readbyte() sets m_bufferSize to zero, it means we reached the end of the stream
                if(m_bufferSize == 0)
                    break;

                byte flag1 = unchecked( (byte)(flags >> 4) );

                if(Constants_UInt32Encoding.IsNonRLE(flag1)) {
                    byte flag2 = unchecked((byte)(flags & 0x0F));
                    // first one is always fine
                    buffer[offset++] = this.ReadValue(flag1);

                    if(flag2 != Constants_UInt32Encoding.SIGNAL_SINGLE_ITEM) {
                        var value = this.ReadValue(flag2);
                        if(offset != endOffset)
                            buffer[offset++] = value;
                        else {
                            // if we don't have the space to store the decompressed value, then you must store it for next call
                            m_decompressedCount = 1;
                            m_decompressedBuffer = value;
                            break; // technically not needed but potential speedup
                        }
                    }
                } else {
                    // RLE zeroes coding
                    int zeroes = Constants_UInt32Encoding.DecodeConsecutiveZeroes(flags);

                    //var remainingWritableItems = endOffset - offset;
                    //var writableZeroes = Math.Min(remainingWritableItems, zeroes); // can't be above MAX_CONSECUTIVE_ZEROES
                    //if(writableZeroes > 0) {
                    //    Array.Clear(buffer, offset, writableZeroes);
                    //    offset += writableZeroes;
                    //    zeroes -= writableZeroes;
                    //}

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
        public uint ReadOne() {
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

            byte flag1 = unchecked( (byte)(flags >> 4) );

            if(Constants_UInt32Encoding.IsNonRLE(flag1)) {
                byte flag2 = unchecked((byte)(flags & 0x0F));
                // first one is always fine
                var res = this.ReadValue(flag1);

                if(flag2 != Constants_UInt32Encoding.SIGNAL_SINGLE_ITEM) {
                    m_decompressedCount = 1;
                    m_decompressedBuffer = this.ReadValue(flag2);
                }

                return res;
            } else {
                // RLE zeroes coding
                int zeroes = Constants_UInt32Encoding.DecodeConsecutiveZeroes(flags);

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

                byte flag1 = unchecked((byte)(flags >> 4));

                if(Constants_UInt32Encoding.IsNonRLE(flag1)) {
                    byte flag2 = unchecked((byte)(flags & 0x0F));
                    // first one is always fine
                    this.ReadValue(flag1);
                    count--;

                    if(flag2 != Constants_UInt32Encoding.SIGNAL_SINGLE_ITEM) {
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
                    int zeroes = Constants_UInt32Encoding.DecodeConsecutiveZeroes(flags);

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
            return this.Read((uint[])items, offset, count);
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
        public IEnumerable<uint> ReadAll() {
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

                if(Constants_UInt32Encoding.IsNonRLE(flag1)) {
                    yield return this.ReadValue(flag1);

                    if(flag2 != Constants_UInt32Encoding.SIGNAL_SINGLE_ITEM)
                        yield return this.ReadValue(flag2);
                } else {
                    // RLE zeroes coding
                    var zeroes = Constants_UInt32Encoding.DecodeConsecutiveZeroes(flags);
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
        private uint ReadValue(byte flag) {
            var nbytes = (flag & Constants_UInt32Encoding.NBYTES_BIT_MASK) >> Constants_UInt32Encoding.NBYTES_SHIFT; //var nbytes = (flag & Constants_UInt32Encoding.NBYTES_BIT_MASK) + 1;

            var value = base.ReadUInt32(nbytes);

            // if leading
            if((flag & Constants_UInt32Encoding.LEADING_BIT_MASK) != 0)
                value <<= (4 - nbytes) * 8;

            return value;
        }
        #endregion

        #region protected EnsureBuffer()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureBuffer() {
            this.EnsureBufferContains(Constants_UInt32Encoding.MAX_ENCODE_FRAME_SIZE);
        }
        #endregion

        #region CreateWriter()
        public Writers.IDataStreamWriter CreateWriter() {
            return new Writers.DataStreamWriter_UInt32();
        }
        #endregion
    }
}
