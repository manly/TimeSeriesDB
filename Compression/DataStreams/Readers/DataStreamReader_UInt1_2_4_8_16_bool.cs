using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using static System.Runtime.CompilerServices.MethodImplOptions;
using System.Diagnostics;
using System.IO;


namespace TimeSeriesDB.DataStreams.Readers
{
    using BaseClasses;

    #region public class DataStreamReader_UInt1
    public sealed class DataStreamReader_UInt1 : DoubleBufferedStreamReaderBase, IDataStreamReader<byte> {
        // (almost) same code for UInt1, UInt2, UInt4, UInt16, Int1, Int2, Int4, Int16, bool
        // most difference is constants used, and the quick read code

        private const int ITEM_SIZEOF_IN_BITS       = 1;
        private const int PRIMARY_BUFFER_ITEM_COUNT = PRIMARY_BUFFER_SIZE / ITEM_SIZEOF_IN_BITS;
        private const uint MASK                     = (1 << ITEM_SIZEOF_IN_BITS) - 1;

        [MethodImpl(AggressiveInlining)]
        public int Read(byte[] items, int offset, int count) {
            var startCount = count;

            // finish primary buffer alignment
            while(m_primaryBufferRemaining != 0 && count-- > 0)
                items[offset++] = this.ReadOne();

            // quick read
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                Debug.Assert(m_primaryBufferRemaining == 0);

                var buffer = this.ReadPrimaryBuffer();

                 // PRIMARY_BUFFER_ITEM_COUNT entries
                items[offset + 0] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 0)) & MASK));
                items[offset + 1] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 1)) & MASK));
                items[offset + 2] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 2)) & MASK));
                items[offset + 3] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 3)) & MASK));
                items[offset + 4] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 4)) & MASK));
                items[offset + 5] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 5)) & MASK));
                items[offset + 6] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 6)) & MASK));
                items[offset + 7] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 7)) & MASK));
                items[offset + 8] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 8)) & MASK));
                items[offset + 9] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 9)) & MASK));
                items[offset + 10] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 10)) & MASK));
                items[offset + 11] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 11)) & MASK));
                items[offset + 12] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 12)) & MASK));
                items[offset + 13] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 13)) & MASK));
                items[offset + 14] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 14)) & MASK));
                items[offset + 15] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 15)) & MASK));
                items[offset + 16] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 16)) & MASK));
                items[offset + 17] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 17)) & MASK));
                items[offset + 18] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 18)) & MASK));
                items[offset + 19] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 19)) & MASK));
                items[offset + 20] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 20)) & MASK));
                items[offset + 21] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 21)) & MASK));
                items[offset + 22] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 22)) & MASK));
                items[offset + 23] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 23)) & MASK));
                items[offset + 24] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 24)) & MASK));
                items[offset + 25] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 25)) & MASK));
                items[offset + 26] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 26)) & MASK));
                items[offset + 27] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 27)) & MASK));
                items[offset + 28] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 28)) & MASK));
                items[offset + 29] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 29)) & MASK));
                items[offset + 30] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 30)) & MASK));
                items[offset + 31] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 31)) & MASK));
                offset += 32;

                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                items[offset++] = this.ReadOne();

            // the count doesn't make much sense in the case of subdivided items where we dont know how many were written
            // as such we just return the count directly.
            return startCount;
        }

        [MethodImpl(AggressiveInlining)]
        public byte ReadOne() {
            if(m_primaryBufferRemaining == 0) {
                m_primaryBuffer          = this.ReadPrimaryBuffer();
                m_primaryBufferRemaining = PRIMARY_BUFFER_ITEM_COUNT;
            }

            m_primaryBufferRemaining--;
            var res = unchecked((byte)(m_primaryBuffer & MASK));
            m_primaryBuffer >>= ITEM_SIZEOF_IN_BITS;
            return res;
        }

        [MethodImpl(AggressiveInlining)]
        public int Read(Array items, int offset, int count) {
            return this.Read((byte[])items, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        object IDataStreamReader.ReadOne() {
            return this.ReadOne();
        }

        public void Skip(int count) {
            // finish primary buffer alignment
            while(m_primaryBufferRemaining != 0 && count-- > 0)
                this.ReadOne();

            // quick read
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                Debug.Assert(m_primaryBufferRemaining == 0);

                this.ReadAndSkipPrimaryBuffer();
                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                this.ReadOne();
        }
        
        public Writers.IDataStreamWriter CreateWriter() {
            return new Writers.DataStreamWriter_UInt1();
        }
    }
    #endregion
    #region public class DataStreamReader_UInt2
    public sealed class DataStreamReader_UInt2 : DoubleBufferedStreamReaderBase, IDataStreamReader<byte> {
        // (almost) same code for UInt1, UInt2, UInt4, UInt16, Int1, Int2, Int4, Int16, bool
        // most difference is constants used, and the quick read code

        private const int ITEM_SIZEOF_IN_BITS       = 2;
        private const int PRIMARY_BUFFER_ITEM_COUNT = PRIMARY_BUFFER_SIZE / ITEM_SIZEOF_IN_BITS;
        private const uint MASK                     = (1 << ITEM_SIZEOF_IN_BITS) - 1;

        [MethodImpl(AggressiveInlining)]
        public int Read(byte[] items, int offset, int count) {
            var startCount = count;

            // finish primary buffer alignment
            while(m_primaryBufferRemaining != 0 && count-- > 0)
                items[offset++] = this.ReadOne();

            // quick read
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                Debug.Assert(m_primaryBufferRemaining == 0);

                var buffer = this.ReadPrimaryBuffer();

                 // PRIMARY_BUFFER_ITEM_COUNT entries
                items[offset + 0] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 0)) & MASK));
                items[offset + 1] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 1)) & MASK));
                items[offset + 2] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 2)) & MASK));
                items[offset + 3] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 3)) & MASK));
                items[offset + 4] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 4)) & MASK));
                items[offset + 5] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 5)) & MASK));
                items[offset + 6] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 6)) & MASK));
                items[offset + 7] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 7)) & MASK));
                items[offset + 8] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 8)) & MASK));
                items[offset + 9] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 9)) & MASK));
                items[offset + 10] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 10)) & MASK));
                items[offset + 11] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 11)) & MASK));
                items[offset + 12] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 12)) & MASK));
                items[offset + 13] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 13)) & MASK));
                items[offset + 14] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 14)) & MASK));
                items[offset + 15] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 15)) & MASK));
                offset += 16;

                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                items[offset++] = this.ReadOne();

            // the count doesn't make much sense in the case of subdivided items where we dont know how many were written
            // as such we just return the count directly.
            return startCount;
        }

        [MethodImpl(AggressiveInlining)]
        public byte ReadOne() {
            if(m_primaryBufferRemaining == 0) {
                m_primaryBuffer          = this.ReadPrimaryBuffer();
                m_primaryBufferRemaining = PRIMARY_BUFFER_ITEM_COUNT;
            }

            m_primaryBufferRemaining--;
            var res = unchecked((byte)(m_primaryBuffer & MASK));
            m_primaryBuffer >>= ITEM_SIZEOF_IN_BITS;
            return res;
        }

        [MethodImpl(AggressiveInlining)]
        public int Read(Array items, int offset, int count) {
            return this.Read((byte[])items, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        object IDataStreamReader.ReadOne() {
            return this.ReadOne();
        }

        public void Skip(int count) {
            // finish primary buffer alignment
            while(m_primaryBufferRemaining != 0 && count-- > 0)
                this.ReadOne();

            // quick read
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                Debug.Assert(m_primaryBufferRemaining == 0);

                this.ReadAndSkipPrimaryBuffer();
                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                this.ReadOne();
        }
        
        public Writers.IDataStreamWriter CreateWriter() {
            return new Writers.DataStreamWriter_UInt2();
        }
    }
    #endregion
    #region public class DataStreamReader_UInt4
    public sealed class DataStreamReader_UInt4 : DoubleBufferedStreamReaderBase, IDataStreamReader<byte> {
        // (almost) same code for UInt1, UInt2, UInt4, UInt16, Int1, Int2, Int4, Int16, bool
        // most difference is constants used, and the quick read code

        private const int ITEM_SIZEOF_IN_BITS       = 4;
        private const int PRIMARY_BUFFER_ITEM_COUNT = PRIMARY_BUFFER_SIZE / ITEM_SIZEOF_IN_BITS;
        private const uint MASK                     = (1 << ITEM_SIZEOF_IN_BITS) - 1;

        [MethodImpl(AggressiveInlining)]
        public int Read(byte[] items, int offset, int count) {
            var startCount = count;

            // finish primary buffer alignment
            while(m_primaryBufferRemaining != 0 && count-- > 0)
                items[offset++] = this.ReadOne();

            // quick read
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                Debug.Assert(m_primaryBufferRemaining == 0);

                var buffer = this.ReadPrimaryBuffer();

                 // PRIMARY_BUFFER_ITEM_COUNT entries
                items[offset + 0] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 0)) & MASK));
                items[offset + 1] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 1)) & MASK));
                items[offset + 2] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 2)) & MASK));
                items[offset + 3] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 3)) & MASK));
                items[offset + 4] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 4)) & MASK));
                items[offset + 5] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 5)) & MASK));
                items[offset + 6] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 6)) & MASK));
                items[offset + 7] = unchecked((byte)((buffer >> (ITEM_SIZEOF_IN_BITS * 7)) & MASK));
                offset += 8;

                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                items[offset++] = this.ReadOne();

            // the count doesn't make much sense in the case of subdivided items where we dont know how many were written
            // as such we just return the count directly.
            return startCount;
        }

        [MethodImpl(AggressiveInlining)]
        public byte ReadOne() {
            if(m_primaryBufferRemaining == 0) {
                m_primaryBuffer          = this.ReadPrimaryBuffer();
                m_primaryBufferRemaining = PRIMARY_BUFFER_ITEM_COUNT;
            }

            m_primaryBufferRemaining--;
            var res = unchecked((byte)(m_primaryBuffer & MASK));
            m_primaryBuffer >>= ITEM_SIZEOF_IN_BITS;
            return res;
        }

        [MethodImpl(AggressiveInlining)]
        public int Read(Array items, int offset, int count) {
            return this.Read((byte[])items, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        object IDataStreamReader.ReadOne() {
            return this.ReadOne();
        }

        public void Skip(int count) {
            // finish primary buffer alignment
            while(m_primaryBufferRemaining != 0 && count-- > 0)
                this.ReadOne();

            // quick read
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                Debug.Assert(m_primaryBufferRemaining == 0);

                this.ReadAndSkipPrimaryBuffer();
                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                this.ReadOne();
        }
        
        public Writers.IDataStreamWriter CreateWriter() {
            return new Writers.DataStreamWriter_UInt4();
        }
    }
    #endregion
    #region public class DataStreamReader_UInt8
    public sealed class DataStreamReader_UInt8 : DoubleBufferedStreamReaderBase, IDataStreamReader<byte> {
        // exceptionally this class doesn't really use a primary buffer

        [MethodImpl(AggressiveInlining)]
        public int Read(byte[] items, int offset, int count) {
            return m_stream.Read(items, offset, count);

            //if(count <= 0)
            //    return 0;
            //var read1 = Math.Min(m_secondaryBufferSize - m_secondaryBufferIndex, count);
            //if(read1 > 0) {
            //    Buffer.BlockCopy(m_secondaryBuffer, m_secondaryBufferIndex, items, offset, read1);
            //    count -= read1;
            //    offset += read1;
            //    m_secondaryBufferIndex += read1;
            //}
            //// if anything remains, read directly as this class isn't intended to act as a bufferedstream
            //if(count > 0) {
            //    var read2 = m_stream.Read(items, offset, count);
            //    return read1 + read2;
            //}
            //return read1;
        }

        [MethodImpl(AggressiveInlining)]
        public byte ReadOne() {
            return unchecked((byte)Math.Min(m_stream.ReadByte(), 0));

            //if(m_secondaryBufferIndex >= m_secondaryBufferSize) {
            //    m_secondaryBufferIndex = 0;
            //    m_secondaryBufferSize = m_stream.Read(m_secondaryBuffer, 0, SECONDARY_BUFFER_SIZE);
            //}
            //return m_secondaryBuffer[m_secondaryBufferIndex++];
        }

        [MethodImpl(AggressiveInlining)]
        public int Read(Array items, int offset, int count) {
            return this.Read((byte[])items, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        object IDataStreamReader.ReadOne() {
            return this.ReadOne();
        }

        public void Skip(int count) {
            m_stream.Seek(count, SeekOrigin.Current);
        }
        
        public Writers.IDataStreamWriter CreateWriter() {
            return new Writers.DataStreamWriter_UInt8();
        }
    }
    #endregion
    #region public class DataStreamReader_UInt16
    public sealed class DataStreamReader_UInt16 : DoubleBufferedStreamReaderBase, IDataStreamReader<ushort> {
        // (almost) same code for UInt1, UInt2, UInt4, UInt16, Int1, Int2, Int4, Int16, bool
        // most difference is constants used, and the quick read code

        private const int ITEM_SIZEOF_IN_BITS       = 16;
        private const int PRIMARY_BUFFER_ITEM_COUNT = PRIMARY_BUFFER_SIZE / ITEM_SIZEOF_IN_BITS;
        private const uint MASK                     = (1 << ITEM_SIZEOF_IN_BITS) - 1;

        [MethodImpl(AggressiveInlining)]
        public int Read(ushort[] items, int offset, int count) {
            var startCount = count;

            // finish primary buffer alignment
            while(m_primaryBufferRemaining != 0 && count-- > 0)
                items[offset++] = this.ReadOne();

            // quick read
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                Debug.Assert(m_primaryBufferRemaining == 0);

                var buffer = this.ReadPrimaryBuffer();

                 // PRIMARY_BUFFER_ITEM_COUNT entries
                items[offset + 0] = unchecked((ushort)((buffer >> (ITEM_SIZEOF_IN_BITS * 0)) & MASK));
                items[offset + 1] = unchecked((ushort)((buffer >> (ITEM_SIZEOF_IN_BITS * 1)) & MASK));
                offset += 2;

                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                items[offset++] = this.ReadOne();

            // the count doesn't make much sense in the case of subdivided items where we dont know how many were written
            // as such we just return the count directly.
            return startCount;
        }

        [MethodImpl(AggressiveInlining)]
        public ushort ReadOne() {
            if(m_primaryBufferRemaining == 0) {
                m_primaryBuffer          = this.ReadPrimaryBuffer();
                m_primaryBufferRemaining = PRIMARY_BUFFER_ITEM_COUNT;
            }

            m_primaryBufferRemaining--;
            var res = unchecked((ushort)(m_primaryBuffer & MASK));
            m_primaryBuffer >>= ITEM_SIZEOF_IN_BITS;
            return res;
        }

        [MethodImpl(AggressiveInlining)]
        public int Read(Array items, int offset, int count) {
            return this.Read((ushort[])items, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        object IDataStreamReader.ReadOne() {
            return this.ReadOne();
        }

        public void Skip(int count) {
            // finish primary buffer alignment
            while(m_primaryBufferRemaining != 0 && count-- > 0)
                this.ReadOne();

            // quick read
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                Debug.Assert(m_primaryBufferRemaining == 0);

                this.ReadAndSkipPrimaryBuffer();
                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                this.ReadOne();
        }
        
        public Writers.IDataStreamWriter CreateWriter() {
            return new Writers.DataStreamWriter_UInt16();
        }
    }
    #endregion

    #region public class DataStreamReader_Bool
    public sealed class DataStreamReader_Bool : DoubleBufferedStreamReaderBase, IDataStreamReader<bool> {
        // (almost) same code for UInt1, UInt2, UInt4, UInt16, Int1, Int2, Int4, Int16, bool
        // most difference is constants used, and the quick read code

        private const int ITEM_SIZEOF_IN_BITS       = 1;
        private const int PRIMARY_BUFFER_ITEM_COUNT = PRIMARY_BUFFER_SIZE / ITEM_SIZEOF_IN_BITS;
        private const uint MASK                     = (1 << ITEM_SIZEOF_IN_BITS) - 1;

        [MethodImpl(AggressiveInlining)]
        public int Read(bool[] items, int offset, int count) {
            var startCount = count;

            // finish primary buffer alignment
            while(m_primaryBufferRemaining != 0 && count-- > 0)
                items[offset++] = this.ReadOne();

            // quick read
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                Debug.Assert(m_primaryBufferRemaining == 0);

                var buffer = this.ReadPrimaryBuffer();

                 // PRIMARY_BUFFER_ITEM_COUNT entries
                items[offset + 0] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 0))) != 0;
                items[offset + 1] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 1))) != 0;
                items[offset + 2] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 2))) != 0;
                items[offset + 3] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 3))) != 0;
                items[offset + 4] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 4))) != 0;
                items[offset + 5] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 5))) != 0;
                items[offset + 6] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 6))) != 0;
                items[offset + 7] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 7))) != 0;
                items[offset + 8] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 8))) != 0;
                items[offset + 9] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 9))) != 0;
                items[offset + 10] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 10))) != 0;
                items[offset + 11] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 11))) != 0;
                items[offset + 12] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 12))) != 0;
                items[offset + 13] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 13))) != 0;
                items[offset + 14] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 14))) != 0;
                items[offset + 15] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 15))) != 0;
                items[offset + 16] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 16))) != 0;
                items[offset + 17] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 17))) != 0;
                items[offset + 18] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 18))) != 0;
                items[offset + 19] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 19))) != 0;
                items[offset + 20] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 20))) != 0;
                items[offset + 21] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 21))) != 0;
                items[offset + 22] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 22))) != 0;
                items[offset + 23] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 23))) != 0;
                items[offset + 24] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 24))) != 0;
                items[offset + 25] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 25))) != 0;
                items[offset + 26] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 26))) != 0;
                items[offset + 27] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 27))) != 0;
                items[offset + 28] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 28))) != 0;
                items[offset + 29] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 29))) != 0;
                items[offset + 30] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 30))) != 0;
                items[offset + 31] = (buffer & (MASK << (ITEM_SIZEOF_IN_BITS * 31))) != 0;
                offset += 32;

                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                items[offset++] = this.ReadOne();

            // the count doesn't make much sense in the case of subdivided items where we dont know how many were written
            // as such we just return the count directly.
            return startCount;
        }

        [MethodImpl(AggressiveInlining)]
        public bool ReadOne() {
            if(m_primaryBufferRemaining == 0) {
                m_primaryBuffer          = this.ReadPrimaryBuffer();
                m_primaryBufferRemaining = PRIMARY_BUFFER_ITEM_COUNT;
            }

            m_primaryBufferRemaining--;
            var res = (m_primaryBuffer & MASK) != 0;
            m_primaryBuffer >>= ITEM_SIZEOF_IN_BITS;
            return res;
        }

        [MethodImpl(AggressiveInlining)]
        public int Read(Array items, int offset, int count) {
            return this.Read((bool[])items, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        object IDataStreamReader.ReadOne() {
            return this.ReadOne();
        }

        public void Skip(int count) {
            // finish primary buffer alignment
            while(m_primaryBufferRemaining != 0 && count-- > 0)
                this.ReadOne();

            // quick read
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                Debug.Assert(m_primaryBufferRemaining == 0);

                this.ReadAndSkipPrimaryBuffer();
                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                this.ReadOne();
        }

        public Writers.IDataStreamWriter CreateWriter() {
            return new Writers.DataStreamWriter_Bool();
        }
    }
    #endregion

    #region public class DataStreamReader_Int1
    public sealed class DataStreamReader_Int1 : DoubleBufferedStreamReaderBase, IDataStreamReader<sbyte> {
        // (almost) same code for UInt1, UInt2, UInt4, UInt16, Int1, Int2, Int4, Int16, bool
        // most difference is constants used, and the quick read code

        private const int ITEM_SIZEOF_IN_BITS       = 1;
        private const int PRIMARY_BUFFER_ITEM_COUNT = PRIMARY_BUFFER_SIZE / ITEM_SIZEOF_IN_BITS;
        private const uint MASK                     = (1 << ITEM_SIZEOF_IN_BITS) - 1;

        [MethodImpl(AggressiveInlining)]
        public int Read(sbyte[] items, int offset, int count) {
            var startCount = count;

            // finish primary buffer alignment
            while(m_primaryBufferRemaining != 0 && count-- > 0)
                items[offset++] = this.ReadOne();

            // quick read
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                Debug.Assert(m_primaryBufferRemaining == 0);

                var buffer = this.ReadPrimaryBuffer();

                 // PRIMARY_BUFFER_ITEM_COUNT entries
                items[offset + 0] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 0)) & MASK));
                items[offset + 1] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 1)) & MASK));
                items[offset + 2] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 2)) & MASK));
                items[offset + 3] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 3)) & MASK));
                items[offset + 4] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 4)) & MASK));
                items[offset + 5] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 5)) & MASK));
                items[offset + 6] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 6)) & MASK));
                items[offset + 7] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 7)) & MASK));
                items[offset + 8] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 8)) & MASK));
                items[offset + 9] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 9)) & MASK));
                items[offset + 10] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 10)) & MASK));
                items[offset + 11] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 11)) & MASK));
                items[offset + 12] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 12)) & MASK));
                items[offset + 13] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 13)) & MASK));
                items[offset + 14] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 14)) & MASK));
                items[offset + 15] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 15)) & MASK));
                items[offset + 16] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 16)) & MASK));
                items[offset + 17] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 17)) & MASK));
                items[offset + 18] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 18)) & MASK));
                items[offset + 19] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 19)) & MASK));
                items[offset + 20] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 20)) & MASK));
                items[offset + 21] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 21)) & MASK));
                items[offset + 22] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 22)) & MASK));
                items[offset + 23] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 23)) & MASK));
                items[offset + 24] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 24)) & MASK));
                items[offset + 25] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 25)) & MASK));
                items[offset + 26] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 26)) & MASK));
                items[offset + 27] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 27)) & MASK));
                items[offset + 28] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 28)) & MASK));
                items[offset + 29] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 29)) & MASK));
                items[offset + 30] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 30)) & MASK));
                items[offset + 31] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 31)) & MASK));
                offset += 32;

                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                items[offset++] = this.ReadOne();

            // the count doesn't make much sense in the case of subdivided items where we dont know how many were written
            // as such we just return the count directly.
            return startCount;
        }

        [MethodImpl(AggressiveInlining)]
        public sbyte ReadOne() {
            if(m_primaryBufferRemaining == 0) {
                m_primaryBuffer          = this.ReadPrimaryBuffer();
                m_primaryBufferRemaining = PRIMARY_BUFFER_ITEM_COUNT;
            }

            m_primaryBufferRemaining--;
            var res = unchecked((sbyte)(m_primaryBuffer & MASK));
            m_primaryBuffer >>= ITEM_SIZEOF_IN_BITS;
            return res;
        }

        [MethodImpl(AggressiveInlining)]
        public int Read(Array items, int offset, int count) {
            return this.Read((sbyte[])items, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        object IDataStreamReader.ReadOne() {
            return this.ReadOne();
        }

        public void Skip(int count) {
            // finish primary buffer alignment
            while(m_primaryBufferRemaining != 0 && count-- > 0)
                this.ReadOne();

            // quick read
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                Debug.Assert(m_primaryBufferRemaining == 0);

                this.ReadAndSkipPrimaryBuffer();
                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                this.ReadOne();
        }
        
        public Writers.IDataStreamWriter CreateWriter() {
            return new Writers.DataStreamWriter_Int1();
        }
    }
    #endregion
    #region public class DataStreamReader_Int2
    public sealed class DataStreamReader_Int2 : DoubleBufferedStreamReaderBase, IDataStreamReader<sbyte> {
        // (almost) same code for UInt1, UInt2, UInt4, UInt16, Int1, Int2, Int4, Int16, bool
        // most difference is constants used, and the quick read code

        private const int ITEM_SIZEOF_IN_BITS       = 2;
        private const int PRIMARY_BUFFER_ITEM_COUNT = PRIMARY_BUFFER_SIZE / ITEM_SIZEOF_IN_BITS;
        private const uint MASK                     = (1 << ITEM_SIZEOF_IN_BITS) - 1;

        [MethodImpl(AggressiveInlining)]
        public int Read(sbyte[] items, int offset, int count) {
            var startCount = count;

            // finish primary buffer alignment
            while(m_primaryBufferRemaining != 0 && count-- > 0)
                items[offset++] = this.ReadOne();

            // quick read
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                Debug.Assert(m_primaryBufferRemaining == 0);

                var buffer = this.ReadPrimaryBuffer();

                 // PRIMARY_BUFFER_ITEM_COUNT entries
                items[offset + 0] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 0)) & MASK));
                items[offset + 1] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 1)) & MASK));
                items[offset + 2] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 2)) & MASK));
                items[offset + 3] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 3)) & MASK));
                items[offset + 4] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 4)) & MASK));
                items[offset + 5] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 5)) & MASK));
                items[offset + 6] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 6)) & MASK));
                items[offset + 7] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 7)) & MASK));
                items[offset + 8] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 8)) & MASK));
                items[offset + 9] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 9)) & MASK));
                items[offset + 10] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 10)) & MASK));
                items[offset + 11] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 11)) & MASK));
                items[offset + 12] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 12)) & MASK));
                items[offset + 13] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 13)) & MASK));
                items[offset + 14] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 14)) & MASK));
                items[offset + 15] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 15)) & MASK));
                offset += 16;

                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                items[offset++] = this.ReadOne();

            // the count doesn't make much sense in the case of subdivided items where we dont know how many were written
            // as such we just return the count directly.
            return startCount;
        }

        [MethodImpl(AggressiveInlining)]
        public sbyte ReadOne() {
            if(m_primaryBufferRemaining == 0) {
                m_primaryBuffer          = this.ReadPrimaryBuffer();
                m_primaryBufferRemaining = PRIMARY_BUFFER_ITEM_COUNT;
            }

            m_primaryBufferRemaining--;
            var res = unchecked((sbyte)(m_primaryBuffer & MASK));
            m_primaryBuffer >>= ITEM_SIZEOF_IN_BITS;
            return res;
        }

        [MethodImpl(AggressiveInlining)]
        public int Read(Array items, int offset, int count) {
            return this.Read((sbyte[])items, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        object IDataStreamReader.ReadOne() {
            return this.ReadOne();
        }

        public void Skip(int count) {
            // finish primary buffer alignment
            while(m_primaryBufferRemaining != 0 && count-- > 0)
                this.ReadOne();

            // quick read
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                Debug.Assert(m_primaryBufferRemaining == 0);

                this.ReadAndSkipPrimaryBuffer();
                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                this.ReadOne();
        }

        public Writers.IDataStreamWriter CreateWriter() {
            return new Writers.DataStreamWriter_Int2();
        }
    }
    #endregion
    #region public class DataStreamReader_Int4
    public sealed class DataStreamReader_Int4 : DoubleBufferedStreamReaderBase, IDataStreamReader<sbyte> {
        // (almost) same code for UInt1, UInt2, UInt4, UInt16, Int1, Int2, Int4, Int16, bool
        // most difference is constants used, and the quick read code

        private const int ITEM_SIZEOF_IN_BITS       = 4;
        private const int PRIMARY_BUFFER_ITEM_COUNT = PRIMARY_BUFFER_SIZE / ITEM_SIZEOF_IN_BITS;
        private const uint MASK                     = (1 << ITEM_SIZEOF_IN_BITS) - 1;

        [MethodImpl(AggressiveInlining)]
        public int Read(sbyte[] items, int offset, int count) {
            var startCount = count;

            // finish primary buffer alignment
            while(m_primaryBufferRemaining != 0 && count-- > 0)
                items[offset++] = this.ReadOne();

            // quick read
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                Debug.Assert(m_primaryBufferRemaining == 0);

                var buffer = this.ReadPrimaryBuffer();

                 // PRIMARY_BUFFER_ITEM_COUNT entries
                items[offset + 0] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 0)) & MASK));
                items[offset + 1] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 1)) & MASK));
                items[offset + 2] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 2)) & MASK));
                items[offset + 3] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 3)) & MASK));
                items[offset + 4] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 4)) & MASK));
                items[offset + 5] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 5)) & MASK));
                items[offset + 6] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 6)) & MASK));
                items[offset + 7] = unchecked((sbyte)((buffer >> (ITEM_SIZEOF_IN_BITS * 7)) & MASK));
                offset += 8;

                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                items[offset++] = this.ReadOne();

            // the count doesn't make much sense in the case of subdivided items where we dont know how many were written
            // as such we just return the count directly.
            return startCount;
        }

        [MethodImpl(AggressiveInlining)]
        public sbyte ReadOne() {
            if(m_primaryBufferRemaining == 0) {
                m_primaryBuffer          = this.ReadPrimaryBuffer();
                m_primaryBufferRemaining = PRIMARY_BUFFER_ITEM_COUNT;
            }

            m_primaryBufferRemaining--;
            var res = unchecked((sbyte)(m_primaryBuffer & MASK));
            m_primaryBuffer >>= ITEM_SIZEOF_IN_BITS;
            return res;
        }

        [MethodImpl(AggressiveInlining)]
        public int Read(Array items, int offset, int count) {
            return this.Read((sbyte[])items, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        object IDataStreamReader.ReadOne() {
            return this.ReadOne();
        }

        public void Skip(int count) {
            // finish primary buffer alignment
            while(m_primaryBufferRemaining != 0 && count-- > 0)
                this.ReadOne();

            // quick read
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                Debug.Assert(m_primaryBufferRemaining == 0);

                this.ReadAndSkipPrimaryBuffer();
                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                this.ReadOne();
        }
        
        public Writers.IDataStreamWriter CreateWriter() {
            return new Writers.DataStreamWriter_Int4();
        }
    }
    #endregion
    #region public class DataStreamReader_Int8
    public sealed class DataStreamReader_Int8 : DoubleBufferedStreamReaderBase, IDataStreamReader<sbyte> {
        // exceptionally this class doesn't really use a primary buffer

        [MethodImpl(AggressiveInlining)]
        public int Read(sbyte[] items, int offset, int count) {
            var startCount = count;

            while(count > 0) {
                if(m_secondaryBufferIndex >= m_secondaryBufferSize) {
                    m_secondaryBufferIndex = 0;
                    m_secondaryBufferSize = m_stream.Read(m_secondaryBuffer, 0, SECONDARY_BUFFER_SIZE);
                    if(m_secondaryBufferSize == 0)
                        break;
                }

                var read = Math.Min(m_secondaryBufferSize - m_secondaryBufferIndex, count);
                
                Buffer.BlockCopy(m_secondaryBuffer, m_secondaryBufferIndex, items, offset, read);

                count -= read;
                offset += read;
                m_secondaryBufferIndex += read;
            }

            return startCount - count;
        }

        [MethodImpl(AggressiveInlining)]
        public sbyte ReadOne() {
            if(m_secondaryBufferIndex >= m_secondaryBufferSize) {
                m_secondaryBufferIndex = 0;
                m_secondaryBufferSize = m_stream.Read(m_secondaryBuffer, 0, SECONDARY_BUFFER_SIZE);
                if(m_secondaryBufferSize == 0)
                    return 0;
            }

            return unchecked((sbyte)m_secondaryBuffer[m_secondaryBufferIndex++]);
        }

        [MethodImpl(AggressiveInlining)]
        public int Read(Array items, int offset, int count) {
            return this.Read((sbyte[])items, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        object IDataStreamReader.ReadOne() {
            return this.ReadOne();
        }

        public void Skip(int count) {
            //m_stream.Seek(count, SeekOrigin.Current);

            while(count > 0) {
                if(m_secondaryBufferIndex >= m_secondaryBufferSize) {
                    m_secondaryBufferIndex = 0;
                    m_secondaryBufferSize = m_stream.Read(m_secondaryBuffer, 0, SECONDARY_BUFFER_SIZE);
                    if(m_secondaryBufferSize == 0)
                        break;
                }

                var read = Math.Min(m_secondaryBufferSize - m_secondaryBufferIndex, count);

                count -= read;
                m_secondaryBufferIndex += read;
            }
        }

        public Writers.IDataStreamWriter CreateWriter() {
            return new Writers.DataStreamWriter_Int8();
        }
    }
    #endregion
    #region public class DataStreamReader_Int16
    public sealed class DataStreamReader_Int16 : DoubleBufferedStreamReaderBase, IDataStreamReader<short> {
        // (almost) same code for UInt1, UInt2, UInt4, UInt16, Int1, Int2, Int4, Int16, bool
        // most difference is constants used, and the quick read code

        private const int ITEM_SIZEOF_IN_BITS       = 16;
        private const int PRIMARY_BUFFER_ITEM_COUNT = PRIMARY_BUFFER_SIZE / ITEM_SIZEOF_IN_BITS;
        private const uint MASK                     = (1 << ITEM_SIZEOF_IN_BITS) - 1;

        [MethodImpl(AggressiveInlining)]
        public int Read(short[] items, int offset, int count) {
            var startCount = count;

            // finish primary buffer alignment
            while(m_primaryBufferRemaining != 0 && count-- > 0)
                items[offset++] = this.ReadOne();

            // quick read
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                Debug.Assert(m_primaryBufferRemaining == 0);

                var buffer = this.ReadPrimaryBuffer();

                 // PRIMARY_BUFFER_ITEM_COUNT entries
                items[offset + 0] = unchecked((short)((buffer >> (ITEM_SIZEOF_IN_BITS * 0)) & MASK));
                items[offset + 1] = unchecked((short)((buffer >> (ITEM_SIZEOF_IN_BITS * 1)) & MASK));
                offset += 2;

                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                items[offset++] = this.ReadOne();

            // the count doesn't make much sense in the case of subdivided items where we dont know how many were written
            // as such we just return the count directly.
            return startCount;
        }

        [MethodImpl(AggressiveInlining)]
        public short ReadOne() {
            if(m_primaryBufferRemaining == 0) {
                m_primaryBuffer          = this.ReadPrimaryBuffer();
                m_primaryBufferRemaining = PRIMARY_BUFFER_ITEM_COUNT;
            }

            m_primaryBufferRemaining--;
            var res = unchecked((short)(m_primaryBuffer & MASK));
            m_primaryBuffer >>= ITEM_SIZEOF_IN_BITS;
            return res;
        }

        [MethodImpl(AggressiveInlining)]
        public int Read(Array items, int offset, int count) {
            return this.Read((short[])items, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        object IDataStreamReader.ReadOne() {
            return this.ReadOne();
        }

        public void Skip(int count) {
            // finish primary buffer alignment
            while(m_primaryBufferRemaining != 0 && count-- > 0)
                this.ReadOne();

            // quick read
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                Debug.Assert(m_primaryBufferRemaining == 0);

                this.ReadAndSkipPrimaryBuffer();
                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                this.ReadOne();
        }

        public Writers.IDataStreamWriter CreateWriter() {
            return new Writers.DataStreamWriter_Int16();
        }
    }
    #endregion
}