using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using static System.Runtime.CompilerServices.MethodImplOptions;
using System.IO;


namespace TimeSeriesDB.DataStreams.Writers
{
    using BaseClasses;

    #region public class DataStreamWriter_UInt1
    public sealed class DataStreamWriter_UInt1 : DoubleBufferedStreamWriterBase, IDataStreamWriter<byte>, IResumableDataStreamWriter {
        // (almost) same code for UInt1, UInt2, UInt4, UInt16, Int1, Int2, Int4, Int16, bool
        // most difference is constants used, and the quick add code

        private const int ITEM_SIZEOF_IN_BITS       = 1;
        private const int PRIMARY_BUFFER_ITEM_COUNT = PRIMARY_BUFFER_SIZE_IN_BITS / ITEM_SIZEOF_IN_BITS;
        private const uint MASK                     = (1 << ITEM_SIZEOF_IN_BITS) - 1;

        [MethodImpl(AggressiveInlining)]
        public void Write(byte[] items, int offset, int count) {
            //while(count-- > 0)
            //    this.Write(items[offset++]);
            //return;

            // finish primary buffer alignment
            while(m_primaryBufferIndex != 0 && count-- > 0)
                this.Write(items[offset++]);

            // quick add
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                uint buffer = // PRIMARY_BUFFER_ITEM_COUNT entries
                    ((items[offset + 0] & MASK) << (ITEM_SIZEOF_IN_BITS * 0)) |
                    ((items[offset + 1] & MASK) << (ITEM_SIZEOF_IN_BITS * 1)) |
                    ((items[offset + 2] & MASK) << (ITEM_SIZEOF_IN_BITS * 2)) |
                    ((items[offset + 3] & MASK) << (ITEM_SIZEOF_IN_BITS * 3)) |
                    ((items[offset + 4] & MASK) << (ITEM_SIZEOF_IN_BITS * 4)) |
                    ((items[offset + 5] & MASK) << (ITEM_SIZEOF_IN_BITS * 5)) |
                    ((items[offset + 6] & MASK) << (ITEM_SIZEOF_IN_BITS * 6)) |
                    ((items[offset + 7] & MASK) << (ITEM_SIZEOF_IN_BITS * 7)) |
                    ((items[offset + 8] & MASK) << (ITEM_SIZEOF_IN_BITS * 8)) |
                    ((items[offset + 9] & MASK) << (ITEM_SIZEOF_IN_BITS * 9)) |
                    ((items[offset + 10] & MASK) << (ITEM_SIZEOF_IN_BITS * 10)) |
                    ((items[offset + 11] & MASK) << (ITEM_SIZEOF_IN_BITS * 11)) |
                    ((items[offset + 12] & MASK) << (ITEM_SIZEOF_IN_BITS * 12)) |
                    ((items[offset + 13] & MASK) << (ITEM_SIZEOF_IN_BITS * 13)) |
                    ((items[offset + 14] & MASK) << (ITEM_SIZEOF_IN_BITS * 14)) |
                    ((items[offset + 15] & MASK) << (ITEM_SIZEOF_IN_BITS * 15)) |
                    ((items[offset + 16] & MASK) << (ITEM_SIZEOF_IN_BITS * 16)) |
                    ((items[offset + 17] & MASK) << (ITEM_SIZEOF_IN_BITS * 17)) |
                    ((items[offset + 18] & MASK) << (ITEM_SIZEOF_IN_BITS * 18)) |
                    ((items[offset + 19] & MASK) << (ITEM_SIZEOF_IN_BITS * 19)) |
                    ((items[offset + 20] & MASK) << (ITEM_SIZEOF_IN_BITS * 20)) |
                    ((items[offset + 21] & MASK) << (ITEM_SIZEOF_IN_BITS * 21)) |
                    ((items[offset + 22] & MASK) << (ITEM_SIZEOF_IN_BITS * 22)) |
                    ((items[offset + 23] & MASK) << (ITEM_SIZEOF_IN_BITS * 23)) |
                    ((items[offset + 24] & MASK) << (ITEM_SIZEOF_IN_BITS * 24)) |
                    ((items[offset + 25] & MASK) << (ITEM_SIZEOF_IN_BITS * 25)) |
                    ((items[offset + 26] & MASK) << (ITEM_SIZEOF_IN_BITS * 26)) |
                    ((items[offset + 27] & MASK) << (ITEM_SIZEOF_IN_BITS * 27)) |
                    ((items[offset + 28] & MASK) << (ITEM_SIZEOF_IN_BITS * 28)) |
                    ((items[offset + 29] & MASK) << (ITEM_SIZEOF_IN_BITS * 29)) |
                    ((items[offset + 30] & MASK) << (ITEM_SIZEOF_IN_BITS * 30)) |
                    ((items[offset + 31] & MASK) << (ITEM_SIZEOF_IN_BITS * 31));
                offset += 32;

                this.FlushPrimaryBuffer(buffer);

                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                this.Write(items[offset++]);
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ITEM_SIZEOF_IN_BITS);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.InternalResume(resumableChannels, rowCount, ITEM_SIZEOF_IN_BITS);
        }

        [MethodImpl(AggressiveInlining)]
        public void Write(byte item) {
            m_primaryBuffer |= (item & MASK) << m_primaryBufferIndex;
            m_primaryBufferIndex += ITEM_SIZEOF_IN_BITS;

            if(m_primaryBufferIndex == PRIMARY_BUFFER_SIZE_IN_BITS)
                this.FlushPrimaryBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((byte[])values, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(object value) {
            this.Write((byte)value);
        }

        public Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_UInt1();
        }
    }
    #endregion
    #region public class DataStreamWriter_UInt2
    public sealed class DataStreamWriter_UInt2 : DoubleBufferedStreamWriterBase, IDataStreamWriter<byte>, IResumableDataStreamWriter {
        // (almost) same code for UInt1, UInt2, UInt4, UInt16, Int1, Int2, Int4, Int16, bool
        // most difference is constants used, and the quick add code

        private const int ITEM_SIZEOF_IN_BITS       = 2;
        private const int PRIMARY_BUFFER_ITEM_COUNT = PRIMARY_BUFFER_SIZE_IN_BITS / ITEM_SIZEOF_IN_BITS;
        private const uint MASK                     = (1 << ITEM_SIZEOF_IN_BITS) - 1;

        [MethodImpl(AggressiveInlining)]
        public void Write(byte[] items, int offset, int count) {
            //while(count-- > 0)
            //    this.Write(items[offset++]);
            //return;

            // finish primary buffer alignment
            while(m_primaryBufferIndex != 0 && count-- > 0)
                this.Write(items[offset++]);

            // quick add
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                uint buffer = // PRIMARY_BUFFER_ITEM_COUNT entries
                    ((items[offset + 0] & MASK) << (ITEM_SIZEOF_IN_BITS * 0)) |
                    ((items[offset + 1] & MASK) << (ITEM_SIZEOF_IN_BITS * 1)) |
                    ((items[offset + 2] & MASK) << (ITEM_SIZEOF_IN_BITS * 2)) |
                    ((items[offset + 3] & MASK) << (ITEM_SIZEOF_IN_BITS * 3)) |
                    ((items[offset + 4] & MASK) << (ITEM_SIZEOF_IN_BITS * 4)) |
                    ((items[offset + 5] & MASK) << (ITEM_SIZEOF_IN_BITS * 5)) |
                    ((items[offset + 6] & MASK) << (ITEM_SIZEOF_IN_BITS * 6)) |
                    ((items[offset + 7] & MASK) << (ITEM_SIZEOF_IN_BITS * 7)) |
                    ((items[offset + 8] & MASK) << (ITEM_SIZEOF_IN_BITS * 8)) |
                    ((items[offset + 9] & MASK) << (ITEM_SIZEOF_IN_BITS * 9)) |
                    ((items[offset + 10] & MASK) << (ITEM_SIZEOF_IN_BITS * 10)) |
                    ((items[offset + 11] & MASK) << (ITEM_SIZEOF_IN_BITS * 11)) |
                    ((items[offset + 12] & MASK) << (ITEM_SIZEOF_IN_BITS * 12)) |
                    ((items[offset + 13] & MASK) << (ITEM_SIZEOF_IN_BITS * 13)) |
                    ((items[offset + 14] & MASK) << (ITEM_SIZEOF_IN_BITS * 14)) |
                    ((items[offset + 15] & MASK) << (ITEM_SIZEOF_IN_BITS * 15));
                offset += 16;

                this.FlushPrimaryBuffer(buffer);

                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                this.Write(items[offset++]);
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ITEM_SIZEOF_IN_BITS);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.InternalResume(resumableChannels, rowCount, ITEM_SIZEOF_IN_BITS);
        }

        [MethodImpl(AggressiveInlining)]
        public void Write(byte item) {
            m_primaryBuffer |= (item & MASK) << m_primaryBufferIndex;
            m_primaryBufferIndex += ITEM_SIZEOF_IN_BITS;

            if(m_primaryBufferIndex == PRIMARY_BUFFER_SIZE_IN_BITS)
                this.FlushPrimaryBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((byte[])values, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(object value) {
            this.Write((byte)value);
        }

        public Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_UInt2();
        }
    }
    #endregion
    #region public class DataStreamWriter_UInt4
    public sealed class DataStreamWriter_UInt4 : DoubleBufferedStreamWriterBase, IDataStreamWriter<byte>, IResumableDataStreamWriter {
        // (almost) same code for UInt1, UInt2, UInt4, UInt16, Int1, Int2, Int4, Int16, bool
        // most difference is constants used, and the quick add code

        private const int ITEM_SIZEOF_IN_BITS       = 4;
        private const int PRIMARY_BUFFER_ITEM_COUNT = PRIMARY_BUFFER_SIZE_IN_BITS / ITEM_SIZEOF_IN_BITS;
        private const uint MASK                     = (1 << ITEM_SIZEOF_IN_BITS) - 1;

        [MethodImpl(AggressiveInlining)]
        public void Write(byte[] items, int offset, int count) {
            //while(count-- > 0)
            //    this.Write(items[offset++]);
            //return;

            // finish primary buffer alignment
            while(m_primaryBufferIndex != 0 && count-- > 0)
                this.Write(items[offset++]);

            // quick add
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                uint buffer = // PRIMARY_BUFFER_ITEM_COUNT entries
                    ((items[offset + 0] & MASK) << (ITEM_SIZEOF_IN_BITS * 0)) |
                    ((items[offset + 1] & MASK) << (ITEM_SIZEOF_IN_BITS * 1)) |
                    ((items[offset + 2] & MASK) << (ITEM_SIZEOF_IN_BITS * 2)) |
                    ((items[offset + 3] & MASK) << (ITEM_SIZEOF_IN_BITS * 3)) |
                    ((items[offset + 4] & MASK) << (ITEM_SIZEOF_IN_BITS * 4)) |
                    ((items[offset + 5] & MASK) << (ITEM_SIZEOF_IN_BITS * 5)) |
                    ((items[offset + 6] & MASK) << (ITEM_SIZEOF_IN_BITS * 6)) |
                    ((items[offset + 7] & MASK) << (ITEM_SIZEOF_IN_BITS * 7));
                offset += 8;

                this.FlushPrimaryBuffer(buffer);

                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                this.Write(items[offset++]);
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ITEM_SIZEOF_IN_BITS);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.InternalResume(resumableChannels, rowCount, ITEM_SIZEOF_IN_BITS);
        }

        [MethodImpl(AggressiveInlining)]
        public void Write(byte item) {
            m_primaryBuffer |= (item & MASK) << m_primaryBufferIndex;
            m_primaryBufferIndex += ITEM_SIZEOF_IN_BITS;

            if(m_primaryBufferIndex == PRIMARY_BUFFER_SIZE_IN_BITS)
                this.FlushPrimaryBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((byte[])values, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(object value) {
            this.Write((byte)value);
        }

        public Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_UInt4();
        }
    }
    #endregion
    #region public class DataStreamWriter_UInt8
    public sealed class DataStreamWriter_UInt8 : DoubleBufferedStreamWriterBase, IDataStreamWriter<byte>, IResumableDataStreamWriter {
        // exceptionally this class doesn't really use a primary buffer
        private const int ITEM_SIZEOF_IN_BITS = 8;

        [MethodImpl(AggressiveInlining)]
        public void Write(byte[] items, int offset, int count) {
            m_stream.Write(items, offset, count);
            
            //// try to fill the secondary buffer if it is partially filled
            //if(count > 0 && m_secondaryBufferIndex > 0) {
            //    var write = Math.Min(SECONDARY_BUFFER_SIZE - m_secondaryBufferIndex, count);
            //    Buffer.BlockCopy(items, offset, m_secondaryBuffer, m_secondaryBufferIndex, write);
            //    count -= write;
            //    offset += write;
            //    m_secondaryBufferIndex += write;
            //    if(m_secondaryBufferIndex == SECONDARY_BUFFER_SIZE) {
            //        m_secondaryBufferIndex = 0;
            //        m_stream.Write(m_secondaryBuffer, 0, SECONDARY_BUFFER_SIZE);
            //    } else
            //        return;
            //}
            //// if anything remains, write directly as this class isn't intended to act as a bufferedstream
            //if(count > 0)
            //    m_stream.Write(items, offset, count);
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ITEM_SIZEOF_IN_BITS);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.InternalResume(resumableChannels, rowCount, ITEM_SIZEOF_IN_BITS);
        }

        [MethodImpl(AggressiveInlining)]
        public void Write(byte item) {
            m_stream.WriteByte(item);

            //m_secondaryBuffer[m_secondaryBufferIndex++] = item;
            //if(m_secondaryBufferIndex == SECONDARY_BUFFER_SIZE) {
            //    m_secondaryBufferIndex = 0;
            //    m_stream.Write(m_secondaryBuffer, 0, SECONDARY_BUFFER_SIZE);
            //}
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((byte[])values, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(object value) {
            this.Write((byte)value);
        }

        public Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_UInt8();
        }
    }
    #endregion
    #region public class DataStreamWriter_UInt16
    public sealed class DataStreamWriter_UInt16 : DoubleBufferedStreamWriterBase, IDataStreamWriter<ushort>, IResumableDataStreamWriter {
        // (almost) same code for UInt1, UInt2, UInt4, UInt16, Int1, Int2, Int4, Int16, bool
        // most difference is constants used, and the quick add code

        private const int ITEM_SIZEOF_IN_BITS       = 16;
        private const int PRIMARY_BUFFER_ITEM_COUNT = PRIMARY_BUFFER_SIZE_IN_BITS / ITEM_SIZEOF_IN_BITS;

        [MethodImpl(AggressiveInlining)]
        public void Write(ushort[] items, int offset, int count) {
            //while(count-- > 0)
            //    this.Write(items[offset++]);
            //return;

            // finish primary buffer alignment
            while(m_primaryBufferIndex != 0 && count-- > 0)
                this.Write(items[offset++]);

            // quick add
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                uint buffer = // PRIMARY_BUFFER_ITEM_COUNT entries
                    ((uint)items[offset + 0] << (ITEM_SIZEOF_IN_BITS * 0)) |
                    ((uint)items[offset + 1] << (ITEM_SIZEOF_IN_BITS * 1));
                offset += 2;

                this.FlushPrimaryBuffer(buffer);

                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                this.Write(items[offset++]);
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ITEM_SIZEOF_IN_BITS);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.InternalResume(resumableChannels, rowCount, ITEM_SIZEOF_IN_BITS);
        }

        [MethodImpl(AggressiveInlining)]
        public void Write(ushort item) {
            m_primaryBuffer |= (uint)item << m_primaryBufferIndex;
            m_primaryBufferIndex += ITEM_SIZEOF_IN_BITS;

            if(m_primaryBufferIndex == PRIMARY_BUFFER_SIZE_IN_BITS)
                this.FlushPrimaryBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((ushort[])values, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(object value) {
            this.Write((ushort)value);
        }

        public Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_UInt16();
        }
    }
    #endregion

    #region public class DataStreamWriter_Bool
    public sealed class DataStreamWriter_Bool : DoubleBufferedStreamWriterBase, IDataStreamWriter<bool>, IResumableDataStreamWriter {
        // (almost) same code for UInt1, UInt2, UInt4, UInt16, Int1, Int2, Int4, Int16, bool
        // most difference is constants used, and the quick add code

        private const int ITEM_SIZEOF_IN_BITS       = 1;
        private const int PRIMARY_BUFFER_ITEM_COUNT = PRIMARY_BUFFER_SIZE_IN_BITS / ITEM_SIZEOF_IN_BITS;

        [MethodImpl(AggressiveInlining)]
        public void Write(bool[] items, int offset, int count) {
            //while(count-- > 0)
            //    this.Write(items[offset++]);
            //return;

            // finish primary buffer alignment
            while(m_primaryBufferIndex != 0 && count-- > 0)
                this.Write(items[offset++]);

            // quick add
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                uint buffer = // PRIMARY_BUFFER_ITEM_COUNT entries
                    (items[offset + 0] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 0) : (uint)0) |
                    (items[offset + 1] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 1) : (uint)0) |
                    (items[offset + 2] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 2) : (uint)0) |
                    (items[offset + 3] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 3) : (uint)0) |
                    (items[offset + 4] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 4) : (uint)0) |
                    (items[offset + 5] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 5) : (uint)0) |
                    (items[offset + 6] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 6) : (uint)0) |
                    (items[offset + 7] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 7) : (uint)0) |
                    (items[offset + 8] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 8) : (uint)0) |
                    (items[offset + 9] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 9) : (uint)0) |
                    (items[offset + 10] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 10) : (uint)0) |
                    (items[offset + 11] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 11) : (uint)0) |
                    (items[offset + 12] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 12) : (uint)0) |
                    (items[offset + 13] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 13) : (uint)0) |
                    (items[offset + 14] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 14) : (uint)0) |
                    (items[offset + 15] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 15) : (uint)0) |
                    (items[offset + 16] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 16) : (uint)0) |
                    (items[offset + 17] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 17) : (uint)0) |
                    (items[offset + 18] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 18) : (uint)0) |
                    (items[offset + 19] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 19) : (uint)0) |
                    (items[offset + 20] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 20) : (uint)0) |
                    (items[offset + 21] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 21) : (uint)0) |
                    (items[offset + 22] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 22) : (uint)0) |
                    (items[offset + 23] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 23) : (uint)0) |
                    (items[offset + 24] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 24) : (uint)0) |
                    (items[offset + 25] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 25) : (uint)0) |
                    (items[offset + 26] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 26) : (uint)0) |
                    (items[offset + 27] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 27) : (uint)0) |
                    (items[offset + 28] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 28) : (uint)0) |
                    (items[offset + 29] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 29) : (uint)0) |
                    (items[offset + 30] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 30) : (uint)0) |
                    (items[offset + 31] ? (uint)1 << (ITEM_SIZEOF_IN_BITS * 31) : (uint)0);
                offset += 32;

                this.FlushPrimaryBuffer(buffer);

                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                this.Write(items[offset++]);
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ITEM_SIZEOF_IN_BITS);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.InternalResume(resumableChannels, rowCount, ITEM_SIZEOF_IN_BITS);
        }

        [MethodImpl(AggressiveInlining)]
        public void Write(bool item) {
            if(item)
                m_primaryBuffer |= (uint)1 << m_primaryBufferIndex;
            m_primaryBufferIndex += ITEM_SIZEOF_IN_BITS;

            if(m_primaryBufferIndex == PRIMARY_BUFFER_SIZE_IN_BITS)
                this.FlushPrimaryBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((bool[])values, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(object value) {
            this.Write((bool)value);
        }

        public Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_Bool();
        }
    }
    #endregion

    #region public class DataStreamWriter_Int1
    public sealed class DataStreamWriter_Int1 : DoubleBufferedStreamWriterBase, IDataStreamWriter<sbyte>, IResumableDataStreamWriter {
        // (almost) same code for UInt1, UInt2, UInt4, UInt16, Int1, Int2, Int4, Int16, bool
        // most difference is constants used, and the quick add code

        private const int ITEM_SIZEOF_IN_BITS       = 1;
        private const int PRIMARY_BUFFER_ITEM_COUNT = PRIMARY_BUFFER_SIZE_IN_BITS / ITEM_SIZEOF_IN_BITS;
        private const uint MASK                     = (1 << ITEM_SIZEOF_IN_BITS) - 1;

        [MethodImpl(AggressiveInlining)]
        public void Write(sbyte[] items, int offset, int count) {
            //while(count-- > 0)
            //    this.Write(items[offset++]);
            //return;

            // finish primary buffer alignment
            while(m_primaryBufferIndex != 0 && count-- > 0)
                this.Write(items[offset++]);

            // quick add
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                uint buffer = unchecked( // PRIMARY_BUFFER_ITEM_COUNT entries
                    (((uint)items[offset + 0] & MASK) << (ITEM_SIZEOF_IN_BITS * 0)) |
                    (((uint)items[offset + 1] & MASK) << (ITEM_SIZEOF_IN_BITS * 1)) |
                    (((uint)items[offset + 2] & MASK) << (ITEM_SIZEOF_IN_BITS * 2)) |
                    (((uint)items[offset + 3] & MASK) << (ITEM_SIZEOF_IN_BITS * 3)) |
                    (((uint)items[offset + 4] & MASK) << (ITEM_SIZEOF_IN_BITS * 4)) |
                    (((uint)items[offset + 5] & MASK) << (ITEM_SIZEOF_IN_BITS * 5)) |
                    (((uint)items[offset + 6] & MASK) << (ITEM_SIZEOF_IN_BITS * 6)) |
                    (((uint)items[offset + 7] & MASK) << (ITEM_SIZEOF_IN_BITS * 7)) |
                    (((uint)items[offset + 8] & MASK) << (ITEM_SIZEOF_IN_BITS * 8)) |
                    (((uint)items[offset + 9] & MASK) << (ITEM_SIZEOF_IN_BITS * 9)) |
                    (((uint)items[offset + 10] & MASK) << (ITEM_SIZEOF_IN_BITS * 10)) |
                    (((uint)items[offset + 11] & MASK) << (ITEM_SIZEOF_IN_BITS * 11)) |
                    (((uint)items[offset + 12] & MASK) << (ITEM_SIZEOF_IN_BITS * 12)) |
                    (((uint)items[offset + 13] & MASK) << (ITEM_SIZEOF_IN_BITS * 13)) |
                    (((uint)items[offset + 14] & MASK) << (ITEM_SIZEOF_IN_BITS * 14)) |
                    (((uint)items[offset + 15] & MASK) << (ITEM_SIZEOF_IN_BITS * 15)) |
                    (((uint)items[offset + 16] & MASK) << (ITEM_SIZEOF_IN_BITS * 16)) |
                    (((uint)items[offset + 17] & MASK) << (ITEM_SIZEOF_IN_BITS * 17)) |
                    (((uint)items[offset + 18] & MASK) << (ITEM_SIZEOF_IN_BITS * 18)) |
                    (((uint)items[offset + 19] & MASK) << (ITEM_SIZEOF_IN_BITS * 19)) |
                    (((uint)items[offset + 20] & MASK) << (ITEM_SIZEOF_IN_BITS * 20)) |
                    (((uint)items[offset + 21] & MASK) << (ITEM_SIZEOF_IN_BITS * 21)) |
                    (((uint)items[offset + 22] & MASK) << (ITEM_SIZEOF_IN_BITS * 22)) |
                    (((uint)items[offset + 23] & MASK) << (ITEM_SIZEOF_IN_BITS * 23)) |
                    (((uint)items[offset + 24] & MASK) << (ITEM_SIZEOF_IN_BITS * 24)) |
                    (((uint)items[offset + 25] & MASK) << (ITEM_SIZEOF_IN_BITS * 25)) |
                    (((uint)items[offset + 26] & MASK) << (ITEM_SIZEOF_IN_BITS * 26)) |
                    (((uint)items[offset + 27] & MASK) << (ITEM_SIZEOF_IN_BITS * 27)) |
                    (((uint)items[offset + 28] & MASK) << (ITEM_SIZEOF_IN_BITS * 28)) |
                    (((uint)items[offset + 29] & MASK) << (ITEM_SIZEOF_IN_BITS * 29)) |
                    (((uint)items[offset + 30] & MASK) << (ITEM_SIZEOF_IN_BITS * 30)) |
                    (((uint)items[offset + 31] & MASK) << (ITEM_SIZEOF_IN_BITS * 31)));
                offset += 32;

                this.FlushPrimaryBuffer(buffer);

                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                this.Write(items[offset++]);
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ITEM_SIZEOF_IN_BITS);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.InternalResume(resumableChannels, rowCount, ITEM_SIZEOF_IN_BITS);
        }

        [MethodImpl(AggressiveInlining)]
        public void Write(sbyte item) {
            m_primaryBuffer |= unchecked(((uint)item & MASK)) << m_primaryBufferIndex;
            m_primaryBufferIndex += ITEM_SIZEOF_IN_BITS;

            if(m_primaryBufferIndex == PRIMARY_BUFFER_SIZE_IN_BITS)
                this.FlushPrimaryBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((sbyte[])values, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(object value) {
            this.Write((sbyte)value);
        }

        public Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_Int1();
        }
    }
    #endregion
    #region public class DataStreamWriter_Int2
    public sealed class DataStreamWriter_Int2 : DoubleBufferedStreamWriterBase, IDataStreamWriter<sbyte>, IResumableDataStreamWriter {
        // (almost) same code for UInt1, UInt2, UInt4, UInt16, Int1, Int2, Int4, Int16, bool
        // most difference is constants used, and the quick add code

        private const int ITEM_SIZEOF_IN_BITS       = 2;
        private const int PRIMARY_BUFFER_ITEM_COUNT = PRIMARY_BUFFER_SIZE_IN_BITS / ITEM_SIZEOF_IN_BITS;
        private const uint MASK                     = (1 << ITEM_SIZEOF_IN_BITS) - 1;

        [MethodImpl(AggressiveInlining)]
        public void Write(sbyte[] items, int offset, int count) {
            //while(count-- > 0)
            //    this.Write(items[offset++]);
            //return;

            // finish primary buffer alignment
            while(m_primaryBufferIndex != 0 && count-- > 0)
                this.Write(items[offset++]);

            // quick add
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                uint buffer = unchecked( // PRIMARY_BUFFER_ITEM_COUNT entries
                    (((uint)items[offset + 0] & MASK) << (ITEM_SIZEOF_IN_BITS * 0)) |
                    (((uint)items[offset + 1] & MASK) << (ITEM_SIZEOF_IN_BITS * 1)) |
                    (((uint)items[offset + 2] & MASK) << (ITEM_SIZEOF_IN_BITS * 2)) |
                    (((uint)items[offset + 3] & MASK) << (ITEM_SIZEOF_IN_BITS * 3)) |
                    (((uint)items[offset + 4] & MASK) << (ITEM_SIZEOF_IN_BITS * 4)) |
                    (((uint)items[offset + 5] & MASK) << (ITEM_SIZEOF_IN_BITS * 5)) |
                    (((uint)items[offset + 6] & MASK) << (ITEM_SIZEOF_IN_BITS * 6)) |
                    (((uint)items[offset + 7] & MASK) << (ITEM_SIZEOF_IN_BITS * 7)) |
                    (((uint)items[offset + 8] & MASK) << (ITEM_SIZEOF_IN_BITS * 8)) |
                    (((uint)items[offset + 9] & MASK) << (ITEM_SIZEOF_IN_BITS * 9)) |
                    (((uint)items[offset + 10] & MASK) << (ITEM_SIZEOF_IN_BITS * 10)) |
                    (((uint)items[offset + 11] & MASK) << (ITEM_SIZEOF_IN_BITS * 11)) |
                    (((uint)items[offset + 12] & MASK) << (ITEM_SIZEOF_IN_BITS * 12)) |
                    (((uint)items[offset + 13] & MASK) << (ITEM_SIZEOF_IN_BITS * 13)) |
                    (((uint)items[offset + 14] & MASK) << (ITEM_SIZEOF_IN_BITS * 14)) |
                    (((uint)items[offset + 15] & MASK) << (ITEM_SIZEOF_IN_BITS * 15)));
                offset += 16;

                this.FlushPrimaryBuffer(buffer);

                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                this.Write(items[offset++]);
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ITEM_SIZEOF_IN_BITS);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.InternalResume(resumableChannels, rowCount, ITEM_SIZEOF_IN_BITS);
        }

        [MethodImpl(AggressiveInlining)]
        public void Write(sbyte item) {
            m_primaryBuffer |= unchecked(((uint)item & MASK)) << m_primaryBufferIndex;
            m_primaryBufferIndex += ITEM_SIZEOF_IN_BITS;

            if(m_primaryBufferIndex == PRIMARY_BUFFER_SIZE_IN_BITS)
                this.FlushPrimaryBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((sbyte[])values, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(object value) {
            this.Write((sbyte)value);
        }

        public Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_Int2();
        }
    }
    #endregion
    #region public class DataStreamWriter_Int4
    public sealed class DataStreamWriter_Int4 : DoubleBufferedStreamWriterBase, IDataStreamWriter<sbyte>, IResumableDataStreamWriter {
        // (almost) same code for UInt1, UInt2, UInt4, UInt16, Int1, Int2, Int4, Int16, bool
        // most difference is constants used, and the quick add code

        private const int ITEM_SIZEOF_IN_BITS       = 4;
        private const int PRIMARY_BUFFER_ITEM_COUNT = PRIMARY_BUFFER_SIZE_IN_BITS / ITEM_SIZEOF_IN_BITS;
        private const uint MASK                     = (1 << ITEM_SIZEOF_IN_BITS) - 1;

        [MethodImpl(AggressiveInlining)]
        public void Write(sbyte[] items, int offset, int count) {
            //while(count-- > 0)
            //    this.Write(items[offset++]);
            //return;

            // finish primary buffer alignment
            while(m_primaryBufferIndex != 0 && count-- > 0)
                this.Write(items[offset++]);

            // quick add
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                uint buffer = unchecked( // PRIMARY_BUFFER_ITEM_COUNT entries
                    (((uint)items[offset + 0] & MASK) << (ITEM_SIZEOF_IN_BITS * 0)) |
                    (((uint)items[offset + 1] & MASK) << (ITEM_SIZEOF_IN_BITS * 1)) |
                    (((uint)items[offset + 2] & MASK) << (ITEM_SIZEOF_IN_BITS * 2)) |
                    (((uint)items[offset + 3] & MASK) << (ITEM_SIZEOF_IN_BITS * 3)) |
                    (((uint)items[offset + 4] & MASK) << (ITEM_SIZEOF_IN_BITS * 4)) |
                    (((uint)items[offset + 5] & MASK) << (ITEM_SIZEOF_IN_BITS * 5)) |
                    (((uint)items[offset + 6] & MASK) << (ITEM_SIZEOF_IN_BITS * 6)) |
                    (((uint)items[offset + 7] & MASK) << (ITEM_SIZEOF_IN_BITS * 7)));
                offset += 8;

                this.FlushPrimaryBuffer(buffer);

                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                this.Write(items[offset++]);
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ITEM_SIZEOF_IN_BITS);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.InternalResume(resumableChannels, rowCount, ITEM_SIZEOF_IN_BITS);
        }

        [MethodImpl(AggressiveInlining)]
        public void Write(sbyte item) {
            m_primaryBuffer |= unchecked(((uint)item & MASK)) << m_primaryBufferIndex;
            m_primaryBufferIndex += ITEM_SIZEOF_IN_BITS;

            if(m_primaryBufferIndex == PRIMARY_BUFFER_SIZE_IN_BITS)
                this.FlushPrimaryBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((sbyte[])values, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(object value) {
            this.Write((sbyte)value);
        }

        public Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_Int4();
        }
    }
    #endregion
    #region public class DataStreamWriter_Int8
    public sealed class DataStreamWriter_Int8 : DoubleBufferedStreamWriterBase, IDataStreamWriter<sbyte>, IResumableDataStreamWriter {
        // exceptionally this class doesn't really use a primary buffer
        private const int ITEM_SIZEOF_IN_BITS = 8;

        [MethodImpl(AggressiveInlining)]
        public void Write(sbyte[] items, int offset, int count) {
            while(count > 0) {
                var write = Math.Min(SECONDARY_BUFFER_SIZE - m_secondaryBufferIndex, count);
                
                Buffer.BlockCopy(items, offset, m_secondaryBuffer, m_secondaryBufferIndex, write);

                count -= write;
                offset += write;
                m_secondaryBufferIndex += write;

                if(m_secondaryBufferIndex == SECONDARY_BUFFER_SIZE) {
                    m_secondaryBufferIndex = 0;
                    m_stream.Write(m_secondaryBuffer, 0, SECONDARY_BUFFER_SIZE);
                }
            }
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ITEM_SIZEOF_IN_BITS);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.InternalResume(resumableChannels, rowCount, ITEM_SIZEOF_IN_BITS);
        }

        [MethodImpl(AggressiveInlining)]
        public void Write(sbyte item) {
            m_secondaryBuffer[m_secondaryBufferIndex++] = unchecked((byte)item);

            if(m_secondaryBufferIndex == SECONDARY_BUFFER_SIZE) {
                m_secondaryBufferIndex = 0;
                m_stream.Write(m_secondaryBuffer, 0, SECONDARY_BUFFER_SIZE);
            }
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((sbyte[])values, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(object value) {
            this.Write((sbyte)value);
        }

        public Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_Int8();
        }
    }
    #endregion
    #region public class DataStreamWriter_Int16
    public sealed class DataStreamWriter_Int16 : DoubleBufferedStreamWriterBase, IDataStreamWriter<short>, IResumableDataStreamWriter {
        // (almost) same code for UInt1, UInt2, UInt4, UInt16, Int1, Int2, Int4, Int16, bool
        // most difference is constants used, and the quick add code

        private const int ITEM_SIZEOF_IN_BITS       = 16;
        private const int PRIMARY_BUFFER_ITEM_COUNT = PRIMARY_BUFFER_SIZE_IN_BITS / ITEM_SIZEOF_IN_BITS;

        [MethodImpl(AggressiveInlining)]
        public void Write(short[] items, int offset, int count) {
            //while(count-- > 0)
            //    this.Write(items[offset++]);
            //return;

            // finish primary buffer alignment
            while(m_primaryBufferIndex != 0 && count-- > 0)
                this.Write(items[offset++]);

            // quick add
            while(count >= PRIMARY_BUFFER_ITEM_COUNT) {
                uint buffer = unchecked( // PRIMARY_BUFFER_ITEM_COUNT entries
                    ((uint)items[offset + 0] << (ITEM_SIZEOF_IN_BITS * 0)) |
                    ((uint)items[offset + 1] << (ITEM_SIZEOF_IN_BITS * 1)));
                offset += 2;

                this.FlushPrimaryBuffer(buffer);

                count -= PRIMARY_BUFFER_ITEM_COUNT;
            }

            // then finish whatever remains
            while(count-- > 0)
                this.Write(items[offset++]);
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ITEM_SIZEOF_IN_BITS);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.InternalResume(resumableChannels, rowCount, ITEM_SIZEOF_IN_BITS);
        }

        [MethodImpl(AggressiveInlining)]
        public void Write(short item) {
            m_primaryBuffer |= unchecked((uint)item) << m_primaryBufferIndex;
            m_primaryBufferIndex += ITEM_SIZEOF_IN_BITS;

            if(m_primaryBufferIndex == PRIMARY_BUFFER_SIZE_IN_BITS)
                this.FlushPrimaryBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((short[])values, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(object value) {
            this.Write((short)value);
        }

        public Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_Int16();
        }
    }
    #endregion
}