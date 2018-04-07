//#define USE_TWO_CHANNEL // if disabled, will use one channel

using System;
using System.IO;
using System.Runtime.CompilerServices;


namespace TimeSeriesDB.DataStreams.Readers
{
    using Internal;
    using BaseClasses;

    public sealed class DataStreamReader_ByteArray : VarSizedStreamReaderBase, IDataStreamReader<byte[]> {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Read(byte[][] items, int offset, int count) {
            var startCount = count;

            while(count >= 16) {
                items[offset + 0] = this.ReadOne();
                items[offset + 1] = this.ReadOne();
                items[offset + 2] = this.ReadOne();
                items[offset + 3] = this.ReadOne();
                items[offset + 4] = this.ReadOne();
                items[offset + 5] = this.ReadOne();
                items[offset + 6] = this.ReadOne();
                items[offset + 7] = this.ReadOne();
                items[offset + 8] = this.ReadOne();
                items[offset + 9] = this.ReadOne();
                items[offset + 10] = this.ReadOne();
                items[offset + 11] = this.ReadOne();
                items[offset + 12] = this.ReadOne();
                items[offset + 13] = this.ReadOne();
                items[offset + 14] = this.ReadOne();
                items[offset + 15] = this.ReadOne();
                offset += 16;
                count -= 16;
            }
            while(count > 0) {
                items[offset++] = this.ReadOne();
                count--;
            }
            
            return startCount - count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte[] ReadOne() {
#if USE_TWO_CHANNEL
            return BitMethods.DecodeByteArray(m_dataStream, this.ReadNextLength);
#else
            return BitMethods.DecodeByteArray(m_buffer, ref m_offset, ref m_read, m_dataStream);
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Read(Array items, int offset, int count) {
            return this.Read((byte[][])items, offset, count);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        object IDataStreamReader.ReadOne() {
            return this.ReadOne();
        }

        public void Skip(int count) {
            while(count >= 16) {
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                count -= 16;
            }
            while(count-- > 0)
                this.InternalSkipOne();
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InternalSkipOne() {
#if USE_TWO_CHANNEL
            BitMethods.SkipVarSizedObject(m_seekableDataStream, ref m_dataStreamPosition, this.ReadNextLength);
#else
            BitMethods.SkipVarSizedObject(m_buffer, ref m_offset, ref m_read, m_dataStream);
#endif
        }

        public Writers.IDataStreamWriter CreateWriter() {
            return new Writers.DataStreamWriter_ByteArray();
        }
    }
}