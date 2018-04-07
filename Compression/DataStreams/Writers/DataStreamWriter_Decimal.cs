using System;
using System.Runtime.CompilerServices;
using System.IO;


namespace TimeSeriesDB.DataStreams.Writers
{
    using Internal;
    using BaseClasses;
    using System.Collections.Generic;

    public sealed class DataStreamWriter_Decimal : LargeFixedSizedStreamWriterBase, IDataStreamWriter<decimal>, IResumableDataStreamWriter {
        private const int ITEM_SIZEOF_IN_BYTES = 16;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(decimal[] values, int offset, int count) {
            // dont set to "> BUFFER_SIZE/ITEM_SIZE"
            // also, must be a multiple of BUFFER_SIZE/ITEM_SIZE
            const int BATCH_SIZE = 32;
            
            // try to align
            while(m_index != 0 && count-- > 0)
                this.Write(values[offset++]);

            // batch writes
            while(count >= BATCH_SIZE) {
                BitMethods.ConvertToBits(values[offset + 0], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 1], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 2], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 3], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 4], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 5], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 6], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 7], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 8], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 9], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 10], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 11], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 12], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 13], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 14], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 15], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 16], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 17], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 18], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 19], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 20], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 21], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 22], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 23], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 24], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 25], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 26], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 27], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 28], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 29], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 30], m_buffer, ref m_index);
                BitMethods.ConvertToBits(values[offset + 31], m_buffer, ref m_index);
                offset += 32;

                if(m_index == BUFFER_SIZE) {
                    m_index = 0;
                    m_dataStream.Write(m_buffer, 0, BUFFER_SIZE);
                }

                count -= BATCH_SIZE;
            }

            while(count-- > 0)
                this.Write(values[offset++]);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(decimal value) {
            BitMethods.ConvertToBits(value, m_buffer, ref m_index);

            if(m_index == BUFFER_SIZE) {
                m_index = 0;
                m_dataStream.Write(m_buffer, 0, BUFFER_SIZE);
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((decimal[])values, offset, count);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(object value) {
            this.Write((decimal)value);
        }

        public Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_Decimal();
        }

        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.InternalResume(resumableChannels, rowCount, ITEM_SIZEOF_IN_BYTES);
        }
    }
}
