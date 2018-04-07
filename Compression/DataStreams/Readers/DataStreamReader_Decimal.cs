using System;
using System.Runtime.CompilerServices;
using System.Collections.Generic;
using System.IO;


namespace TimeSeriesDB.DataStreams.Readers
{
    using Internal;
    using BaseClasses;

    public sealed class DataStreamReader_Decimal : LargeFixedSizedStreamReaderBase, IDataStreamReader<decimal> {
        private const int ITEM_SIZE = sizeof(decimal); // 128

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Read(decimal[] buffer, int offset, int count) {
            const int BATCH_SIZE = 32; // dont set to "> BUFFER_SIZE/ITEM_SIZE"

            int total = 0;

            // batch reads
            while(count >= BATCH_SIZE) {
                this.EnsureReadable(ITEM_SIZE * BATCH_SIZE);

                buffer[offset + 0] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 1] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 2] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 3] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 4] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 5] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 6] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 7] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 8] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 9] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 10] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 11] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 12] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 13] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 14] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 15] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 16] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 17] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 18] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 19] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 20] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 21] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 22] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 23] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 24] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 25] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 26] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 27] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 28] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 29] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 30] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                buffer[offset + 31] = BitMethods.ConvertToDecimal(m_buffer, ref m_index);
                offset += 32;

                count -= BATCH_SIZE;
                total += BATCH_SIZE;
            }

            while(count > 0) {
                buffer[offset++] = this.ReadOne();

                count--;
                total++;
            }

            return total;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public decimal ReadOne() {
            this.EnsureReadable(ITEM_SIZE);

            return BitMethods.ConvertToDecimal(m_buffer, ref m_index);
        }

        public void Skip(int count) {
            const int BATCH_SIZE = 32; // dont set to "> BUFFER_SIZE/ITEM_SIZE"

            // batch reads
            while(count >= BATCH_SIZE) {
                this.EnsureReadable(ITEM_SIZE * BATCH_SIZE);

                m_index += 16 * BATCH_SIZE;
                count -= BATCH_SIZE;
            }

            while(count > 0) {
                this.ReadOne();
                count--;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Read(Array items, int offset, int count) {
            return this.Read((decimal[])items, offset, count);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        object IDataStreamReader.ReadOne() {
            return this.ReadOne();
        }

        public IEnumerable<decimal> ReadAll() {
            while(true)
                yield return this.ReadOne();
        }

        public Writers.IDataStreamWriter CreateWriter() {
            return new Writers.DataStreamWriter_Decimal();
        }
    }
}
