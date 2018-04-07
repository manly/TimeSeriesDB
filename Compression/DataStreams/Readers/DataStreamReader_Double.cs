﻿#define NON_PORTABLE_CODE

using System;
using System.Runtime.CompilerServices;
using System.Collections.Generic;
using System.IO;


namespace TimeSeriesDB.DataStreams.Readers
{
    using Internal;
    using BaseClasses;

    public sealed class DataStreamReader_Double : DataStreamReaderWrapperBase_Complex<double, ulong> {
        // dont use DataStreamReader_UInt64_LSB for this because we are more likely to write on the high bytes
        public DataStreamReader_Double() : base(new DataStreamReader_UInt64(), sizeof(ulong)) { }

        protected override double Convert(ulong value) {
            return this.InternalConvert(value);
        }

        protected override void ConvertFromBuffer(double[] values, int offset, int count) {
#if NON_PORTABLE_CODE
            var converted = new ReadOnlySpan<ulong>(m_buffer, 0, count).NonPortableCast<ulong, double>();
            converted.CopyTo(new Span<double>(values, offset, count));
#else
            int read = 0;
            while(count >= 16) {
                values[offset + 0] = this.InternalConvert(m_buffer[read + 0]);
                values[offset + 1] = this.InternalConvert(m_buffer[read + 1]);
                values[offset + 2] = this.InternalConvert(m_buffer[read + 2]);
                values[offset + 3] = this.InternalConvert(m_buffer[read + 3]);
                values[offset + 4] = this.InternalConvert(m_buffer[read + 4]);
                values[offset + 5] = this.InternalConvert(m_buffer[read + 5]);
                values[offset + 6] = this.InternalConvert(m_buffer[read + 6]);
                values[offset + 7] = this.InternalConvert(m_buffer[read + 7]);
                values[offset + 8] = this.InternalConvert(m_buffer[read + 8]);
                values[offset + 9] = this.InternalConvert(m_buffer[read + 9]);
                values[offset + 10] = this.InternalConvert(m_buffer[read + 10]);
                values[offset + 11] = this.InternalConvert(m_buffer[read + 11]);
                values[offset + 12] = this.InternalConvert(m_buffer[read + 12]);
                values[offset + 13] = this.InternalConvert(m_buffer[read + 13]);
                values[offset + 14] = this.InternalConvert(m_buffer[read + 14]);
                values[offset + 15] = this.InternalConvert(m_buffer[read + 15]);
                read += 16;
                offset += 16;
                count -= 16;
            }
            while(count-- > 0)
                values[offset++] = this.InternalConvert(m_buffer[read++]);
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private double InternalConvert(ulong value) {
            return BitMethods.ConvertToDouble(value);
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.DataStreamWriter_Double();
        }
    }
}
