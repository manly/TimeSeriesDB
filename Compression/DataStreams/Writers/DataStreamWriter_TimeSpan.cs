#define NON_PORTABLE_CODE

using System;
using System.Runtime.CompilerServices;
using System.IO;
using System.Diagnostics;


namespace TimeSeriesDB.DataStreams.Writers
{
    using BaseClasses;

    public sealed class DataStreamWriter_TimeSpan : DataStreamWriterWrapperBase_Complex<TimeSpan, ulong>, IResumableDataStreamWriter {
        public DataStreamWriter_TimeSpan() : base(new DataStreamWriter_UInt64_LSB(), sizeof(ulong)) { }

        protected override ulong Convert(TimeSpan value) {
            return this.InternalConvert(value);
        }

        protected override void ConvertToBuffer(TimeSpan[] values, int offset, int count) {
#if NON_PORTABLE_CODE
            var converted = new ReadOnlySpan<TimeSpan>(values, offset, count).NonPortableCast<TimeSpan, ulong>();
            converted.CopyTo(m_buffer);
#else
            int write = 0;
            while(count >= 16) {
                m_buffer[write + 0] = this.InternalConvert(values[offset + 0]);
                m_buffer[write + 1] = this.InternalConvert(values[offset + 1]);
                m_buffer[write + 2] = this.InternalConvert(values[offset + 2]);
                m_buffer[write + 3] = this.InternalConvert(values[offset + 3]);
                m_buffer[write + 4] = this.InternalConvert(values[offset + 4]);
                m_buffer[write + 5] = this.InternalConvert(values[offset + 5]);
                m_buffer[write + 6] = this.InternalConvert(values[offset + 6]);
                m_buffer[write + 7] = this.InternalConvert(values[offset + 7]);
                m_buffer[write + 8] = this.InternalConvert(values[offset + 8]);
                m_buffer[write + 9] = this.InternalConvert(values[offset + 9]);
                m_buffer[write + 10] = this.InternalConvert(values[offset + 10]);
                m_buffer[write + 11] = this.InternalConvert(values[offset + 11]);
                m_buffer[write + 12] = this.InternalConvert(values[offset + 12]);
                m_buffer[write + 13] = this.InternalConvert(values[offset + 13]);
                m_buffer[write + 14] = this.InternalConvert(values[offset + 14]);
                m_buffer[write + 15] = this.InternalConvert(values[offset + 15]);
                write += 16;
                offset += 16;
                count -= 16;
            }
            while(count-- > 0)
                m_buffer[write++] = this.InternalConvert(values[offset++]);
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ulong InternalConvert(TimeSpan value) {
            return unchecked((ulong)value.Ticks);
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_TimeSpan();
        }
    }
}
