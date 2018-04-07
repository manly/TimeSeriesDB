using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using static System.Runtime.CompilerServices.MethodImplOptions;
using System.IO;


namespace TimeSeriesDB.DataStreams.Readers.WithDecoders
{
    using Internal;
    using BaseClasses;

    #region public class DeltaDelta_DataStreamReader_UInt64_LSB
    public sealed class DeltaDelta_DataStreamReader_UInt64_LSB : DataStreamReaderWrapperBase_DeltaDelta<ulong> {
        private ulong m_prev = 0;

        public DeltaDelta_DataStreamReader_UInt64_LSB() : base(new DataStreamReader_UInt64_LSB(), sizeof(ulong)) { }

        protected override void ReadFullFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            this.Delta(ref m_buffer[index + 0], min);
            this.Delta(ref m_buffer[index + 1], min);
            this.Delta(ref m_buffer[index + 2], min);
            this.Delta(ref m_buffer[index + 3], min);
            this.Delta(ref m_buffer[index + 4], min);
            this.Delta(ref m_buffer[index + 5], min);
            this.Delta(ref m_buffer[index + 6], min);
            this.Delta(ref m_buffer[index + 7], min);
            this.Delta(ref m_buffer[index + 8], min);
            this.Delta(ref m_buffer[index + 9], min);
            this.Delta(ref m_buffer[index + 10], min);
            this.Delta(ref m_buffer[index + 11], min);
            this.Delta(ref m_buffer[index + 12], min);
            this.Delta(ref m_buffer[index + 13], min);
            this.Delta(ref m_buffer[index + 14], min);
            this.Delta(ref m_buffer[index + 15], min);

            //m_bufferIndex += 17;
        }

        protected override void ReadPartialFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            while(index != m_frameEndIndex)
                this.Delta(ref m_buffer[index++], min);
        }

        [MethodImpl(AggressiveInlining)]
        private void Delta(ref ulong frameItem, ulong min) {
            var current = frameItem;
            frameItem = Helper.DeltaAdd(m_prev, current + min);
            m_prev = frameItem;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.DeltaDelta_DataStreamWriter_UInt64_LSB();
        }
    }
    #endregion
    #region public class DeltaDelta_DataStreamReader_UInt64
    public sealed class DeltaDelta_DataStreamReader_UInt64 : DataStreamReaderWrapperBase_DeltaDelta<ulong> {
        private ulong m_prev = 0;

        public DeltaDelta_DataStreamReader_UInt64() : base(new DataStreamReader_UInt64(), sizeof(ulong)) { }

        protected override void ReadFullFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            this.Delta(ref m_buffer[index + 0], min);
            this.Delta(ref m_buffer[index + 1], min);
            this.Delta(ref m_buffer[index + 2], min);
            this.Delta(ref m_buffer[index + 3], min);
            this.Delta(ref m_buffer[index + 4], min);
            this.Delta(ref m_buffer[index + 5], min);
            this.Delta(ref m_buffer[index + 6], min);
            this.Delta(ref m_buffer[index + 7], min);
            this.Delta(ref m_buffer[index + 8], min);
            this.Delta(ref m_buffer[index + 9], min);
            this.Delta(ref m_buffer[index + 10], min);
            this.Delta(ref m_buffer[index + 11], min);
            this.Delta(ref m_buffer[index + 12], min);
            this.Delta(ref m_buffer[index + 13], min);
            this.Delta(ref m_buffer[index + 14], min);
            this.Delta(ref m_buffer[index + 15], min);

            //m_bufferIndex += 17;
        }

        protected override void ReadPartialFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            while(index != m_frameEndIndex)
                this.Delta(ref m_buffer[index++], min);
        }

        [MethodImpl(AggressiveInlining)]
        private void Delta(ref ulong frameItem, ulong min) {
            var current = frameItem;
            frameItem = Helper.DeltaAdd(m_prev, current + min);
            m_prev = frameItem;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.DeltaDelta_DataStreamWriter_UInt64();
        }
    }
    #endregion
    #region public class DeltaDelta_DataStreamReader_UInt32
    public sealed class DeltaDelta_DataStreamReader_UInt32 : DataStreamReaderWrapperBase_DeltaDelta<uint> {
        private uint m_prev = 0;

        public DeltaDelta_DataStreamReader_UInt32() : base(new DataStreamReader_UInt32(), sizeof(uint)) { }

        protected override void ReadFullFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            this.Delta(ref m_buffer[index + 0], min);
            this.Delta(ref m_buffer[index + 1], min);
            this.Delta(ref m_buffer[index + 2], min);
            this.Delta(ref m_buffer[index + 3], min);
            this.Delta(ref m_buffer[index + 4], min);
            this.Delta(ref m_buffer[index + 5], min);
            this.Delta(ref m_buffer[index + 6], min);
            this.Delta(ref m_buffer[index + 7], min);
            this.Delta(ref m_buffer[index + 8], min);
            this.Delta(ref m_buffer[index + 9], min);
            this.Delta(ref m_buffer[index + 10], min);
            this.Delta(ref m_buffer[index + 11], min);
            this.Delta(ref m_buffer[index + 12], min);
            this.Delta(ref m_buffer[index + 13], min);
            this.Delta(ref m_buffer[index + 14], min);
            this.Delta(ref m_buffer[index + 15], min);

            //m_bufferIndex += 17;
        }

        protected override void ReadPartialFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            while(index != m_frameEndIndex)
                this.Delta(ref m_buffer[index++], min);
        }

        [MethodImpl(AggressiveInlining)]
        private void Delta(ref uint frameItem, uint min) {
            var current = frameItem;
            frameItem = Helper.DeltaAdd(m_prev, current + min);
            m_prev = frameItem;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.DeltaDelta_DataStreamWriter_UInt32();
        }
    }
    #endregion
    #region public class DeltaDelta_DataStreamReader_UInt16
    public sealed class DeltaDelta_DataStreamReader_UInt16 : DataStreamReaderWrapperBase_DeltaDelta<ushort> {
        private ushort m_prev = 0;

        public DeltaDelta_DataStreamReader_UInt16() : base(new DataStreamReader_UInt16(), sizeof(ushort)) { }

        protected override void ReadFullFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            this.Delta(ref m_buffer[index + 0], min);
            this.Delta(ref m_buffer[index + 1], min);
            this.Delta(ref m_buffer[index + 2], min);
            this.Delta(ref m_buffer[index + 3], min);
            this.Delta(ref m_buffer[index + 4], min);
            this.Delta(ref m_buffer[index + 5], min);
            this.Delta(ref m_buffer[index + 6], min);
            this.Delta(ref m_buffer[index + 7], min);
            this.Delta(ref m_buffer[index + 8], min);
            this.Delta(ref m_buffer[index + 9], min);
            this.Delta(ref m_buffer[index + 10], min);
            this.Delta(ref m_buffer[index + 11], min);
            this.Delta(ref m_buffer[index + 12], min);
            this.Delta(ref m_buffer[index + 13], min);
            this.Delta(ref m_buffer[index + 14], min);
            this.Delta(ref m_buffer[index + 15], min);

            //m_bufferIndex += 17;
        }

        protected override void ReadPartialFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            while(index != m_frameEndIndex)
                this.Delta(ref m_buffer[index++], min);
        }

        [MethodImpl(AggressiveInlining)]
        private void Delta(ref ushort frameItem, ushort min) {
            var current = frameItem;
            frameItem = unchecked((ushort)(Helper.DeltaAdd(m_prev, (ushort)(current + min))));
            m_prev = frameItem;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.DeltaDelta_DataStreamWriter_UInt16();
        }
    }
    #endregion
    #region public class DeltaDelta_DataStreamReader_UInt8
    public sealed class DeltaDelta_DataStreamReader_UInt8 : DataStreamReaderWrapperBase_DeltaDelta<byte> {
        private byte m_prev = 0;

        public DeltaDelta_DataStreamReader_UInt8() : base(new DataStreamReader_UInt8(), sizeof(byte)) { }

        protected override void ReadFullFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            this.Delta(ref m_buffer[index + 0], min);
            this.Delta(ref m_buffer[index + 1], min);
            this.Delta(ref m_buffer[index + 2], min);
            this.Delta(ref m_buffer[index + 3], min);
            this.Delta(ref m_buffer[index + 4], min);
            this.Delta(ref m_buffer[index + 5], min);
            this.Delta(ref m_buffer[index + 6], min);
            this.Delta(ref m_buffer[index + 7], min);
            this.Delta(ref m_buffer[index + 8], min);
            this.Delta(ref m_buffer[index + 9], min);
            this.Delta(ref m_buffer[index + 10], min);
            this.Delta(ref m_buffer[index + 11], min);
            this.Delta(ref m_buffer[index + 12], min);
            this.Delta(ref m_buffer[index + 13], min);
            this.Delta(ref m_buffer[index + 14], min);
            this.Delta(ref m_buffer[index + 15], min);

            //m_bufferIndex += 17;
        }

        protected override void ReadPartialFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            while(index != m_frameEndIndex)
                this.Delta(ref m_buffer[index++], min);
        }

        [MethodImpl(AggressiveInlining)]
        private void Delta(ref byte frameItem, byte min) {
            var current = frameItem;
            frameItem = unchecked((byte)(Helper.DeltaAdd(m_prev, (byte)(current + min))));
            m_prev = frameItem;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.DeltaDelta_DataStreamWriter_UInt8();
        }
    }
    #endregion

    #region public class DeltaDelta_DataStreamReader_Int64_LSB
    public sealed class DeltaDelta_DataStreamReader_Int64_LSB : DataStreamReaderWrapperBase_DeltaDelta<long> {
        private long m_prev = 0;

        public DeltaDelta_DataStreamReader_Int64_LSB() : base(new DataStreamReader_Int64_LSB(), sizeof(long)) { }

        protected override void ReadFullFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            this.Delta(ref m_buffer[index + 0], min);
            this.Delta(ref m_buffer[index + 1], min);
            this.Delta(ref m_buffer[index + 2], min);
            this.Delta(ref m_buffer[index + 3], min);
            this.Delta(ref m_buffer[index + 4], min);
            this.Delta(ref m_buffer[index + 5], min);
            this.Delta(ref m_buffer[index + 6], min);
            this.Delta(ref m_buffer[index + 7], min);
            this.Delta(ref m_buffer[index + 8], min);
            this.Delta(ref m_buffer[index + 9], min);
            this.Delta(ref m_buffer[index + 10], min);
            this.Delta(ref m_buffer[index + 11], min);
            this.Delta(ref m_buffer[index + 12], min);
            this.Delta(ref m_buffer[index + 13], min);
            this.Delta(ref m_buffer[index + 14], min);
            this.Delta(ref m_buffer[index + 15], min);

            //m_bufferIndex += 17;
        }

        protected override void ReadPartialFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            while(index != m_frameEndIndex)
                this.Delta(ref m_buffer[index++], min);
        }

        [MethodImpl(AggressiveInlining)]
        private void Delta(ref long frameItem, long min) {
            var current = frameItem;
            frameItem = m_prev + current + min;
            m_prev = frameItem;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.DeltaDelta_DataStreamWriter_Int64_LSB();
        }
    }
    #endregion
    #region public class DeltaDelta_DataStreamReader_Int64
    public sealed class DeltaDelta_DataStreamReader_Int64 : DataStreamReaderWrapperBase_DeltaDelta<long> {
        private long m_prev = 0;

        public DeltaDelta_DataStreamReader_Int64() : base(new DataStreamReader_Int64(), sizeof(long)) { }

        protected override void ReadFullFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            this.Delta(ref m_buffer[index + 0], min);
            this.Delta(ref m_buffer[index + 1], min);
            this.Delta(ref m_buffer[index + 2], min);
            this.Delta(ref m_buffer[index + 3], min);
            this.Delta(ref m_buffer[index + 4], min);
            this.Delta(ref m_buffer[index + 5], min);
            this.Delta(ref m_buffer[index + 6], min);
            this.Delta(ref m_buffer[index + 7], min);
            this.Delta(ref m_buffer[index + 8], min);
            this.Delta(ref m_buffer[index + 9], min);
            this.Delta(ref m_buffer[index + 10], min);
            this.Delta(ref m_buffer[index + 11], min);
            this.Delta(ref m_buffer[index + 12], min);
            this.Delta(ref m_buffer[index + 13], min);
            this.Delta(ref m_buffer[index + 14], min);
            this.Delta(ref m_buffer[index + 15], min);

            //m_bufferIndex += 17;
        }

        protected override void ReadPartialFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            while(index != m_frameEndIndex)
                this.Delta(ref m_buffer[index++], min);
        }

        [MethodImpl(AggressiveInlining)]
        private void Delta(ref long frameItem, long min) {
            var current = frameItem;
            frameItem = m_prev + current + min;
            m_prev = frameItem;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.DeltaDelta_DataStreamWriter_Int64();
        }
    }
    #endregion
    #region public class DeltaDelta_DataStreamReader_Int32
    public sealed class DeltaDelta_DataStreamReader_Int32 : DataStreamReaderWrapperBase_DeltaDelta<int> {
        private int m_prev = 0;

        public DeltaDelta_DataStreamReader_Int32() : base(new DataStreamReader_Int32(), sizeof(int)) { }

        protected override void ReadFullFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            this.Delta(ref m_buffer[index + 0], min);
            this.Delta(ref m_buffer[index + 1], min);
            this.Delta(ref m_buffer[index + 2], min);
            this.Delta(ref m_buffer[index + 3], min);
            this.Delta(ref m_buffer[index + 4], min);
            this.Delta(ref m_buffer[index + 5], min);
            this.Delta(ref m_buffer[index + 6], min);
            this.Delta(ref m_buffer[index + 7], min);
            this.Delta(ref m_buffer[index + 8], min);
            this.Delta(ref m_buffer[index + 9], min);
            this.Delta(ref m_buffer[index + 10], min);
            this.Delta(ref m_buffer[index + 11], min);
            this.Delta(ref m_buffer[index + 12], min);
            this.Delta(ref m_buffer[index + 13], min);
            this.Delta(ref m_buffer[index + 14], min);
            this.Delta(ref m_buffer[index + 15], min);

            //m_bufferIndex += 17;
        }

        protected override void ReadPartialFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            while(index != m_frameEndIndex)
                this.Delta(ref m_buffer[index++], min);
        }

        [MethodImpl(AggressiveInlining)]
        private void Delta(ref int frameItem, int min) {
            var current = frameItem;
            frameItem = m_prev + current + min;
            m_prev = frameItem;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.DeltaDelta_DataStreamWriter_Int32();
        }
    }
    #endregion
    #region public class DeltaDelta_DataStreamReader_Int16
    public sealed class DeltaDelta_DataStreamReader_Int16 : DataStreamReaderWrapperBase_DeltaDelta<short> {
        private short m_prev = 0;

        public DeltaDelta_DataStreamReader_Int16() : base(new DataStreamReader_Int16(), sizeof(short)) { }

        protected override void ReadFullFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            this.Delta(ref m_buffer[index + 0], min);
            this.Delta(ref m_buffer[index + 1], min);
            this.Delta(ref m_buffer[index + 2], min);
            this.Delta(ref m_buffer[index + 3], min);
            this.Delta(ref m_buffer[index + 4], min);
            this.Delta(ref m_buffer[index + 5], min);
            this.Delta(ref m_buffer[index + 6], min);
            this.Delta(ref m_buffer[index + 7], min);
            this.Delta(ref m_buffer[index + 8], min);
            this.Delta(ref m_buffer[index + 9], min);
            this.Delta(ref m_buffer[index + 10], min);
            this.Delta(ref m_buffer[index + 11], min);
            this.Delta(ref m_buffer[index + 12], min);
            this.Delta(ref m_buffer[index + 13], min);
            this.Delta(ref m_buffer[index + 14], min);
            this.Delta(ref m_buffer[index + 15], min);

            //m_bufferIndex += 17;
        }

        protected override void ReadPartialFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            while(index != m_frameEndIndex)
                this.Delta(ref m_buffer[index++], min);
        }

        [MethodImpl(AggressiveInlining)]
        private void Delta(ref short frameItem, short min) {
            var current = frameItem;
            frameItem = unchecked((short)(m_prev + current + min));
            m_prev = frameItem;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.DeltaDelta_DataStreamWriter_Int16();
        }
    }
    #endregion
    #region public class DeltaDelta_DataStreamReader_Int8
    public sealed class DeltaDelta_DataStreamReader_Int8 : DataStreamReaderWrapperBase_DeltaDelta<sbyte> {
        private sbyte m_prev = 0;

        public DeltaDelta_DataStreamReader_Int8() : base(new DataStreamReader_Int8(), sizeof(sbyte)) { }

        protected override void ReadFullFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            this.Delta(ref m_buffer[index + 0], min);
            this.Delta(ref m_buffer[index + 1], min);
            this.Delta(ref m_buffer[index + 2], min);
            this.Delta(ref m_buffer[index + 3], min);
            this.Delta(ref m_buffer[index + 4], min);
            this.Delta(ref m_buffer[index + 5], min);
            this.Delta(ref m_buffer[index + 6], min);
            this.Delta(ref m_buffer[index + 7], min);
            this.Delta(ref m_buffer[index + 8], min);
            this.Delta(ref m_buffer[index + 9], min);
            this.Delta(ref m_buffer[index + 10], min);
            this.Delta(ref m_buffer[index + 11], min);
            this.Delta(ref m_buffer[index + 12], min);
            this.Delta(ref m_buffer[index + 13], min);
            this.Delta(ref m_buffer[index + 14], min);
            this.Delta(ref m_buffer[index + 15], min);

            //m_bufferIndex += 17;
        }

        protected override void ReadPartialFrame() {
            var index = m_bufferIndex;
            var min = m_buffer[index++];

            while(index != m_frameEndIndex)
                this.Delta(ref m_buffer[index++], min);
        }

        [MethodImpl(AggressiveInlining)]
        private void Delta(ref sbyte frameItem, sbyte min) {
            var current = frameItem;
            frameItem = unchecked((sbyte)(m_prev + current + min));
            m_prev = frameItem;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.DeltaDelta_DataStreamWriter_Int8();
        }
    }
    #endregion

    #region public class DeltaDelta_DataStreamReader_DateTime
    public sealed class DeltaDelta_DataStreamReader_DateTime : DataStreamReaderWrapperBase_Complex<DateTime, ulong> {
        public DeltaDelta_DataStreamReader_DateTime() : base(new DeltaDelta_DataStreamReader_UInt64_LSB(), sizeof(ulong)) { }

        protected override DateTime Convert(ulong value) {
            return this.InternalConvert(value);
        }

        protected override void ConvertFromBuffer(DateTime[] values, int offset, int count) {
            var converted = new ReadOnlySpan<ulong>(m_buffer, 0, count).NonPortableCast<ulong, DateTime>();
            converted.CopyTo(new Span<DateTime>(values, offset, count));

            //int read = 0;
            //while(count >= 16) {
            //    values[offset + 0] = this.InternalConvert(m_buffer[read + 0]);
            //    values[offset + 1] = this.InternalConvert(m_buffer[read + 1]);
            //    values[offset + 2] = this.InternalConvert(m_buffer[read + 2]);
            //    values[offset + 3] = this.InternalConvert(m_buffer[read + 3]);
            //    values[offset + 4] = this.InternalConvert(m_buffer[read + 4]);
            //    values[offset + 5] = this.InternalConvert(m_buffer[read + 5]);
            //    values[offset + 6] = this.InternalConvert(m_buffer[read + 6]);
            //    values[offset + 7] = this.InternalConvert(m_buffer[read + 7]);
            //    values[offset + 8] = this.InternalConvert(m_buffer[read + 8]);
            //    values[offset + 9] = this.InternalConvert(m_buffer[read + 9]);
            //    values[offset + 10] = this.InternalConvert(m_buffer[read + 10]);
            //    values[offset + 11] = this.InternalConvert(m_buffer[read + 11]);
            //    values[offset + 12] = this.InternalConvert(m_buffer[read + 12]);
            //    values[offset + 13] = this.InternalConvert(m_buffer[read + 13]);
            //    values[offset + 14] = this.InternalConvert(m_buffer[read + 14]);
            //    values[offset + 15] = this.InternalConvert(m_buffer[read + 15]);
            //    read += 16;
            //    offset += 16;
            //    count -= 16;
            //}
            //while(count-- > 0)
            //    values[offset++] = this.InternalConvert(m_buffer[read++]);
        }

        [MethodImpl(AggressiveInlining)]
        private DateTime InternalConvert(ulong value) {
            return DateTime.FromBinary(unchecked((long)value));
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.DeltaDelta_DataStreamWriter_DateTime();
        }
    }
    #endregion
    #region public class DeltaDelta_DataStreamReader_TimeSpan
    public sealed class DeltaDelta_DataStreamReader_TimeSpan : DataStreamReaderWrapperBase_Complex<TimeSpan, ulong> {
        public DeltaDelta_DataStreamReader_TimeSpan() : base(new DeltaDelta_DataStreamReader_UInt64_LSB(), sizeof(ulong)) { }

        protected override TimeSpan Convert(ulong value) {
            return this.InternalConvert(value);
        }

        protected override void ConvertFromBuffer(TimeSpan[] values, int offset, int count) {
            var converted = new ReadOnlySpan<ulong>(m_buffer, 0, count).NonPortableCast<ulong, TimeSpan>();
            converted.CopyTo(new Span<TimeSpan>(values, offset, count));

            //int read = 0;
            //while(count >= 16) {
            //    values[offset + 0] = this.InternalConvert(m_buffer[read + 0]);
            //    values[offset + 1] = this.InternalConvert(m_buffer[read + 1]);
            //    values[offset + 2] = this.InternalConvert(m_buffer[read + 2]);
            //    values[offset + 3] = this.InternalConvert(m_buffer[read + 3]);
            //    values[offset + 4] = this.InternalConvert(m_buffer[read + 4]);
            //    values[offset + 5] = this.InternalConvert(m_buffer[read + 5]);
            //    values[offset + 6] = this.InternalConvert(m_buffer[read + 6]);
            //    values[offset + 7] = this.InternalConvert(m_buffer[read + 7]);
            //    values[offset + 8] = this.InternalConvert(m_buffer[read + 8]);
            //    values[offset + 9] = this.InternalConvert(m_buffer[read + 9]);
            //    values[offset + 10] = this.InternalConvert(m_buffer[read + 10]);
            //    values[offset + 11] = this.InternalConvert(m_buffer[read + 11]);
            //    values[offset + 12] = this.InternalConvert(m_buffer[read + 12]);
            //    values[offset + 13] = this.InternalConvert(m_buffer[read + 13]);
            //    values[offset + 14] = this.InternalConvert(m_buffer[read + 14]);
            //    values[offset + 15] = this.InternalConvert(m_buffer[read + 15]);
            //    read += 16;
            //    offset += 16;
            //    count -= 16;
            //}
            //while(count-- > 0)
            //    values[offset++] = this.InternalConvert(m_buffer[read++]);
        }

        [MethodImpl(AggressiveInlining)]
        private TimeSpan InternalConvert(ulong value) {
            return new TimeSpan(unchecked((long)value));
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.DeltaDelta_DataStreamWriter_TimeSpan();
        }
    }
    #endregion
}