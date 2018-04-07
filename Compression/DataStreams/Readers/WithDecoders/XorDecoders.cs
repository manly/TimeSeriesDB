using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using static System.Runtime.CompilerServices.MethodImplOptions;
using System.IO;
using System.Numerics;

//using System.Numerics;
// this code cannot use SIMD optimisations because we need to read the data one depending on the previous one, which precludes the usage of SIMD vector optimisations.


namespace TimeSeriesDB.DataStreams.Readers.WithDecoders
{
    using Internal;
    using BaseClasses;

    #region public class Xor_DataStreamReader_UInt64_LSB
    public sealed class Xor_DataStreamReader_UInt64_LSB : DataStreamReaderWrapperBase<ulong> {
        private ulong m_prev = 0;

        public Xor_DataStreamReader_UInt64_LSB() : base(new DataStreamReader_UInt64_LSB()) { }

        protected override void GetNext(ref ulong value) {
            this.InternalGetNext(ref value);
        }
        protected override void GetNext(ulong[] values, int offset, int count) {
            while(count >= 16) {
                this.InternalGetNext(ref values[offset + 0]);
                this.InternalGetNext(ref values[offset + 1]);
                this.InternalGetNext(ref values[offset + 2]);
                this.InternalGetNext(ref values[offset + 3]);
                this.InternalGetNext(ref values[offset + 4]);
                this.InternalGetNext(ref values[offset + 5]);
                this.InternalGetNext(ref values[offset + 6]);
                this.InternalGetNext(ref values[offset + 7]);
                this.InternalGetNext(ref values[offset + 8]);
                this.InternalGetNext(ref values[offset + 9]);
                this.InternalGetNext(ref values[offset + 10]);
                this.InternalGetNext(ref values[offset + 11]);
                this.InternalGetNext(ref values[offset + 12]);
                this.InternalGetNext(ref values[offset + 13]);
                this.InternalGetNext(ref values[offset + 14]);
                this.InternalGetNext(ref values[offset + 15]);
                offset += 16;
                count -= 16;
            }
            while(count-- > 0)
                this.InternalGetNext(ref values[offset++]);
        }

        [MethodImpl(AggressiveInlining)]
        private void InternalGetNext(ref ulong value) {
            value ^= m_prev;
            m_prev = value;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.Xor_DataStreamWriter_UInt64_LSB();
        }
    }
    #endregion
    #region public class Xor_DataStreamReader_UInt64
    public sealed class Xor_DataStreamReader_UInt64 : DataStreamReaderWrapperBase<ulong> {
        private ulong m_prev = 0;

        public Xor_DataStreamReader_UInt64() : base(new DataStreamReader_UInt64()) { }

        protected override void GetNext(ref ulong value) {
            this.InternalGetNext(ref value);
        }
        protected override void GetNext(ulong[] values, int offset, int count) {
            while(count >= 16) {
                this.InternalGetNext(ref values[offset + 0]);
                this.InternalGetNext(ref values[offset + 1]);
                this.InternalGetNext(ref values[offset + 2]);
                this.InternalGetNext(ref values[offset + 3]);
                this.InternalGetNext(ref values[offset + 4]);
                this.InternalGetNext(ref values[offset + 5]);
                this.InternalGetNext(ref values[offset + 6]);
                this.InternalGetNext(ref values[offset + 7]);
                this.InternalGetNext(ref values[offset + 8]);
                this.InternalGetNext(ref values[offset + 9]);
                this.InternalGetNext(ref values[offset + 10]);
                this.InternalGetNext(ref values[offset + 11]);
                this.InternalGetNext(ref values[offset + 12]);
                this.InternalGetNext(ref values[offset + 13]);
                this.InternalGetNext(ref values[offset + 14]);
                this.InternalGetNext(ref values[offset + 15]);
                offset += 16;
                count -= 16;
            }
            while(count-- > 0)
                this.InternalGetNext(ref values[offset++]);
        }

        [MethodImpl(AggressiveInlining)]
        private void InternalGetNext(ref ulong value) {
            value ^= m_prev;
            m_prev = value;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.Xor_DataStreamWriter_UInt64();
        }
    }
    #endregion
    #region public class Xor_DataStreamReader_UInt32
    public sealed class Xor_DataStreamReader_UInt32 : DataStreamReaderWrapperBase<uint> {
        private uint m_prev = 0;

        public Xor_DataStreamReader_UInt32() : base(new DataStreamReader_UInt32()) { }

        protected override void GetNext(ref uint value) {
            this.InternalGetNext(ref value);
        }
        protected override void GetNext(uint[] values, int offset, int count) {
            while(count >= 16) {
                this.InternalGetNext(ref values[offset + 0]);
                this.InternalGetNext(ref values[offset + 1]);
                this.InternalGetNext(ref values[offset + 2]);
                this.InternalGetNext(ref values[offset + 3]);
                this.InternalGetNext(ref values[offset + 4]);
                this.InternalGetNext(ref values[offset + 5]);
                this.InternalGetNext(ref values[offset + 6]);
                this.InternalGetNext(ref values[offset + 7]);
                this.InternalGetNext(ref values[offset + 8]);
                this.InternalGetNext(ref values[offset + 9]);
                this.InternalGetNext(ref values[offset + 10]);
                this.InternalGetNext(ref values[offset + 11]);
                this.InternalGetNext(ref values[offset + 12]);
                this.InternalGetNext(ref values[offset + 13]);
                this.InternalGetNext(ref values[offset + 14]);
                this.InternalGetNext(ref values[offset + 15]);
                offset += 16;
                count -= 16;
            }
            while(count-- > 0)
                this.InternalGetNext(ref values[offset++]);
        }

        [MethodImpl(AggressiveInlining)]
        private void InternalGetNext(ref uint value) {
            value ^= m_prev;
            m_prev = value;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.Xor_DataStreamWriter_UInt32();
        }
    }
    #endregion
    #region public class Xor_DataStreamReader_UInt16
    public sealed class Xor_DataStreamReader_UInt16 : DataStreamReaderWrapperBase<ushort> {
        private ushort m_prev = 0;

        public Xor_DataStreamReader_UInt16() : base(new DataStreamReader_UInt16()) { }

        protected override void GetNext(ref ushort value) {
            this.InternalGetNext(ref value);
        }
        protected override void GetNext(ushort[] values, int offset, int count) {
            while(count >= 16) {
                this.InternalGetNext(ref values[offset + 0]);
                this.InternalGetNext(ref values[offset + 1]);
                this.InternalGetNext(ref values[offset + 2]);
                this.InternalGetNext(ref values[offset + 3]);
                this.InternalGetNext(ref values[offset + 4]);
                this.InternalGetNext(ref values[offset + 5]);
                this.InternalGetNext(ref values[offset + 6]);
                this.InternalGetNext(ref values[offset + 7]);
                this.InternalGetNext(ref values[offset + 8]);
                this.InternalGetNext(ref values[offset + 9]);
                this.InternalGetNext(ref values[offset + 10]);
                this.InternalGetNext(ref values[offset + 11]);
                this.InternalGetNext(ref values[offset + 12]);
                this.InternalGetNext(ref values[offset + 13]);
                this.InternalGetNext(ref values[offset + 14]);
                this.InternalGetNext(ref values[offset + 15]);
                offset += 16;
                count -= 16;
            }
            while(count-- > 0)
                this.InternalGetNext(ref values[offset++]);
        }

        [MethodImpl(AggressiveInlining)]
        private void InternalGetNext(ref ushort value) {
            value = unchecked((ushort)(m_prev ^ value));
            m_prev = value;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.Xor_DataStreamWriter_UInt16();
        }
    }
    #endregion
    #region public class Xor_DataStreamReader_UInt8
    public sealed class Xor_DataStreamReader_UInt8 : DataStreamReaderWrapperBase<byte> {
        private byte m_prev = 0;

        public Xor_DataStreamReader_UInt8() : base(new DataStreamReader_UInt8()) { }

        protected override void GetNext(ref byte value) {
            this.InternalGetNext(ref value);
        }
        protected override void GetNext(byte[] values, int offset, int count) {
            while(count >= 16) {
                this.InternalGetNext(ref values[offset + 0]);
                this.InternalGetNext(ref values[offset + 1]);
                this.InternalGetNext(ref values[offset + 2]);
                this.InternalGetNext(ref values[offset + 3]);
                this.InternalGetNext(ref values[offset + 4]);
                this.InternalGetNext(ref values[offset + 5]);
                this.InternalGetNext(ref values[offset + 6]);
                this.InternalGetNext(ref values[offset + 7]);
                this.InternalGetNext(ref values[offset + 8]);
                this.InternalGetNext(ref values[offset + 9]);
                this.InternalGetNext(ref values[offset + 10]);
                this.InternalGetNext(ref values[offset + 11]);
                this.InternalGetNext(ref values[offset + 12]);
                this.InternalGetNext(ref values[offset + 13]);
                this.InternalGetNext(ref values[offset + 14]);
                this.InternalGetNext(ref values[offset + 15]);
                offset += 16;
                count -= 16;
            }
            while(count-- > 0)
                this.InternalGetNext(ref values[offset++]);
        }

        [MethodImpl(AggressiveInlining)]
        private void InternalGetNext(ref byte value) {
            value = unchecked((byte)(m_prev ^ value));
            m_prev = value;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.Xor_DataStreamWriter_UInt8();
        }
    }
    #endregion

    #region public class Xor_DataStreamReader_Int64_LSB
    public sealed class Xor_DataStreamReader_Int64_LSB : DataStreamReaderWrapperBase<long> {
        private long m_prev = 0;

        public Xor_DataStreamReader_Int64_LSB() : base(new DataStreamReader_Int64_LSB()) { }

        protected override void GetNext(ref long value) {
            this.InternalGetNext(ref value);
        }
        protected override void GetNext(long[] values, int offset, int count) {
            while(count >= 16) {
                this.InternalGetNext(ref values[offset + 0]);
                this.InternalGetNext(ref values[offset + 1]);
                this.InternalGetNext(ref values[offset + 2]);
                this.InternalGetNext(ref values[offset + 3]);
                this.InternalGetNext(ref values[offset + 4]);
                this.InternalGetNext(ref values[offset + 5]);
                this.InternalGetNext(ref values[offset + 6]);
                this.InternalGetNext(ref values[offset + 7]);
                this.InternalGetNext(ref values[offset + 8]);
                this.InternalGetNext(ref values[offset + 9]);
                this.InternalGetNext(ref values[offset + 10]);
                this.InternalGetNext(ref values[offset + 11]);
                this.InternalGetNext(ref values[offset + 12]);
                this.InternalGetNext(ref values[offset + 13]);
                this.InternalGetNext(ref values[offset + 14]);
                this.InternalGetNext(ref values[offset + 15]);
                offset += 16;
                count -= 16;
            }
            while(count-- > 0)
                this.InternalGetNext(ref values[offset++]);
        }

        [MethodImpl(AggressiveInlining)]
        private void InternalGetNext(ref long value) {
            value ^= m_prev;
            m_prev = value;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.Xor_DataStreamWriter_Int64_LSB();
        }
    }
    #endregion
    #region public class Xor_DataStreamReader_Int64
    public sealed class Xor_DataStreamReader_Int64 : DataStreamReaderWrapperBase<long> {
        private long m_prev = 0;

        public Xor_DataStreamReader_Int64() : base(new DataStreamReader_Int64()) { }

        protected override void GetNext(ref long value) {
            this.InternalGetNext(ref value);
        }
        protected override void GetNext(long[] values, int offset, int count) {
            while(count >= 16) {
                this.InternalGetNext(ref values[offset + 0]);
                this.InternalGetNext(ref values[offset + 1]);
                this.InternalGetNext(ref values[offset + 2]);
                this.InternalGetNext(ref values[offset + 3]);
                this.InternalGetNext(ref values[offset + 4]);
                this.InternalGetNext(ref values[offset + 5]);
                this.InternalGetNext(ref values[offset + 6]);
                this.InternalGetNext(ref values[offset + 7]);
                this.InternalGetNext(ref values[offset + 8]);
                this.InternalGetNext(ref values[offset + 9]);
                this.InternalGetNext(ref values[offset + 10]);
                this.InternalGetNext(ref values[offset + 11]);
                this.InternalGetNext(ref values[offset + 12]);
                this.InternalGetNext(ref values[offset + 13]);
                this.InternalGetNext(ref values[offset + 14]);
                this.InternalGetNext(ref values[offset + 15]);
                offset += 16;
                count -= 16;
            }
            while(count-- > 0)
                this.InternalGetNext(ref values[offset++]);
        }

        [MethodImpl(AggressiveInlining)]
        private void InternalGetNext(ref long value) {
            value ^= m_prev;
            m_prev = value;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.Xor_DataStreamWriter_Int64();
        }
    }
    #endregion
    #region public class Xor_DataStreamReader_Int32
    public sealed class Xor_DataStreamReader_Int32 : DataStreamReaderWrapperBase<int> {
        private int m_prev = 0;

        public Xor_DataStreamReader_Int32() : base(new DataStreamReader_Int32()) { }

        protected override void GetNext(ref int value) {
            this.InternalGetNext(ref value);
        }
        protected override void GetNext(int[] values, int offset, int count) {
            while(count >= 16) {
                this.InternalGetNext(ref values[offset + 0]);
                this.InternalGetNext(ref values[offset + 1]);
                this.InternalGetNext(ref values[offset + 2]);
                this.InternalGetNext(ref values[offset + 3]);
                this.InternalGetNext(ref values[offset + 4]);
                this.InternalGetNext(ref values[offset + 5]);
                this.InternalGetNext(ref values[offset + 6]);
                this.InternalGetNext(ref values[offset + 7]);
                this.InternalGetNext(ref values[offset + 8]);
                this.InternalGetNext(ref values[offset + 9]);
                this.InternalGetNext(ref values[offset + 10]);
                this.InternalGetNext(ref values[offset + 11]);
                this.InternalGetNext(ref values[offset + 12]);
                this.InternalGetNext(ref values[offset + 13]);
                this.InternalGetNext(ref values[offset + 14]);
                this.InternalGetNext(ref values[offset + 15]);
                offset += 16;
                count -= 16;
            }
            while(count-- > 0)
                this.InternalGetNext(ref values[offset++]);
        }

        [MethodImpl(AggressiveInlining)]
        private void InternalGetNext(ref int value) {
            value ^= m_prev;
            m_prev = value;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.Xor_DataStreamWriter_Int32();
        }
    }
    #endregion
    #region public class Xor_DataStreamReader_Int16
    public sealed class Xor_DataStreamReader_Int16 : DataStreamReaderWrapperBase<short> {
        private short m_prev = 0;

        public Xor_DataStreamReader_Int16() : base(new DataStreamReader_Int16()) { }

        protected override void GetNext(ref short value) {
            this.InternalGetNext(ref value);
        }
        protected override void GetNext(short[] values, int offset, int count) {
            while(count >= 16) {
                this.InternalGetNext(ref values[offset + 0]);
                this.InternalGetNext(ref values[offset + 1]);
                this.InternalGetNext(ref values[offset + 2]);
                this.InternalGetNext(ref values[offset + 3]);
                this.InternalGetNext(ref values[offset + 4]);
                this.InternalGetNext(ref values[offset + 5]);
                this.InternalGetNext(ref values[offset + 6]);
                this.InternalGetNext(ref values[offset + 7]);
                this.InternalGetNext(ref values[offset + 8]);
                this.InternalGetNext(ref values[offset + 9]);
                this.InternalGetNext(ref values[offset + 10]);
                this.InternalGetNext(ref values[offset + 11]);
                this.InternalGetNext(ref values[offset + 12]);
                this.InternalGetNext(ref values[offset + 13]);
                this.InternalGetNext(ref values[offset + 14]);
                this.InternalGetNext(ref values[offset + 15]);
                offset += 16;
                count -= 16;
            }
            while(count-- > 0)
                this.InternalGetNext(ref values[offset++]);
        }

        [MethodImpl(AggressiveInlining)]
        private void InternalGetNext(ref short value) {
            value = unchecked((short)(m_prev ^ value));
            m_prev = value;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.Xor_DataStreamWriter_Int16();
        }
    }
    #endregion
    #region public class Xor_DataStreamReader_Int8
    public sealed class Xor_DataStreamReader_Int8 : DataStreamReaderWrapperBase<sbyte> {
        private sbyte m_prev = 0;

        public Xor_DataStreamReader_Int8() : base(new DataStreamReader_Int8()) { }

        protected override void GetNext(ref sbyte value) {
            this.InternalGetNext(ref value);
        }
        protected override void GetNext(sbyte[] values, int offset, int count) {
            while(count >= 16) {
                this.InternalGetNext(ref values[offset + 0]);
                this.InternalGetNext(ref values[offset + 1]);
                this.InternalGetNext(ref values[offset + 2]);
                this.InternalGetNext(ref values[offset + 3]);
                this.InternalGetNext(ref values[offset + 4]);
                this.InternalGetNext(ref values[offset + 5]);
                this.InternalGetNext(ref values[offset + 6]);
                this.InternalGetNext(ref values[offset + 7]);
                this.InternalGetNext(ref values[offset + 8]);
                this.InternalGetNext(ref values[offset + 9]);
                this.InternalGetNext(ref values[offset + 10]);
                this.InternalGetNext(ref values[offset + 11]);
                this.InternalGetNext(ref values[offset + 12]);
                this.InternalGetNext(ref values[offset + 13]);
                this.InternalGetNext(ref values[offset + 14]);
                this.InternalGetNext(ref values[offset + 15]);
                offset += 16;
                count -= 16;
            }
            while(count-- > 0)
                this.InternalGetNext(ref values[offset++]);
        }

        [MethodImpl(AggressiveInlining)]
        private void InternalGetNext(ref sbyte value) {
            value = unchecked((sbyte)(m_prev ^ value));
            m_prev = value;
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.Xor_DataStreamWriter_Int8();
        }
    }
    #endregion

    #region public class Xor_DataStreamReader_DateTime
    public sealed class Xor_DataStreamReader_DateTime : DataStreamReaderWrapperBase_Complex<DateTime, ulong> {
        private ulong m_prev = 0;

        public Xor_DataStreamReader_DateTime() : base(new DataStreamReader_UInt64_LSB(), sizeof(ulong)) { }

        protected override DateTime Convert(ulong value) {
            return this.InternalConvert(value);
        }

        protected override void ConvertFromBuffer(DateTime[] values, int offset, int count) {
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
        }

        [MethodImpl(AggressiveInlining)]
        private DateTime InternalConvert(ulong value) {
            value ^= m_prev;
            m_prev = value;
            return DateTime.FromBinary(unchecked((long)value));
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.Xor_DataStreamWriter_DateTime();
        }
    }
    #endregion
    #region public class Xor_DataStreamReader_TimeSpan
    public sealed class Xor_DataStreamReader_TimeSpan : DataStreamReaderWrapperBase_Complex<TimeSpan, ulong> {
        private ulong m_prev = 0;

        public Xor_DataStreamReader_TimeSpan() : base(new DataStreamReader_UInt64_LSB(), sizeof(ulong)) { }

        protected override TimeSpan Convert(ulong value) {
            return this.InternalConvert(value);
        }

        protected override void ConvertFromBuffer(TimeSpan[] values, int offset, int count) {
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
        }

        [MethodImpl(AggressiveInlining)]
        private TimeSpan InternalConvert(ulong value) {
            value ^= m_prev;
            m_prev = value;
            return new TimeSpan(unchecked((long)value));
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.Xor_DataStreamWriter_TimeSpan();
        }
    }
    #endregion
    #region public class Xor_DataStreamReader_Float
    public sealed class Xor_DataStreamReader_Float : DataStreamReaderWrapperBase_Complex<float, uint> {
        private uint m_prev = 0;

        public Xor_DataStreamReader_Float() : base(new DataStreamReader_UInt32(), sizeof(uint)) { }

        protected override float Convert(uint value) {
            return this.InternalConvert(value);
        }

        protected override void ConvertFromBuffer(float[] values, int offset, int count) {
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
        }

        [MethodImpl(AggressiveInlining)]
        private float InternalConvert(uint value) {
            value ^= m_prev;
            m_prev = value;
            return BitMethods.ConvertToFloat(value);
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.Xor_DataStreamWriter_Float();
        }
    }
    #endregion
    #region public class Xor_DataStreamReader_Double
    public sealed class Xor_DataStreamReader_Double : DataStreamReaderWrapperBase_Complex<double, ulong> {
        private ulong m_prev = 0;

        public Xor_DataStreamReader_Double() : base(new DataStreamReader_UInt64_LSB(), sizeof(ulong)) { }

        protected override double Convert(ulong value) {
            return this.InternalConvert(value);
        }

        protected override void ConvertFromBuffer(double[] values, int offset, int count) {
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
        }

        [MethodImpl(AggressiveInlining)]
        private double InternalConvert(ulong value) {
            value ^= m_prev;
            m_prev = value;
            return BitMethods.ConvertToDouble(value);
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.Xor_DataStreamWriter_Double();
        }
    }
    #endregion
}
