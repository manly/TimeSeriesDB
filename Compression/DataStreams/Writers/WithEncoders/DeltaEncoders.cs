using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using static System.Runtime.CompilerServices.MethodImplOptions;
using System.IO;
using System.Linq;
using System.Numerics;


namespace TimeSeriesDB.DataStreams.Writers.WithEncoders
{
    using Internal;
    using BaseClasses;

    #region public class Delta_DataStreamWriter_UInt64_LSB
    public sealed class Delta_DataStreamWriter_UInt64_LSB : DataStreamWriterWrapperBase<ulong>, IResumableDataStreamWriter {
        private ulong m_prev = 0;

        public Delta_DataStreamWriter_UInt64_LSB() : base(new DataStreamWriter_UInt64_LSB()) { }

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
            var prev = m_prev;
            m_prev = value;
            value = Helper.DeltaRemove(value, prev);
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.Delta_DataStreamReader_UInt64_LSB();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    #region public class Delta_DataStreamWriter_UInt64
    public sealed class Delta_DataStreamWriter_UInt64 : DataStreamWriterWrapperBase<ulong>, IResumableDataStreamWriter {
        private ulong m_prev = 0;

        public Delta_DataStreamWriter_UInt64() : base(new DataStreamWriter_UInt64()) { }

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
            var prev = m_prev;
            m_prev = value;
            value = Helper.DeltaRemove(value, prev);
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.Delta_DataStreamReader_UInt64();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    #region public class Delta_DataStreamWriter_UInt32
    public sealed class Delta_DataStreamWriter_UInt32 : DataStreamWriterWrapperBase<uint>, IResumableDataStreamWriter {
        private uint m_prev = 0;

        public Delta_DataStreamWriter_UInt32() : base(new DataStreamWriter_UInt32()) { }

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
            var prev = m_prev;
            m_prev = value;
            value = Helper.DeltaRemove(value, prev);
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.Delta_DataStreamReader_UInt32();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    #region public class Delta_DataStreamWriter_UInt16
    public sealed class Delta_DataStreamWriter_UInt16 : DataStreamWriterWrapperBase<ushort>, IResumableDataStreamWriter {
        private ushort m_prev = 0;

        public Delta_DataStreamWriter_UInt16() : base(new DataStreamWriter_UInt16()) { }

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
            var prev = m_prev;
            m_prev = value;
            value = Helper.DeltaRemove(value, prev);
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.Delta_DataStreamReader_UInt16();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    #region public class Delta_DataStreamWriter_UInt8
    public sealed class Delta_DataStreamWriter_UInt8 : DataStreamWriterWrapperBase<byte>, IResumableDataStreamWriter {
        private byte m_prev = 0;

        public Delta_DataStreamWriter_UInt8() : base(new DataStreamWriter_UInt8()) { }

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
            var prev = m_prev;
            m_prev = value;
            value = Helper.DeltaRemove(value, prev);
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.Delta_DataStreamReader_UInt8();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    
    #region public class Delta_DataStreamWriter_Int64_LSB
    public sealed class Delta_DataStreamWriter_Int64_LSB : DataStreamWriterWrapperBase<long>, IResumableDataStreamWriter {
        private long m_prev = 0;

        public Delta_DataStreamWriter_Int64_LSB() : base(new DataStreamWriter_Int64_LSB()) { }

        protected override void GetNext(ref long value) {
            this.InternalGetNext(ref value);
        }
        protected override void GetNext(long[] values, int offset, int count) {
            if(Vector.IsHardwareAccelerated) {
                var chunk = Vector<long>.Count;
                if(count > chunk) {
                    var prev = new Vector<long>(values, offset);
                    values[offset] -= m_prev; // this.InternalGetNext(ref values[offset]);
                    Vector<long> current = default;
                    while(count > chunk) {
                        current = new Vector<long>(values, offset + 1);
                        var next = new Vector<long>(values, offset + chunk);
                        (current - prev).CopyTo(values, offset + 1);
                        prev = next;
                        offset += chunk;
                        count -= chunk;
                    }
                    offset++;
                    count--;
                    m_prev = current[chunk - 1];
                }
            } else {
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
            }
            while(count-- > 0)
                this.InternalGetNext(ref values[offset++]);
        }

        [MethodImpl(AggressiveInlining)]
        private void InternalGetNext(ref long value) {
            var prev = m_prev;
            m_prev = value;
            value -= prev;
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.Delta_DataStreamReader_Int64_LSB();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    #region public class Delta_DataStreamWriter_Int64
    public sealed class Delta_DataStreamWriter_Int64 : DataStreamWriterWrapperBase<long>, IResumableDataStreamWriter {
        private long m_prev = 0;

        public Delta_DataStreamWriter_Int64() : base(new DataStreamWriter_Int64()) { }

        protected override void GetNext(ref long value) {
            this.InternalGetNext(ref value);
        }
        protected override void GetNext(long[] values, int offset, int count) {
            if(Vector.IsHardwareAccelerated) {
                var chunk = Vector<long>.Count;
                if(count > chunk) {
                    var prev = new Vector<long>(values, offset);
                    values[offset] -= m_prev; // this.InternalGetNext(ref values[offset]);
                    Vector<long> current = default;
                    while(count > chunk) {
                        current = new Vector<long>(values, offset + 1);
                        var next = new Vector<long>(values, offset + chunk);
                        (current - prev).CopyTo(values, offset + 1);
                        prev = next;
                        offset += chunk;
                        count -= chunk;
                    }
                    offset++;
                    count--;
                    m_prev = current[chunk - 1];
                }
            } else {
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
            }
            while(count-- > 0)
                this.InternalGetNext(ref values[offset++]);
        }

        [MethodImpl(AggressiveInlining)]
        private void InternalGetNext(ref long value) {
            var prev = m_prev;
            m_prev = value;
            value -= prev;
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.Delta_DataStreamReader_Int64();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    #region public class Delta_DataStreamWriter_Int32
    public sealed class Delta_DataStreamWriter_Int32 : DataStreamWriterWrapperBase<int>, IResumableDataStreamWriter {
        private int m_prev = 0;

        public Delta_DataStreamWriter_Int32() : base(new DataStreamWriter_Int32()) { }

        protected override void GetNext(ref int value) {
            this.InternalGetNext(ref value);
        }
        protected override void GetNext(int[] values, int offset, int count) {
            if(Vector.IsHardwareAccelerated) {
                var chunk = Vector<int>.Count;
                if(count > chunk) {
                    var prev = new Vector<int>(values, offset);
                    values[offset] -= m_prev; // this.InternalGetNext(ref values[offset]);
                    Vector<int> current = default;
                    while(count > chunk) {
                        current = new Vector<int>(values, offset + 1);
                        var next = new Vector<int>(values, offset + chunk);
                        (current - prev).CopyTo(values, offset + 1);
                        prev = next;
                        offset += chunk;
                        count -= chunk;
                    }
                    offset++;
                    count--;
                    m_prev = current[chunk - 1];
                }
            } else {
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
            }
            while(count-- > 0)
                this.InternalGetNext(ref values[offset++]);
        }

        [MethodImpl(AggressiveInlining)]
        private void InternalGetNext(ref int value) {
            var prev = m_prev;
            m_prev = value;
            value -= prev;
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.Delta_DataStreamReader_Int32();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    #region public class Delta_DataStreamWriter_Int16
    public sealed class Delta_DataStreamWriter_Int16 : DataStreamWriterWrapperBase<short>, IResumableDataStreamWriter {
        private short m_prev = 0;

        public Delta_DataStreamWriter_Int16() : base(new DataStreamWriter_Int16()) { }

        protected override void GetNext(ref short value) {
            this.InternalGetNext(ref value);
        }
        protected override void GetNext(short[] values, int offset, int count) {
            if(Vector.IsHardwareAccelerated) {
                var chunk = Vector<short>.Count;
                if(count > chunk) {
                    var prev = new Vector<short>(values, offset);
                    values[offset] = unchecked((short)(values[offset] - m_prev)); // this.InternalGetNext(ref values[offset]);
                    Vector<short> current = default;
                    while(count > chunk) {
                        current = new Vector<short>(values, offset + 1);
                        var next = new Vector<short>(values, offset + chunk);
                        (current - prev).CopyTo(values, offset + 1);
                        prev = next;
                        offset += chunk;
                        count -= chunk;
                    }
                    offset++;
                    count--;
                    m_prev = current[chunk - 1];
                }
            } else {
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
            }
            while(count-- > 0)
                this.InternalGetNext(ref values[offset++]);
        }

        [MethodImpl(AggressiveInlining)]
        private void InternalGetNext(ref short value) {
            var prev = m_prev;
            m_prev = value;
            value = unchecked((short)(value - prev));
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.Delta_DataStreamReader_Int16();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    #region public class Delta_DataStreamWriter_Int8
    public sealed class Delta_DataStreamWriter_Int8 : DataStreamWriterWrapperBase<sbyte>, IResumableDataStreamWriter {
        private sbyte m_prev = 0;

        public Delta_DataStreamWriter_Int8() : base(new DataStreamWriter_Int8()) { }

        protected override void GetNext(ref sbyte value) {
            this.InternalGetNext(ref value);
        }
        protected override void GetNext(sbyte[] values, int offset, int count) {
            if(Vector.IsHardwareAccelerated) {
                var chunk = Vector<sbyte>.Count;
                if(count > chunk) {
                    var prev = new Vector<sbyte>(values, offset);
                    values[offset] = unchecked((sbyte)(values[offset] - m_prev)); // this.InternalGetNext(ref values[offset]);
                    Vector<sbyte> current = default;
                    while(count > chunk) {
                        current = new Vector<sbyte>(values, offset + 1);
                        var next = new Vector<sbyte>(values, offset + chunk);
                        (current - prev).CopyTo(values, offset + 1);
                        prev = next;
                        offset += chunk;
                        count -= chunk;
                    }
                    offset++;
                    count--;
                    m_prev = current[chunk - 1];
                }
            } else {
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
            }
            while(count-- > 0)
                this.InternalGetNext(ref values[offset++]);
        }

        [MethodImpl(AggressiveInlining)]
        private void InternalGetNext(ref sbyte value) {
            var prev = m_prev;
            m_prev = value;
            value = unchecked((sbyte)(value - prev));
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.Delta_DataStreamReader_Int8();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion

    #region public class Delta_DataStreamWriter_DateTime
    public sealed class Delta_DataStreamWriter_DateTime : DataStreamWriterWrapperBase_Complex<DateTime, ulong>, IResumableDataStreamWriter {
        private ulong m_prev = 0;

        // don't use LSB because most dates have the top byte set anyway, but often with the lower precision you can save 1 byte at the bottom
        public Delta_DataStreamWriter_DateTime() : base(new DataStreamWriter_UInt64(), sizeof(ulong)) { }

        protected override ulong Convert(DateTime value) {
            return this.InternalConvert(value);
        }

        protected override void ConvertToBuffer(DateTime[] values, int offset, int count) {
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
        }

        [MethodImpl(AggressiveInlining)]
        private ulong InternalConvert(DateTime value) {
            var v = unchecked((ulong)value.ToBinary());
            var prev = m_prev;
            m_prev = v;
            return Helper.DeltaRemove(v, prev);
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.Delta_DataStreamReader_DateTime();
        }

        public override void Resume(IEnumerable<Stream> channels, long rowCount) {
            var channelsBackup = channels.ToList();
            var reader = this.CreateReader();
            reader.Init(channelsBackup);

            var size = unchecked((int)Math.Min(rowCount, 4096));
            var buffer = new DateTime[size];

            while(rowCount > 0) {
                int request = unchecked((int)Math.Min(rowCount, 4096));
                int read = reader.Read(buffer, 0, request);
                if(read == 0)
                    break;
                rowCount -= read;
                m_prev = unchecked((ulong)buffer[read - 1].ToBinary());
            }
            //this.Init(channelsBackup);
            ((IResumableDataStreamWriter)m_internal).Resume(channelsBackup, rowCount);
        }
        public override void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    #region public class Delta_DataStreamWriter_TimeSpan
    public sealed class Delta_DataStreamWriter_TimeSpan : DataStreamWriterWrapperBase_Complex<TimeSpan, ulong>, IResumableDataStreamWriter {
        private ulong m_prev = 0;

        public Delta_DataStreamWriter_TimeSpan() : base(new DataStreamWriter_UInt64_LSB(), sizeof(ulong)) { }

        protected override ulong Convert(TimeSpan value) {
            return this.InternalConvert(value);
        }

        protected override void ConvertToBuffer(TimeSpan[] values, int offset, int count) {
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
        }

        [MethodImpl(AggressiveInlining)]
        private ulong InternalConvert(TimeSpan value) {
            var v = unchecked((ulong)value.Ticks);
            var prev = m_prev;
            m_prev = v;
            return Helper.DeltaRemove(v, prev);
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.Delta_DataStreamReader_TimeSpan();
        }

        public override void Resume(IEnumerable<Stream> channels, long rowCount) {
            var channelsBackup = channels.ToList();
            var reader = this.CreateReader();
            reader.Init(channelsBackup);

            var size = unchecked((int)Math.Min(rowCount, 4096));
            var buffer = new TimeSpan[size];

            while(rowCount > 0) {
                int request = unchecked((int)Math.Min(rowCount, 4096));
                int read = reader.Read(buffer, 0, request);
                if(read == 0)
                    break;
                rowCount -= read;
                m_prev = unchecked((ulong)buffer[read - 1].Ticks);
            }
            //this.Init(channelsBackup);
            ((IResumableDataStreamWriter)m_internal).Resume(channelsBackup, rowCount);
        }
        public override void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
}
