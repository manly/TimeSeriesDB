#define NON_PORTABLE_CODE

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using static System.Runtime.CompilerServices.MethodImplOptions;
using System.IO;
using System.Numerics;


// note: partial frames (at end of streams) have no delta-delta min value.
// this is intentional, as having any value other than 0 might mess up Resume() as well as complicating the Resume() code to fetch that value.


namespace TimeSeriesDB.DataStreams.Writers.WithEncoders
{
    using Internal;
    using BaseClasses;
    
    #region public class DeltaDelta_DataStreamWriter_UInt64_LSB
    public sealed class DeltaDelta_DataStreamWriter_UInt64_LSB : DataStreamWriterWrapperBase_DeltaDelta<ulong>, IResumableDataStreamWriter {
        private ulong m_prev = 0;

        public DeltaDelta_DataStreamWriter_UInt64_LSB() : base(new DataStreamWriter_UInt64_LSB()) { }

        protected override void FlushFullFrame() {
            var min = ulong.MaxValue;
            this.Delta(ref m_frame[0], ref min);
            this.Delta(ref m_frame[1], ref min);
            this.Delta(ref m_frame[2], ref min);
            this.Delta(ref m_frame[3], ref min);
            this.Delta(ref m_frame[4], ref min);
            this.Delta(ref m_frame[5], ref min);
            this.Delta(ref m_frame[6], ref min);
            this.Delta(ref m_frame[7], ref min);
            this.Delta(ref m_frame[8], ref min);
            this.Delta(ref m_frame[9], ref min);
            this.Delta(ref m_frame[10], ref min);
            this.Delta(ref m_frame[11], ref min);
            this.Delta(ref m_frame[12], ref min);
            this.Delta(ref m_frame[13], ref min);
            this.Delta(ref m_frame[14], ref min);
            this.Delta(ref m_frame[15], ref min);
            
            // remove the min diff from all values
            m_frame[0] -= min;
            m_frame[1] -= min;
            m_frame[2] -= min;
            m_frame[3] -= min;
            m_frame[4] -= min;
            m_frame[5] -= min;
            m_frame[6] -= min;
            m_frame[7] -= min;
            m_frame[8] -= min;
            m_frame[9] -= min;
            m_frame[10] -= min;
            m_frame[11] -= min;
            m_frame[12] -= min;
            m_frame[13] -= min;
            m_frame[14] -= min;
            m_frame[15] -= min;

            m_internal.Write(min);
            m_internal.Write(m_frame, 0, FRAME_SIZE);
        }

        protected override void FlushPartialFrame() {
            var min = ulong.MaxValue;
            if(m_partialResumeIndex == 0) {
                for(int i = 0; i < m_index; i++)
                    this.Delta(ref m_frame[i], ref min);
                //// remove the min diff from all values
                //for(int i = 0; i < m_index; i++)
                //    m_frame[i] -= min;
                //m_internal.Write(min);
                m_internal.Write(0);
                m_internal.Write(m_frame, 0, m_index);
            } else {
                for(int i = m_partialResumeIndex; i < m_index; i++)
                    this.Delta(ref m_frame[i], ref min);
                m_internal.Write(m_frame, m_partialResumeIndex, m_index - m_partialResumeIndex);
            }
            m_partialResumeIndex = m_index;
        }

        protected override void InternalReset() {
            m_prev = 0;
        }

        [MethodImpl(AggressiveInlining)]
        private void Delta(ref ulong frameItem, ref ulong min) {
            var current = frameItem;
            frameItem = Helper.DeltaRemove(current, m_prev);
            m_prev = current;
            min = Math.Min(min, frameItem);
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.DeltaDelta_DataStreamReader_UInt64_LSB();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    #region public class DeltaDelta_DataStreamWriter_UInt64
    public sealed class DeltaDelta_DataStreamWriter_UInt64 : DataStreamWriterWrapperBase_DeltaDelta<ulong>, IResumableDataStreamWriter {
        private ulong m_prev = 0;

        public DeltaDelta_DataStreamWriter_UInt64() : base(new DataStreamWriter_UInt64()) { }

        protected override void FlushFullFrame() {
            var min = ulong.MaxValue;
            this.Delta(ref m_frame[0], ref min);
            this.Delta(ref m_frame[1], ref min);
            this.Delta(ref m_frame[2], ref min);
            this.Delta(ref m_frame[3], ref min);
            this.Delta(ref m_frame[4], ref min);
            this.Delta(ref m_frame[5], ref min);
            this.Delta(ref m_frame[6], ref min);
            this.Delta(ref m_frame[7], ref min);
            this.Delta(ref m_frame[8], ref min);
            this.Delta(ref m_frame[9], ref min);
            this.Delta(ref m_frame[10], ref min);
            this.Delta(ref m_frame[11], ref min);
            this.Delta(ref m_frame[12], ref min);
            this.Delta(ref m_frame[13], ref min);
            this.Delta(ref m_frame[14], ref min);
            this.Delta(ref m_frame[15], ref min);

            // remove the min diff from all values
            m_frame[0] -= min;
            m_frame[1] -= min;
            m_frame[2] -= min;
            m_frame[3] -= min;
            m_frame[4] -= min;
            m_frame[5] -= min;
            m_frame[6] -= min;
            m_frame[7] -= min;
            m_frame[8] -= min;
            m_frame[9] -= min;
            m_frame[10] -= min;
            m_frame[11] -= min;
            m_frame[12] -= min;
            m_frame[13] -= min;
            m_frame[14] -= min;
            m_frame[15] -= min;

            m_internal.Write(min);
            m_internal.Write(m_frame, 0, FRAME_SIZE);
        }

        protected override void FlushPartialFrame() {
            var min = ulong.MaxValue;
            if(m_partialResumeIndex == 0) {
                for(int i = 0; i < m_index; i++)
                    this.Delta(ref m_frame[i], ref min);
                //// remove the min diff from all values
                //for(int i = 0; i < m_index; i++)
                //    m_frame[i] -= min;
                //m_internal.Write(min);
                m_internal.Write(0);
                m_internal.Write(m_frame, 0, m_index);
            } else {
                for(int i = m_partialResumeIndex; i < m_index; i++)
                    this.Delta(ref m_frame[i], ref min);
                m_internal.Write(m_frame, m_partialResumeIndex, m_index - m_partialResumeIndex);
            }
            m_partialResumeIndex = m_index;
        }

        protected override void InternalReset() {
            m_prev = 0;
        }

        [MethodImpl(AggressiveInlining)]
        private void Delta(ref ulong frameItem, ref ulong min) {
            var current = frameItem;
            frameItem = Helper.DeltaRemove(current, m_prev);
            m_prev = current;
            min = Math.Min(min, frameItem);
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.DeltaDelta_DataStreamReader_UInt64();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    #region public class DeltaDelta_DataStreamWriter_UInt32
    public sealed class DeltaDelta_DataStreamWriter_UInt32 : DataStreamWriterWrapperBase_DeltaDelta<uint>, IResumableDataStreamWriter {
        private uint m_prev = 0;

        public DeltaDelta_DataStreamWriter_UInt32() : base(new DataStreamWriter_UInt32()) { }
        
        protected override void FlushFullFrame() {
            var min = uint.MaxValue;
            this.Delta(ref m_frame[0], ref min);
            this.Delta(ref m_frame[1], ref min);
            this.Delta(ref m_frame[2], ref min);
            this.Delta(ref m_frame[3], ref min);
            this.Delta(ref m_frame[4], ref min);
            this.Delta(ref m_frame[5], ref min);
            this.Delta(ref m_frame[6], ref min);
            this.Delta(ref m_frame[7], ref min);
            this.Delta(ref m_frame[8], ref min);
            this.Delta(ref m_frame[9], ref min);
            this.Delta(ref m_frame[10], ref min);
            this.Delta(ref m_frame[11], ref min);
            this.Delta(ref m_frame[12], ref min);
            this.Delta(ref m_frame[13], ref min);
            this.Delta(ref m_frame[14], ref min);
            this.Delta(ref m_frame[15], ref min);

            // remove the min diff from all values
            m_frame[0] -= min;
            m_frame[1] -= min;
            m_frame[2] -= min;
            m_frame[3] -= min;
            m_frame[4] -= min;
            m_frame[5] -= min;
            m_frame[6] -= min;
            m_frame[7] -= min;
            m_frame[8] -= min;
            m_frame[9] -= min;
            m_frame[10] -= min;
            m_frame[11] -= min;
            m_frame[12] -= min;
            m_frame[13] -= min;
            m_frame[14] -= min;
            m_frame[15] -= min;

            m_internal.Write(min);
            m_internal.Write(m_frame, 0, FRAME_SIZE);
        }

        protected override void FlushPartialFrame() {
            var min = uint.MaxValue;
            if(m_partialResumeIndex == 0) {
                for(int i = 0; i < m_index; i++)
                    this.Delta(ref m_frame[i], ref min);
                //// remove the min diff from all values
                //for(int i = 0; i < m_index; i++)
                //    m_frame[i] -= min;
                //m_internal.Write(min);
                m_internal.Write(0);
                m_internal.Write(m_frame, 0, m_index);
            } else {
                for(int i = m_partialResumeIndex; i < m_index; i++)
                    this.Delta(ref m_frame[i], ref min);
                m_internal.Write(m_frame, m_partialResumeIndex, m_index - m_partialResumeIndex);
            }
            m_partialResumeIndex = m_index;
        }

        protected override void InternalReset() {
            m_prev = 0;
        }

        [MethodImpl(AggressiveInlining)]
        private void Delta(ref uint frameItem, ref uint min) {
            var current = frameItem;
            frameItem = Helper.DeltaRemove(current, m_prev);
            m_prev = current;
            min = Math.Min(min, frameItem);
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.DeltaDelta_DataStreamReader_UInt32();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    #region public class DeltaDelta_DataStreamWriter_UInt16
    public sealed class DeltaDelta_DataStreamWriter_UInt16 : DataStreamWriterWrapperBase_DeltaDelta<ushort>, IResumableDataStreamWriter {
        private ushort m_prev = 0;

        public DeltaDelta_DataStreamWriter_UInt16() : base(new DataStreamWriter_UInt16()) { }
        
        protected override void FlushFullFrame() {
            var min = ushort.MaxValue;
            this.Delta(ref m_frame[0], ref min);
            this.Delta(ref m_frame[1], ref min);
            this.Delta(ref m_frame[2], ref min);
            this.Delta(ref m_frame[3], ref min);
            this.Delta(ref m_frame[4], ref min);
            this.Delta(ref m_frame[5], ref min);
            this.Delta(ref m_frame[6], ref min);
            this.Delta(ref m_frame[7], ref min);
            this.Delta(ref m_frame[8], ref min);
            this.Delta(ref m_frame[9], ref min);
            this.Delta(ref m_frame[10], ref min);
            this.Delta(ref m_frame[11], ref min);
            this.Delta(ref m_frame[12], ref min);
            this.Delta(ref m_frame[13], ref min);
            this.Delta(ref m_frame[14], ref min);
            this.Delta(ref m_frame[15], ref min);

            // remove the min diff from all values
            m_frame[0] -= min;
            m_frame[1] -= min;
            m_frame[2] -= min;
            m_frame[3] -= min;
            m_frame[4] -= min;
            m_frame[5] -= min;
            m_frame[6] -= min;
            m_frame[7] -= min;
            m_frame[8] -= min;
            m_frame[9] -= min;
            m_frame[10] -= min;
            m_frame[11] -= min;
            m_frame[12] -= min;
            m_frame[13] -= min;
            m_frame[14] -= min;
            m_frame[15] -= min;

            m_internal.Write(min);
            m_internal.Write(m_frame, 0, FRAME_SIZE);
        }

        protected override void FlushPartialFrame() {
            var min = ushort.MaxValue;
            if(m_partialResumeIndex == 0) {
                for(int i = 0; i < m_index; i++)
                    this.Delta(ref m_frame[i], ref min);
                //// remove the min diff from all values
                //for(int i = 0; i < m_index; i++)
                //    m_frame[i] -= min;
                //m_internal.Write(min);
                m_internal.Write(0);
                m_internal.Write(m_frame, 0, m_index);
            } else {
                for(int i = m_partialResumeIndex; i < m_index; i++)
                    this.Delta(ref m_frame[i], ref min);
                m_internal.Write(m_frame, m_partialResumeIndex, m_index - m_partialResumeIndex);
            }
            m_partialResumeIndex = m_index;
        }

        protected override void InternalReset() {
            m_prev = 0;
        }

        [MethodImpl(AggressiveInlining)]
        private void Delta(ref ushort frameItem, ref ushort min) {
            var current = frameItem;
            frameItem = Helper.DeltaRemove(current, m_prev);
            m_prev = current;
            min = Math.Min(min, frameItem);
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.DeltaDelta_DataStreamReader_UInt16();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    #region public class DeltaDelta_DataStreamWriter_UInt8
    public sealed class DeltaDelta_DataStreamWriter_UInt8 : DataStreamWriterWrapperBase_DeltaDelta<byte>, IResumableDataStreamWriter {
        private byte m_prev = 0;

        public DeltaDelta_DataStreamWriter_UInt8() : base(new DataStreamWriter_UInt8()) { }
        
        protected override void FlushFullFrame() {
            var min = byte.MaxValue;
            this.Delta(ref m_frame[0], ref min);
            this.Delta(ref m_frame[1], ref min);
            this.Delta(ref m_frame[2], ref min);
            this.Delta(ref m_frame[3], ref min);
            this.Delta(ref m_frame[4], ref min);
            this.Delta(ref m_frame[5], ref min);
            this.Delta(ref m_frame[6], ref min);
            this.Delta(ref m_frame[7], ref min);
            this.Delta(ref m_frame[8], ref min);
            this.Delta(ref m_frame[9], ref min);
            this.Delta(ref m_frame[10], ref min);
            this.Delta(ref m_frame[11], ref min);
            this.Delta(ref m_frame[12], ref min);
            this.Delta(ref m_frame[13], ref min);
            this.Delta(ref m_frame[14], ref min);
            this.Delta(ref m_frame[15], ref min);

            // remove the min diff from all values
            m_frame[0] -= min;
            m_frame[1] -= min;
            m_frame[2] -= min;
            m_frame[3] -= min;
            m_frame[4] -= min;
            m_frame[5] -= min;
            m_frame[6] -= min;
            m_frame[7] -= min;
            m_frame[8] -= min;
            m_frame[9] -= min;
            m_frame[10] -= min;
            m_frame[11] -= min;
            m_frame[12] -= min;
            m_frame[13] -= min;
            m_frame[14] -= min;
            m_frame[15] -= min;

            m_internal.Write(min);
            m_internal.Write(m_frame, 0, FRAME_SIZE);
        }

        protected override void FlushPartialFrame() {
            var min = byte.MaxValue;
            if(m_partialResumeIndex == 0) {
                for(int i = 0; i < m_index; i++)
                    this.Delta(ref m_frame[i], ref min);
                //// remove the min diff from all values
                //for(int i = 0; i < m_index; i++)
                //    m_frame[i] -= min;
                //m_internal.Write(min);
                m_internal.Write(0);
                m_internal.Write(m_frame, 0, m_index);
            } else {
                for(int i = m_partialResumeIndex; i < m_index; i++)
                    this.Delta(ref m_frame[i], ref min);
                m_internal.Write(m_frame, m_partialResumeIndex, m_index - m_partialResumeIndex);
            }
            m_partialResumeIndex = m_index;
        }

        protected override void InternalReset() {
            m_prev = 0;
        }

        [MethodImpl(AggressiveInlining)]
        private void Delta(ref byte frameItem, ref byte min) {
            var current = frameItem;
            frameItem = Helper.DeltaRemove(current, m_prev);
            m_prev = current;
            min = Math.Min(min, frameItem);
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.DeltaDelta_DataStreamReader_UInt8();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    
    #region public class DeltaDelta_DataStreamWriter_Int64_LSB
    public sealed class DeltaDelta_DataStreamWriter_Int64_LSB : DataStreamWriterWrapperBase_DeltaDelta_WithSIMD<long>, IResumableDataStreamWriter {
        private long m_prev = 0;

        public DeltaDelta_DataStreamWriter_Int64_LSB() : base(new DataStreamWriter_Int64_LSB()) { }

        protected override void ApplyDeltaToFullFrame(int readOffset, int writeOffset) {
            var min = m_buffer[readOffset + 0];
            min = Math.Min(min, m_buffer[readOffset + 1]);
            min = Math.Min(min, m_buffer[readOffset + 2]);
            min = Math.Min(min, m_buffer[readOffset + 3]);
            min = Math.Min(min, m_buffer[readOffset + 4]);
            min = Math.Min(min, m_buffer[readOffset + 5]);
            min = Math.Min(min, m_buffer[readOffset + 6]);
            min = Math.Min(min, m_buffer[readOffset + 7]);
            min = Math.Min(min, m_buffer[readOffset + 8]);
            min = Math.Min(min, m_buffer[readOffset + 9]);
            min = Math.Min(min, m_buffer[readOffset + 10]);
            min = Math.Min(min, m_buffer[readOffset + 11]);
            min = Math.Min(min, m_buffer[readOffset + 12]);
            min = Math.Min(min, m_buffer[readOffset + 13]);
            min = Math.Min(min, m_buffer[readOffset + 14]);
            min = Math.Min(min, m_buffer[readOffset + 15]);

            m_writeBuffer[writeOffset + 0] = min;
            if(Vector.IsHardwareAccelerated) {
                var chunk = Vector<long>.Count;
                var minVector = new Vector<long>(min);
                int count = FRAME_SIZE;
                writeOffset++;
                while(count >= chunk) {
                    (new Vector<long>(m_buffer, readOffset) - minVector).CopyTo(m_writeBuffer, writeOffset);
                    readOffset += chunk;
                    count -= chunk;
                    writeOffset += chunk;
                }
                while(count-- > 0)
                    m_writeBuffer[writeOffset++] = m_buffer[readOffset++] - min;
            } else {
                m_writeBuffer[writeOffset + 1] = m_buffer[readOffset + 0] - min;
                m_writeBuffer[writeOffset + 2] = m_buffer[readOffset + 1] - min;
                m_writeBuffer[writeOffset + 3] = m_buffer[readOffset + 2] - min;
                m_writeBuffer[writeOffset + 4] = m_buffer[readOffset + 3] - min;
                m_writeBuffer[writeOffset + 5] = m_buffer[readOffset + 4] - min;
                m_writeBuffer[writeOffset + 6] = m_buffer[readOffset + 5] - min;
                m_writeBuffer[writeOffset + 7] = m_buffer[readOffset + 6] - min;
                m_writeBuffer[writeOffset + 8] = m_buffer[readOffset + 7] - min;
                m_writeBuffer[writeOffset + 9] = m_buffer[readOffset + 8] - min;
                m_writeBuffer[writeOffset + 10] = m_buffer[readOffset + 9] - min;
                m_writeBuffer[writeOffset + 11] = m_buffer[readOffset + 10] - min;
                m_writeBuffer[writeOffset + 12] = m_buffer[readOffset + 11] - min;
                m_writeBuffer[writeOffset + 13] = m_buffer[readOffset + 12] - min;
                m_writeBuffer[writeOffset + 14] = m_buffer[readOffset + 13] - min;
                m_writeBuffer[writeOffset + 15] = m_buffer[readOffset + 14] - min;
                m_writeBuffer[writeOffset + 16] = m_buffer[readOffset + 15] - min;
            }
        }

        protected override void ApplyDeltaToBuffer() {
            BitMethods.ApplyDeltaToBuffer(m_buffer, m_partialResumeIndex, m_index - m_partialResumeIndex, ref m_prev);
        }

        protected override void InternalReset() {
            m_prev = 0;
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.DeltaDelta_DataStreamReader_Int64_LSB();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    #region public class DeltaDelta_DataStreamWriter_Int64
    public sealed class DeltaDelta_DataStreamWriter_Int64 : DataStreamWriterWrapperBase_DeltaDelta_WithSIMD<long>, IResumableDataStreamWriter {
        private long m_prev = 0;

        public DeltaDelta_DataStreamWriter_Int64() : base(new DataStreamWriter_Int64()) { }

        protected override void ApplyDeltaToFullFrame(int readOffset, int writeOffset) {
            var min = m_buffer[readOffset + 0];
            min = Math.Min(min, m_buffer[readOffset + 1]);
            min = Math.Min(min, m_buffer[readOffset + 2]);
            min = Math.Min(min, m_buffer[readOffset + 3]);
            min = Math.Min(min, m_buffer[readOffset + 4]);
            min = Math.Min(min, m_buffer[readOffset + 5]);
            min = Math.Min(min, m_buffer[readOffset + 6]);
            min = Math.Min(min, m_buffer[readOffset + 7]);
            min = Math.Min(min, m_buffer[readOffset + 8]);
            min = Math.Min(min, m_buffer[readOffset + 9]);
            min = Math.Min(min, m_buffer[readOffset + 10]);
            min = Math.Min(min, m_buffer[readOffset + 11]);
            min = Math.Min(min, m_buffer[readOffset + 12]);
            min = Math.Min(min, m_buffer[readOffset + 13]);
            min = Math.Min(min, m_buffer[readOffset + 14]);
            min = Math.Min(min, m_buffer[readOffset + 15]);

            m_writeBuffer[writeOffset + 0] = min;
            if(Vector.IsHardwareAccelerated) {
                var chunk = Vector<long>.Count;
                var minVector = new Vector<long>(min);
                int count = FRAME_SIZE;
                writeOffset++;
                while(count >= chunk) {
                    (new Vector<long>(m_buffer, readOffset) - minVector).CopyTo(m_writeBuffer, writeOffset);
                    readOffset += chunk;
                    count -= chunk;
                    writeOffset += chunk;
                }
                while(count-- > 0)
                    m_writeBuffer[writeOffset++] = m_buffer[readOffset++] - min;
            } else {
                m_writeBuffer[writeOffset + 1] = m_buffer[readOffset + 0] - min;
                m_writeBuffer[writeOffset + 2] = m_buffer[readOffset + 1] - min;
                m_writeBuffer[writeOffset + 3] = m_buffer[readOffset + 2] - min;
                m_writeBuffer[writeOffset + 4] = m_buffer[readOffset + 3] - min;
                m_writeBuffer[writeOffset + 5] = m_buffer[readOffset + 4] - min;
                m_writeBuffer[writeOffset + 6] = m_buffer[readOffset + 5] - min;
                m_writeBuffer[writeOffset + 7] = m_buffer[readOffset + 6] - min;
                m_writeBuffer[writeOffset + 8] = m_buffer[readOffset + 7] - min;
                m_writeBuffer[writeOffset + 9] = m_buffer[readOffset + 8] - min;
                m_writeBuffer[writeOffset + 10] = m_buffer[readOffset + 9] - min;
                m_writeBuffer[writeOffset + 11] = m_buffer[readOffset + 10] - min;
                m_writeBuffer[writeOffset + 12] = m_buffer[readOffset + 11] - min;
                m_writeBuffer[writeOffset + 13] = m_buffer[readOffset + 12] - min;
                m_writeBuffer[writeOffset + 14] = m_buffer[readOffset + 13] - min;
                m_writeBuffer[writeOffset + 15] = m_buffer[readOffset + 14] - min;
                m_writeBuffer[writeOffset + 16] = m_buffer[readOffset + 15] - min;
            }
        }

        protected override void ApplyDeltaToBuffer() {
            BitMethods.ApplyDeltaToBuffer(m_buffer, m_partialResumeIndex, m_index - m_partialResumeIndex, ref m_prev);
        }

        protected override void InternalReset() {
            m_prev = 0;
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.DeltaDelta_DataStreamReader_Int64();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    #region public class DeltaDelta_DataStreamWriter_Int32
    public sealed class DeltaDelta_DataStreamWriter_Int32 : DataStreamWriterWrapperBase_DeltaDelta_WithSIMD<int>, IResumableDataStreamWriter {
        private int m_prev = 0;

        public DeltaDelta_DataStreamWriter_Int32() : base(new DataStreamWriter_Int32()) { }

        protected override void ApplyDeltaToFullFrame(int readOffset, int writeOffset) {
            var min = m_buffer[readOffset + 0];
            min = Math.Min(min, m_buffer[readOffset + 1]);
            min = Math.Min(min, m_buffer[readOffset + 2]);
            min = Math.Min(min, m_buffer[readOffset + 3]);
            min = Math.Min(min, m_buffer[readOffset + 4]);
            min = Math.Min(min, m_buffer[readOffset + 5]);
            min = Math.Min(min, m_buffer[readOffset + 6]);
            min = Math.Min(min, m_buffer[readOffset + 7]);
            min = Math.Min(min, m_buffer[readOffset + 8]);
            min = Math.Min(min, m_buffer[readOffset + 9]);
            min = Math.Min(min, m_buffer[readOffset + 10]);
            min = Math.Min(min, m_buffer[readOffset + 11]);
            min = Math.Min(min, m_buffer[readOffset + 12]);
            min = Math.Min(min, m_buffer[readOffset + 13]);
            min = Math.Min(min, m_buffer[readOffset + 14]);
            min = Math.Min(min, m_buffer[readOffset + 15]);

            m_writeBuffer[writeOffset + 0] = min;
            if(Vector.IsHardwareAccelerated) {
                var chunk = Vector<int>.Count;
                var minVector = new Vector<int>(min);
                int count = FRAME_SIZE;
                writeOffset++;
                while(count >= chunk) {
                    (new Vector<int>(m_buffer, readOffset) - minVector).CopyTo(m_writeBuffer, writeOffset);
                    readOffset += chunk;
                    count -= chunk;
                    writeOffset += chunk;
                }
                while(count-- > 0)
                    m_writeBuffer[writeOffset++] = m_buffer[readOffset++] - min;
            } else {
                m_writeBuffer[writeOffset + 1] = m_buffer[readOffset + 0] - min;
                m_writeBuffer[writeOffset + 2] = m_buffer[readOffset + 1] - min;
                m_writeBuffer[writeOffset + 3] = m_buffer[readOffset + 2] - min;
                m_writeBuffer[writeOffset + 4] = m_buffer[readOffset + 3] - min;
                m_writeBuffer[writeOffset + 5] = m_buffer[readOffset + 4] - min;
                m_writeBuffer[writeOffset + 6] = m_buffer[readOffset + 5] - min;
                m_writeBuffer[writeOffset + 7] = m_buffer[readOffset + 6] - min;
                m_writeBuffer[writeOffset + 8] = m_buffer[readOffset + 7] - min;
                m_writeBuffer[writeOffset + 9] = m_buffer[readOffset + 8] - min;
                m_writeBuffer[writeOffset + 10] = m_buffer[readOffset + 9] - min;
                m_writeBuffer[writeOffset + 11] = m_buffer[readOffset + 10] - min;
                m_writeBuffer[writeOffset + 12] = m_buffer[readOffset + 11] - min;
                m_writeBuffer[writeOffset + 13] = m_buffer[readOffset + 12] - min;
                m_writeBuffer[writeOffset + 14] = m_buffer[readOffset + 13] - min;
                m_writeBuffer[writeOffset + 15] = m_buffer[readOffset + 14] - min;
                m_writeBuffer[writeOffset + 16] = m_buffer[readOffset + 15] - min;
            }
        }

        protected override void ApplyDeltaToBuffer() {
            BitMethods.ApplyDeltaToBuffer(m_buffer, m_partialResumeIndex, m_index - m_partialResumeIndex, ref m_prev);
        }

        protected override void InternalReset() {
            m_prev = 0;
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.DeltaDelta_DataStreamReader_Int32();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    #region public class DeltaDelta_DataStreamWriter_Int16
    public sealed class DeltaDelta_DataStreamWriter_Int16 : DataStreamWriterWrapperBase_DeltaDelta_WithSIMD<short>, IResumableDataStreamWriter {
        private short m_prev = 0;

        public DeltaDelta_DataStreamWriter_Int16() : base(new DataStreamWriter_Int16()) { }

        protected override void ApplyDeltaToFullFrame(int readOffset, int writeOffset) {
            var min = m_buffer[readOffset + 0];
            min = Math.Min(min, m_buffer[readOffset + 1]);
            min = Math.Min(min, m_buffer[readOffset + 2]);
            min = Math.Min(min, m_buffer[readOffset + 3]);
            min = Math.Min(min, m_buffer[readOffset + 4]);
            min = Math.Min(min, m_buffer[readOffset + 5]);
            min = Math.Min(min, m_buffer[readOffset + 6]);
            min = Math.Min(min, m_buffer[readOffset + 7]);
            min = Math.Min(min, m_buffer[readOffset + 8]);
            min = Math.Min(min, m_buffer[readOffset + 9]);
            min = Math.Min(min, m_buffer[readOffset + 10]);
            min = Math.Min(min, m_buffer[readOffset + 11]);
            min = Math.Min(min, m_buffer[readOffset + 12]);
            min = Math.Min(min, m_buffer[readOffset + 13]);
            min = Math.Min(min, m_buffer[readOffset + 14]);
            min = Math.Min(min, m_buffer[readOffset + 15]);

            m_writeBuffer[writeOffset + 0] = min;
            if(Vector.IsHardwareAccelerated) {
                var chunk = Vector<short>.Count;
                var minVector = new Vector<short>(min);
                int count = FRAME_SIZE;
                writeOffset++;
                while(count >= chunk) {
                    (new Vector<short>(m_buffer, readOffset) - minVector).CopyTo(m_writeBuffer, writeOffset);
                    readOffset += chunk;
                    count -= chunk;
                    writeOffset += chunk;
                }
                while(count-- > 0)
                    m_writeBuffer[writeOffset++] = unchecked((short)(m_buffer[readOffset++] - min));
            } else {
                m_writeBuffer[writeOffset + 1] = unchecked((short)(m_buffer[readOffset + 0] - min));
                m_writeBuffer[writeOffset + 2] = unchecked((short)(m_buffer[readOffset + 1] - min));
                m_writeBuffer[writeOffset + 3] = unchecked((short)(m_buffer[readOffset + 2] - min));
                m_writeBuffer[writeOffset + 4] = unchecked((short)(m_buffer[readOffset + 3] - min));
                m_writeBuffer[writeOffset + 5] = unchecked((short)(m_buffer[readOffset + 4] - min));
                m_writeBuffer[writeOffset + 6] = unchecked((short)(m_buffer[readOffset + 5] - min));
                m_writeBuffer[writeOffset + 7] = unchecked((short)(m_buffer[readOffset + 6] - min));
                m_writeBuffer[writeOffset + 8] = unchecked((short)(m_buffer[readOffset + 7] - min));
                m_writeBuffer[writeOffset + 9] = unchecked((short)(m_buffer[readOffset + 8] - min));
                m_writeBuffer[writeOffset + 10] = unchecked((short)(m_buffer[readOffset + 9] - min));
                m_writeBuffer[writeOffset + 11] = unchecked((short)(m_buffer[readOffset + 10] - min));
                m_writeBuffer[writeOffset + 12] = unchecked((short)(m_buffer[readOffset + 11] - min));
                m_writeBuffer[writeOffset + 13] = unchecked((short)(m_buffer[readOffset + 12] - min));
                m_writeBuffer[writeOffset + 14] = unchecked((short)(m_buffer[readOffset + 13] - min));
                m_writeBuffer[writeOffset + 15] = unchecked((short)(m_buffer[readOffset + 14] - min));
                m_writeBuffer[writeOffset + 16] = unchecked((short)(m_buffer[readOffset + 15] - min));
            }
        }
        
        protected override void ApplyDeltaToBuffer() {
            BitMethods.ApplyDeltaToBuffer(m_buffer, m_partialResumeIndex, m_index - m_partialResumeIndex, ref m_prev);
        }

        protected override void InternalReset() {
            m_prev = 0;
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.DeltaDelta_DataStreamReader_Int16();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion
    #region public class DeltaDelta_DataStreamWriter_Int8
    public sealed class DeltaDelta_DataStreamWriter_Int8 : DataStreamWriterWrapperBase_DeltaDelta_WithSIMD<sbyte>, IResumableDataStreamWriter {
        private sbyte m_prev = 0;

        public DeltaDelta_DataStreamWriter_Int8() : base(new DataStreamWriter_Int8()) { }

        protected override void ApplyDeltaToFullFrame(int readOffset, int writeOffset) {
            var min = m_buffer[readOffset + 0];
            min = Math.Min(min, m_buffer[readOffset + 1]);
            min = Math.Min(min, m_buffer[readOffset + 2]);
            min = Math.Min(min, m_buffer[readOffset + 3]);
            min = Math.Min(min, m_buffer[readOffset + 4]);
            min = Math.Min(min, m_buffer[readOffset + 5]);
            min = Math.Min(min, m_buffer[readOffset + 6]);
            min = Math.Min(min, m_buffer[readOffset + 7]);
            min = Math.Min(min, m_buffer[readOffset + 8]);
            min = Math.Min(min, m_buffer[readOffset + 9]);
            min = Math.Min(min, m_buffer[readOffset + 10]);
            min = Math.Min(min, m_buffer[readOffset + 11]);
            min = Math.Min(min, m_buffer[readOffset + 12]);
            min = Math.Min(min, m_buffer[readOffset + 13]);
            min = Math.Min(min, m_buffer[readOffset + 14]);
            min = Math.Min(min, m_buffer[readOffset + 15]);

            m_writeBuffer[writeOffset + 0] = min;
            if(Vector.IsHardwareAccelerated) {
                var chunk = Vector<sbyte>.Count;
                var minVector = new Vector<sbyte>(min);
                int count = FRAME_SIZE;
                writeOffset++;
                while(count >= chunk) {
                    (new Vector<sbyte>(m_buffer, readOffset) - minVector).CopyTo(m_writeBuffer, writeOffset);
                    readOffset += chunk;
                    count -= chunk;
                    writeOffset += chunk;
                }
                while(count-- > 0)
                    m_writeBuffer[writeOffset++] = unchecked((sbyte)(m_buffer[readOffset++] - min));
            } else {
                m_writeBuffer[writeOffset + 1] = unchecked((sbyte)(m_buffer[readOffset + 0] - min));
                m_writeBuffer[writeOffset + 2] = unchecked((sbyte)(m_buffer[readOffset + 1] - min));
                m_writeBuffer[writeOffset + 3] = unchecked((sbyte)(m_buffer[readOffset + 2] - min));
                m_writeBuffer[writeOffset + 4] = unchecked((sbyte)(m_buffer[readOffset + 3] - min));
                m_writeBuffer[writeOffset + 5] = unchecked((sbyte)(m_buffer[readOffset + 4] - min));
                m_writeBuffer[writeOffset + 6] = unchecked((sbyte)(m_buffer[readOffset + 5] - min));
                m_writeBuffer[writeOffset + 7] = unchecked((sbyte)(m_buffer[readOffset + 6] - min));
                m_writeBuffer[writeOffset + 8] = unchecked((sbyte)(m_buffer[readOffset + 7] - min));
                m_writeBuffer[writeOffset + 9] = unchecked((sbyte)(m_buffer[readOffset + 8] - min));
                m_writeBuffer[writeOffset + 10] = unchecked((sbyte)(m_buffer[readOffset + 9] - min));
                m_writeBuffer[writeOffset + 11] = unchecked((sbyte)(m_buffer[readOffset + 10] - min));
                m_writeBuffer[writeOffset + 12] = unchecked((sbyte)(m_buffer[readOffset + 11] - min));
                m_writeBuffer[writeOffset + 13] = unchecked((sbyte)(m_buffer[readOffset + 12] - min));
                m_writeBuffer[writeOffset + 14] = unchecked((sbyte)(m_buffer[readOffset + 13] - min));
                m_writeBuffer[writeOffset + 15] = unchecked((sbyte)(m_buffer[readOffset + 14] - min));
                m_writeBuffer[writeOffset + 16] = unchecked((sbyte)(m_buffer[readOffset + 15] - min));
            }
        }

        protected override void ApplyDeltaToBuffer() {
            BitMethods.ApplyDeltaToBuffer(m_buffer, m_partialResumeIndex, m_index - m_partialResumeIndex, ref m_prev);
        }

        protected override void InternalReset() {
            m_prev = 0;
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.DeltaDelta_DataStreamReader_Int8();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.InternalResume(channels, rowCount, ref m_prev);
        }
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);
        }
    }
    #endregion

    #region public class DeltaDelta_DataStreamWriter_DateTime
    public sealed class DeltaDelta_DataStreamWriter_DateTime : DataStreamWriterWrapperBase_Complex<DateTime, ulong>, IResumableDataStreamWriter {
        public DeltaDelta_DataStreamWriter_DateTime() : base(new DeltaDelta_DataStreamWriter_UInt64_LSB(), sizeof(ulong)) { }

        protected override ulong Convert(DateTime value) {
            return this.InternalConvert(value);
        }

        protected override void ConvertToBuffer(DateTime[] values, int offset, int count) {
#if NON_PORTABLE_CODE
            var converted = new ReadOnlySpan<DateTime>(values, offset, count).NonPortableCast<DateTime, ulong>();
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

        [MethodImpl(AggressiveInlining)]
        private ulong InternalConvert(DateTime value) {
            return unchecked((ulong)value.ToBinary());
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.DeltaDelta_DataStreamReader_DateTime();
        }
    }
    #endregion
    #region public class DeltaDelta_DataStreamWriter_TimeSpan
    public sealed class DeltaDelta_DataStreamWriter_TimeSpan : DataStreamWriterWrapperBase_Complex<TimeSpan, ulong>, IResumableDataStreamWriter {
        public DeltaDelta_DataStreamWriter_TimeSpan() : base(new DeltaDelta_DataStreamWriter_UInt64_LSB(), sizeof(ulong)) { }

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

        [MethodImpl(AggressiveInlining)]
        private ulong InternalConvert(TimeSpan value) {
            return unchecked((ulong)value.Ticks);
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.DeltaDelta_DataStreamReader_TimeSpan();
        }
    }
    #endregion
}
