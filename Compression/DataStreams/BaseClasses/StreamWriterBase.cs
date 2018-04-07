using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Runtime.CompilerServices;


namespace TimeSeriesDB.DataStreams.BaseClasses
{
    using Internal;

    public abstract class StreamWriterBase : StreamBase {
        protected int  m_prevFlag;
        protected int  m_consecutiveZeroes = 0;
        protected bool m_hasPrev = false;

        public override void Init(IEnumerable<Stream> channels) {
            base.Init(channels);
            this.SetBuffer(new byte[BUFFER_SIZE], 0, BUFFER_SIZE);
        }

        public abstract void Commit();
        #region Flush()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Flush() {
            if(m_index == 0)
                return;
            
            this.InternalFlush();

            this.Stream.Flush();
        }
        #endregion
        #region protected InternalFlush()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void InternalFlush() {
            this.Stream.Write(m_buffer, 0, m_index);
            //this.Stream.Flush(); // DO NOT DO THIS HERE! causes segment fragmentation
            m_index = 0;
            //m_bufferSize = m_buffer.Length;
        }
        #endregion

        #region Reset()
        public void Reset() {
            m_index = 0;
            m_hasPrev = false;
            //m_consecutiveZeroes = 0;

            Helper.StreamReset(this.Stream);
        }
        #endregion
        #region Resume()
        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.Init(channels);
            this.Stream.Seek(0, SeekOrigin.End);
        }
        #endregion

        #region protected WriteByte()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void WriteByte(byte value) {
            m_buffer[m_index++] = value;
        }
        #endregion
        #region protected WriteUInt32()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void WriteUInt32(uint value, int byte_count) {
            //if(byte_count == 0)
            //    return;
            //BitMethods.WriteUInt32(m_buffer, ref m_index, value, byte_count);

            // make code fully branchless, on top of this being optimized into "(uint*) = value;"
            var index = m_index;
            m_buffer[index + 0] = unchecked((byte)((value >> 0) & 0xFF));
            m_buffer[index + 1] = unchecked((byte)((value >> 8) & 0xFF));
            m_buffer[index + 2] = unchecked((byte)((value >> 16) & 0xFF));
            m_buffer[index + 3] = unchecked((byte)((value >> 24) & 0xFF));
            m_index += byte_count;
        }
        #endregion
        #region protected WriteUInt64()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void WriteUInt64(ulong value, int byte_count) {
            //if(byte_count == 0)
            //    return;
            //BitMethods.WriteUInt64(m_buffer, ref m_index, value, byte_count);

            // make code fully branchless, on top of this being optimized into "(ulong*) = value;"
            var index = m_index;
            m_buffer[index + 0] = unchecked((byte)((value >> 0) & 0xFF));
            m_buffer[index + 1] = unchecked((byte)((value >> 8) & 0xFF));
            m_buffer[index + 2] = unchecked((byte)((value >> 16) & 0xFF));
            m_buffer[index + 3] = unchecked((byte)((value >> 24) & 0xFF));
            m_buffer[index + 4] = unchecked((byte)((value >> 32) & 0xFF));
            m_buffer[index + 5] = unchecked((byte)((value >> 40) & 0xFF));
            m_buffer[index + 6] = unchecked((byte)((value >> 48) & 0xFF));
            m_buffer[index + 7] = unchecked((byte)((value >> 56) & 0xFF));
            m_index += byte_count;
        }
        #endregion
    }
}
