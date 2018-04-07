using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;


namespace TimeSeriesDB.DataStreams.BaseClasses
{
    using Internal;

    public abstract class StreamReaderBase : StreamBase {
        public ulong ItemCount { get; set; }

        protected sbyte m_decompressedCount = 0; // >0 = # of values (can only be 1), <0 = # of zeroes

        public override void Init(IEnumerable<Stream> channels) {
            base.Init(channels);
            this.SetBuffer(new byte[BUFFER_SIZE], 0, 0);
        }

        //#region Reset()
        //public void Reset() {
        //    m_index = 0;
        //    m_decompressedCount = 0;
        //}
        //#endregion

        #region protected ReadByte()
        /// <summary>
        ///     Reads a byte from the buffer, and if reaching end of buffer, will continue reading from the stream.
        ///     If the stream has reached its end, m_bufferSize = 0.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected byte ReadByte() {
            //if(m_index == m_bufferSize) {
            //    m_bufferSize = this.Stream.Read(m_buffer, 0, m_buffer.Length);
            //    m_index = 0;
            //}
            //this.EnsureBufferContains(1);

            return m_buffer[m_index++];
        }
        #endregion
        #region protected ReadUInt32()
        /// <summary>
        ///     Reads a UInt32 from the buffer, and if reaching end of buffer, will continue reading from the stream.
        ///     If the stream has reached its end, m_bufferSize = 0.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected uint ReadUInt32(int byte_count) {
            if(byte_count == 0)
                return 0;
            //this.EnsureBufferContains(byte_count);
            return BitMethods.ReadUInt32(m_buffer, ref m_index, byte_count);


            //// make code fully branchless, on top of this being optimized into "value = (ulong*);"
            //var index = m_index;
            //var res = m_buffer[index + 0] |
            //    ((uint)m_buffer[index + 1] << 8) |
            //    ((uint)m_buffer[index + 2] << 16) |
            //    ((uint)m_buffer[index + 3] << 24);
            //m_index += byte_count;
            //int remove_bytes = (4 - byte_count) << 3;
            //res = (res << remove_bytes) >> remove_bytes;
            //return res;
        }
        #endregion
        #region protected ReadUInt64()
        /// <summary>
        ///     Reads a UInt64 from the buffer, and if reaching end of buffer, will continue reading from the stream.
        ///     If the stream has reached its end, m_bufferSize = 0.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected ulong ReadUInt64(int byte_count) {
            if(byte_count == 0)
                return 0;
            //this.EnsureBufferContains(byte_count);
            return BitMethods.ReadUInt64(m_buffer, ref m_index, byte_count);


            //// make code fully branchless, on top of this being optimized into "value = (ulong*);"
            //var index = m_index;
            //var res = m_buffer[index + 0] |
            //    ((ulong)m_buffer[index + 1] << 8) |
            //    ((ulong)m_buffer[index + 2] << 16) |
            //    ((ulong)m_buffer[index + 3] << 24) |
            //    ((ulong)m_buffer[index + 4] << 32) |
            //    ((ulong)m_buffer[index + 5] << 40) |
            //    ((ulong)m_buffer[index + 6] << 48) |
            //    ((ulong)m_buffer[index + 7] << 56);
            //m_index += byte_count;
            //int remove_bytes = (8 - byte_count) << 3;
            //res = (res << remove_bytes) >> remove_bytes;
            //return res;
        }
        #endregion

        #region protected EnsureBufferContains()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void EnsureBufferContains(int byte_count) {
            if(m_index + byte_count > m_bufferSize) {
                int bytes_to_copy = m_bufferSize - m_index;
                if(bytes_to_copy != 0)
                    Buffer.BlockCopy(m_buffer, m_index, m_buffer, 0, bytes_to_copy);

                m_bufferSize = this.Stream.Read(m_buffer, bytes_to_copy, m_buffer.Length - bytes_to_copy) + bytes_to_copy;
                m_index = 0;
            }
        }
        #endregion
    }
}
