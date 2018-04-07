using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;


namespace TimeSeriesDB.DataStreams.BaseClasses
{
    /// <summary>
    ///     Represents a stream reader whose items inside are fixed-sized and of large size (> 64 bits).
    /// </summary>
    public abstract class LargeFixedSizedStreamReaderBase {
        protected const int BUFFER_SIZE = 4096;

        public ulong ItemCount { get; set; }
        public virtual int ChannelCount => 1;

        protected readonly byte[] m_buffer = new byte[BUFFER_SIZE];
        protected int m_index = 0;
        protected int m_readBytes = 0;

        protected Stream m_dataStream;

        public void Init(IEnumerable<Stream> channels) {
            m_dataStream = channels.First();
        }

        /// <summary>
        ///     Re-aligns cache to make sure item_size consecutive bytes exist starting from m_index.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void EnsureReadable(int item_size) {
            if(m_index + item_size > m_readBytes) {
                int remainder = m_readBytes - m_index;
                
                if(remainder > 0)
                    Buffer.BlockCopy(m_buffer, m_index, m_buffer, 0, remainder);

                m_index = 0;
                m_readBytes = m_dataStream.Read(m_buffer, remainder, BUFFER_SIZE - remainder) + remainder;
            }
        }
    }
}