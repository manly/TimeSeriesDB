using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;


namespace TimeSeriesDB.DataStreams.BaseClasses
{
    using Internal;

    /// <summary>
    ///     Optimized for storage of small static-sized items.
    /// </summary>
    public abstract class DoubleBufferedStreamReaderBase {
        public ulong ItemCount { get; set; }
        public virtual int ChannelCount => 1;

        protected const int PRIMARY_BUFFER_SIZE            = sizeof(uint) * 8;
        protected const int SECONDARY_BUFFER_SIZE          = 4096;

        protected uint            m_primaryBuffer          = 0;
        protected int             m_primaryBufferRemaining = 0; // count

        protected readonly byte[] m_secondaryBuffer        = new byte[SECONDARY_BUFFER_SIZE];
        protected int             m_secondaryBufferIndex   = 0;
        protected int             m_secondaryBufferSize    = 0; // read bytes

        protected Stream m_stream;
        
        public void Init(IEnumerable<Stream> channels) {
            m_stream = channels.FirstOrDefault() ?? throw new ArgumentNullException(nameof(channels));

            m_primaryBuffer = 0;
            m_primaryBufferRemaining = 0;
            m_secondaryBufferIndex = 0;
            m_secondaryBufferSize = 0;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected uint ReadPrimaryBuffer() {
            // fill secondary buffer if needed
            if(m_secondaryBufferIndex >= m_secondaryBufferSize) {
                m_secondaryBufferIndex = 0;
                m_secondaryBufferSize = m_stream.Read(m_secondaryBuffer, 0, SECONDARY_BUFFER_SIZE);
            }
            return 
                ((uint)m_secondaryBuffer[m_secondaryBufferIndex++] << 0) |
                ((uint)m_secondaryBuffer[m_secondaryBufferIndex++] << 8) |
                ((uint)m_secondaryBuffer[m_secondaryBufferIndex++] << 16) |
                ((uint)m_secondaryBuffer[m_secondaryBufferIndex++] << 24);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void ReadAndSkipPrimaryBuffer() {
            // fill secondary buffer if needed
            if(m_secondaryBufferIndex >= m_secondaryBufferSize) {
                m_secondaryBufferIndex = 0;
                m_secondaryBufferSize = m_stream.Read(m_secondaryBuffer, 0, SECONDARY_BUFFER_SIZE);
            }
            m_secondaryBufferIndex += 4;
        }
    }
}