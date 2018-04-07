//#define USE_TWO_CHANNEL // if disabled, will use one channel

using System.IO;
using System.Collections.Generic;
using System.Linq;


namespace TimeSeriesDB.DataStreams.BaseClasses {
    /// <summary>
    ///     Represents a stream reader whose items inside are variable-sized.
    /// </summary>
    public abstract class VarSizedStreamReaderBase {
#if USE_TWO_CHANNEL
        public virtual int ChannelCount => 2;
        protected const int SIZEBUFFER_SIZE = 4096 / sizeof(ulong);
        protected readonly ulong[] m_sizeBuffer = new ulong[SIZEBUFFER_SIZE];
        protected int m_sizeBufferIndex;
        protected ulong m_remainingItems;
        protected Readers.DataStreamReader_UInt64_LSB m_sizes; // due to encoding compression, no real benefit to use uint32 encoding instead of uint64
#else
        public virtual int ChannelCount => 1;
        protected byte[] m_buffer = new byte[4096];
        protected int m_offset = 0;
        protected int m_read = 0;
#endif

        #region ItemCount
        private ulong m_itemCount = 0;
        public ulong ItemCount {
            get => m_itemCount;
            set {
                m_itemCount = value;
#if USE_TWO_CHANNEL
                m_remainingItems = value;
                m_sizes.ItemCount = value;
#endif
            }
        }
        #endregion
        
        protected Stream m_dataStream;
        
        public virtual void Init(IEnumerable<Stream> channels) {
#if USE_TWO_CHANNEL
            var streams = channels.ToList();
            m_sizes = new Readers.DataStreamReader_UInt64_LSB();
            m_sizes.Init(new[] { streams[0] });
            m_dataStream = streams[1];
#else
            m_dataStream = channels.First();
            m_offset = 0;
            m_read = 0;
#endif
        }

#if USE_TWO_CHANNEL
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected ulong ReadNextLength() {
            if(m_sizeBufferIndex == 0) {
                var request = unchecked((int)Math.Min(SIZEBUFFER_SIZE, m_remainingItems));
                var read = m_sizes.Read(m_sizeBuffer, 0, request);
                if(read == 0)
                    throw new EndOfStreamException();
                m_remainingItems -= unchecked((ulong)read);
            }

            var encodedLength = m_sizeBuffer[m_sizeBufferIndex++];
            if(m_sizeBufferIndex == SIZEBUFFER_SIZE) // == m_sizeReadCount
                m_sizeBufferIndex = 0;

            return encodedLength;
        }
#endif
    }
}