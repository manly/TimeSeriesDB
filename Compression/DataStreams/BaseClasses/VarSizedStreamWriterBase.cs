//#define USE_TWO_CHANNEL // if disabled, will use one channel

using System.IO;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Linq;


namespace TimeSeriesDB.DataStreams.BaseClasses {
    /// <summary>
    ///     Represents a stream writer whose items inside are variable-sized.
    /// </summary>
    public abstract class VarSizedStreamWriterBase : Writers.IResumableDataStreamWriter {
#if USE_TWO_CHANNEL
        protected const int SIZEBUFFER_SIZE = 4096 / sizeof(ulong);
        public virtual int ChannelCount => 2;
        protected readonly ulong[] m_sizeBuffer = new ulong[SIZEBUFFER_SIZE];
        protected int m_sizeBufferIndex;
        protected Writers.DataStreamWriter_UInt64_LSB m_sizes; // could use uint32 instead since stream.read() is limited to 32 bits
#else
        public virtual int ChannelCount => 1;

        protected byte[] m_buffer = new byte[4096];
        protected int m_offset = 0;
#endif

        protected Stream m_dataStream;

        public virtual void Init(IEnumerable<Stream> channels) {
#if USE_TWO_CHANNEL
            var streams = channels.ToList();
            m_sizes = new Writers.DataStreamWriter_UInt64_LSB();
            m_sizes.Init(new[] { streams[0] });
            m_dataStream = streams[1];
#else
            m_dataStream = channels.First();
            m_offset = 0;
#endif
        }


        public virtual void Reset() {
#if USE_TWO_CHANNEL
            m_sizeBufferIndex = 0;
            m_sizes.Reset();
#else
            m_offset = 0;
#endif
            m_dataStream.Position = 0;
            //BitMethods.StreamReset(m_dataStream);
        }
        
        public virtual void Commit() {
#if USE_TWO_CHANNEL
            if(m_sizeBufferIndex > 0) {
                m_sizes.Put(m_sizeBuffer, 0, m_sizeBufferIndex);
                m_sizes.Commit();
                m_sizeBufferIndex = 0;
            }
#else
            if(m_offset > 0) {
                m_dataStream.Write(m_buffer, 0, m_offset);
                m_offset = 0;
            }
#endif
        }

        public void Flush() {
#if USE_TWO_CHANNEL
            m_sizes.Flush();
#endif
            m_dataStream.Flush();
        }

#if USE_TWO_CHANNEL
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void WriteNextLength(ulong value) {
            m_sizeBuffer[m_sizeBufferIndex++] = value;

            if(m_sizeBufferIndex == SIZEBUFFER_SIZE) {
                m_sizes.Put(m_sizeBuffer, 0, SIZEBUFFER_SIZE);
                m_sizeBufferIndex = 0;
            }
        }
#endif

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.Init(channels);
            m_dataStream.Seek(0, SeekOrigin.End);

#if USE_TWO_CHANNEL
            ((IResumableDataStreamWriter)m_sizes).Resume(channels, rowCount);
#endif
        }
        public abstract void Resume(IEnumerable<Writers.ResumableChannel> resumableChannels, long rowCount);
    }
}