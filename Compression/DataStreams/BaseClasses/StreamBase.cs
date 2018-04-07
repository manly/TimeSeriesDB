using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;


namespace TimeSeriesDB.DataStreams.BaseClasses
{
    public abstract class StreamBase {
        protected const int BUFFER_SIZE = 4096;

        protected int    m_index;
        protected int    m_bufferSize = 0; // encoder = available bytes, decoder = bytes read
        protected byte[] m_buffer = null;

        protected Stream Stream { get; private set; }
        public int ChannelCount => 1;

        public virtual void Init(IEnumerable<Stream> channels) {
            this.Stream = channels.FirstOrDefault() ?? throw new ArgumentNullException(nameof(channels));
            //this.SetBuffer(buffer, offset, bufferSize);
        }


        #region SetBuffer()
        /// <param name="bufferSize">Represents either the available bytes to write to, or the number of read bytes.</param>
        public void SetBuffer(byte[] buffer, int index, int bufferSize) {
            if(buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if(index < 0 || index > bufferSize)
                throw new ArgumentOutOfRangeException(nameof(index));
            if(bufferSize > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(bufferSize));

            m_buffer = buffer;
            this.SetBufferSize(bufferSize);
            m_index = index;
        }
        #endregion
        #region SetBufferSize()
        /// <summary>
        ///     Sets the size of the buffer.
        ///     This indicates the available size to use while writing, or the amount of bytes read while reading.
        /// </summary>
        public void SetBufferSize(int bufferSize) {
            if(bufferSize > m_buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(bufferSize));

            m_bufferSize = bufferSize;
        }
        #endregion

        internal int GetInternalIndex() {
            return m_index;
        }
    }
}
