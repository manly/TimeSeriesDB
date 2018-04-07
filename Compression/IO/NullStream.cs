using System;
using System.IO;


namespace TimeSeriesDB.IO
{
    /// <summary>
    ///     Forgets every writes done, always returns zeroes.
    ///     Position/Length behave as normal.
    /// </summary>
    public sealed class NullStream : Stream {
        private long m_position = 0;
        private long m_length = 0;

        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => true;
        
        public override long Position {
            get => m_position;
            set => this.Seek(value, SeekOrigin.Begin);
        }
        public override long Length => m_length;

        public override void Flush() { }
        public override void Close() {
            m_position = 0;
            m_length = 0;
        }

        public override int Read(byte[] buffer, int offset, int count) {
            if(buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if(offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if(count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if(offset + count > buffer.Length)
                throw new ArgumentException();

            count = unchecked((int)Math.Min(count, Math.Max(m_length - m_position, 0)));

            if(count > 0) {
                Array.Clear(buffer, offset, count);
                m_position += count;
                return count;
            }

            return 0;
        }
        public override int ReadByte() {
            if(m_position >= m_length)
                return -1;
            m_position++;
            return 0;
        }

        public override void Write(byte[] buffer, int offset, int count) {
            if(buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if(offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if(count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if(offset + count > buffer.Length)
                throw new ArgumentException();

            m_position += count;
            if(m_length < ++m_position)
                m_length = m_position;
        }
        public override void WriteByte(byte value) {
            if(m_length < ++m_position)
                m_length = m_position;
        }

        public override long Seek(long offset, SeekOrigin origin) {
            // note: As per MemoryStream, setting the position past the Length is OK, 
            // as allocation is only done upon Write()

            switch(origin) {
                case SeekOrigin.Begin:   break;
                case SeekOrigin.Current: offset = m_position + offset; break;
                case SeekOrigin.End:     offset = m_length + offset;   break;
                default: throw new NotImplementedException();
            }

            if(offset != m_position) {
                if(offset < 0)
                    throw new IOException();
                m_position = offset;
            }

            return offset;
        }

        public override void SetLength(long value) {
            if(value < 0)
                throw new IOException();
            m_length = value;
        }
    }
}
