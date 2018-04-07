using System;
using System.IO;


namespace TimeSeriesDB.IO
{
    /// <summary>
    ///     Handles a pair of read-only/write-only streams.
    ///     The goal is to allow in-place stream replacement, where you write unto the stream you read.
    ///     This is meant to allow in-place compressed stream replacement, so that the compressed stream can be appended in-place.
    /// </summary>
    public sealed class ResumableStreamManager {
        private readonly Stream m_stream;

        #region ReadStream
        private readonly ResumableReadStream m_readStream;
        /// <summary>
        /// The read-only version that wraps the original stream.
        /// This is assumed to be read ahead of the write-only stream in order to allow in-place replacement.
        /// </summary>
        public Stream ReadStream => m_readStream;
        #endregion
        #region WriteStream
        private readonly ResumableWriteStream m_writeStream;
        /// <summary>
        /// The write-only version that wraps the original stream.
        /// This is assumed to be written after the read-only stream in order to allow in-place replacement.
        /// </summary>
        public Stream WriteStream => m_writeStream;
        #endregion

        #region constructors
        public ResumableStreamManager(Stream stream) {
            if(stream == null)
                throw new ArgumentNullException(nameof(stream));
            if(!stream.CanSeek)
                throw new ArgumentException("The stream must support seeking.", nameof(stream));

            m_stream = stream;

            m_readStream = new ResumableReadStream(this);
            m_writeStream = new ResumableWriteStream(this);
        }
        #endregion

        #region private class ResumableReadStream
        private sealed class ResumableReadStream : Stream {
            private readonly ResumableStreamManager m_owner;
            private readonly Stream m_stream;
            private readonly ResumableWriteStream m_pair;

            private long m_position = 0;
            private readonly long m_length; // cant use m_stream.Length because we don't want to allow reading what the paired write stream is writing

            public ResumableReadStream(ResumableStreamManager owner) {
                m_owner = owner;
                m_stream = owner.m_stream;
                m_length = owner.m_stream.Length;
                m_pair = owner.m_writeStream;
            }

            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => false;

            public override long Position {
                get => m_position;
                set => this.Seek(value, SeekOrigin.Begin);
            }
            public override long Length => m_length;

            public override void Flush() {
                // silently ignore since writes aren't supported
                //throw new InvalidOperationException();
            }
            public override void Close() {
                m_position = 0;
                //m_length = 0;
            }

            public override int Read(byte[] buffer, int offset, int count) {
                // don't allow reading past the original length of the stream when the constructor was invoked
                // this is important because if we don't do that, it would allow the read stream to read what the write stream is writing, 
                // which would not make any sense since the previously read data before m_length would differ from what the write stream is writing
                var request = unchecked((int)Math.Min(m_length - m_position, count));
                if(request <= 0)
                    return 0;

                if(m_stream.Position != m_position)
                    m_stream.Position = m_position;

                int read = m_stream.Read(buffer, offset, request);
                m_position += read;
                return read;
            }
            public override int ReadByte() {
                // don't allow reading past the original length of the stream when the constructor was invoked
                // this is important because if we don't do that, it would allow the read stream to read what the write stream is writing, 
                // which would not make any sense since the previously read data before m_length would differ from what the write stream is writing
                if(m_position >= m_length)
                    return 0;

                if(m_stream.Position != m_position)
                    m_stream.Position = m_position;

                int read = m_stream.ReadByte();
                if(read >= 0)
                    m_position++;
                return read;
            }

            public override void Write(byte[] buffer, int offset, int count) {
                throw new NotSupportedException("The stream does not support writing.");
            }

            public override long Seek(long offset, SeekOrigin origin) {
                throw new NotSupportedException("The stream does not support seeking.");
            }

            public override void SetLength(long value) {
                throw new NotSupportedException("The stream does not support writing.");
            }
        }
        #endregion
        #region private class ResumableWriteStream
        private sealed class ResumableWriteStream : Stream {
            private readonly ResumableStreamManager m_owner;
            private readonly Stream m_stream;
            private readonly ResumableReadStream m_pair;

            private long m_position = 0; // m_position should always = m_length due to CanSeek()=false and AppendOnly
            private long m_length = 0;

            // cannot allow writing past the ReadStream position; but if we do, then we store the data in this temporary cache 
            // this way, the ReadStream can read data that makes sense and not partial chunks left and right
            private DynamicMemoryStream m_writeCache = null;
            private long m_writeCachePosition = -1;

            public ResumableWriteStream(ResumableStreamManager owner) {
                m_owner = owner;
                m_stream = owner.m_stream;
                m_pair = owner.m_readStream;
            }

            public override bool CanRead => false;
            public override bool CanSeek => false;
            public override bool CanWrite => true;

            public override long Position {
                get => m_position;
                set => this.Seek(value, SeekOrigin.Begin);
            }
            public override long Length => m_length;

            public override void Flush() {
                // exceptionally, you do not want to flush the writecache into m_stream
                // if you did, you would enter the same problem you were trying to avoid
                // in short, the restriction of write() that you can't write past the ReadStream.Position applies here too
                // to make this code simple, we simply do not write the cache at all, and reserve this step for the close()
            }
            public override void Close() {
                this.FlushWriteCache();

                this.Flush();

                m_position = 0;
                m_length = 0;

                if(m_writeCache != null) {
                    m_writeCache.Dispose();
                    m_writeCache.Close();
                    m_writeCache = null;
                }
                m_writeCachePosition = -1;
            }

            public override int Read(byte[] buffer, int offset, int count) {
                throw new NotSupportedException("The stream does not support reading.");
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

                // any write that are further than the ReadStream.Position must be cached

                if(m_writeCachePosition < 0) {
                    // write whatever comes before the reading position into the stream directly
                    int write = unchecked((int)Math.Min(Math.Max(m_pair.Position - m_position, 0), count));
                    if(write > 0) {
                        if(m_stream.Position != m_position)
                            m_stream.Position = m_position;

                        m_stream.Write(buffer, offset, write);

                        count      -= write;
                        offset     += write;
                        m_position += write;
                        m_length   = m_position;
                    }

                    // whatever remains must be written to the write cache
                    if(count <= 0)
                        return;
                    
                    if(m_writeCache == null)
                        m_writeCache = new DynamicMemoryStream();
                    m_writeCachePosition = m_position;
                }

                // if we already have a write cache, then we must write to it
                m_writeCache.Write(buffer, offset, count);

                m_position += count;
                m_length = m_position; // since were append-only and cant seek, we know the length=position
            }
            //public override void WriteByte(byte value) {
            //    base.WriteByte(value);
            //}

            public override long Seek(long offset, SeekOrigin origin) {
                throw new NotSupportedException("The stream does not support seeking.");
            }

            public override void SetLength(long value) {
                throw new NotSupportedException("Only Write()/WriteByte() is supported because this Stream does not support seeking (ie: cant setlength() without moving .position).");
            }

            /// <summary>
            ///     Flush the write cache.
            ///     Be aware that this will intentionally not look if it writes past the ReadStream.Position.
            ///     Do not call this if you think the ReadStream might read that data.
            /// </summary>
            private void FlushWriteCache() {
                if(m_writeCachePosition < 0)
                    return;

                m_writeCache.Flush();
                m_writeCache.Position = 0;

                var remaining = m_writeCache.Length;
                if(m_stream.Position != m_writeCachePosition)
                    m_stream.Position = m_writeCachePosition;

                foreach(var section in m_writeCache.GetInternalSections()) {
                    int write = unchecked((int)Math.Min(section.Length, remaining));
                    if(write == 0)
                        break;

                    m_stream.Write(section.Buffer, section.Index, write);
                    remaining -= write;

                    // since this is a write cache, the pos/len of this stream is already including it, so we dont touch them
                    //m_position += write;
                    //if(m_length < m_position)
                    //    m_length = m_position;
                }

                m_writeCache.Position = 0;
                m_writeCache.SetLength(0);
                m_writeCachePosition = -1;
            }
        }
        #endregion
    }
}
