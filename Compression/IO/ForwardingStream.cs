using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace TimeSeriesDB.IO
{
    /// <summary>
    ///     A stream that alerts/forwards every read/writes.
    /// </summary>
    public sealed class ForwardingStream : Stream {
        private readonly Stream m_stream;

        public override bool CanRead => m_stream.CanRead;
        public override bool CanSeek => m_stream.CanSeek;
        public override bool CanWrite => m_stream.CanWrite;

        public ForwardingStream(Stream stream) {
            m_stream = stream ?? throw new ArgumentNullException(nameof(stream));
        }
        
        public override long Position {
            get => m_stream.Position;
            set => m_stream.Position = value;
        }
        public override long Length => m_stream.Length;

        public override void Flush() => m_stream.Flush();
        public override void Close() => m_stream.Close();

        public object UserValue { get; set; }

        /// <summary>
        ///     Invoked after having forwarded the read.
        /// </summary>
        public event StreamReadHandler Reading;
        /// <summary>
        ///     Invoked after having forwarded the read.
        /// </summary>
        public event StreamReadByteHandler ReadingByte;
        /// <summary>
        ///     Invoked after having forwarded the write.
        /// </summary>
        public event StreamWriteHandler Writing;
        /// <summary>
        ///     Invoked after having forwarded the write.
        /// </summary>
        public event StreamWriteByteHandler WritingByte;

        public override int Read(byte[] buffer, int offset, int count) {
            int read = m_stream.Read(buffer, offset, count);
            this.Reading?.Invoke(this, new StreamReadEventArgs(buffer, offset, count, read));
            return read;
        }
        public override int ReadByte() {
            int value = m_stream.ReadByte();
            this.ReadingByte?.Invoke(this, new StreamReadByteEventArgs(value));
            return value;
        }
        public override void Write(byte[] buffer, int offset, int count) {
            m_stream.Write(buffer, offset, count);
            this.Writing?.Invoke(this, new StreamWriteEventArgs(buffer, offset, count));
        }
        public override void WriteByte(byte value) {
            m_stream.WriteByte(value);
            this.WritingByte?.Invoke(this, new StreamWriteByteEventArgs(value));
        }

        public override long Seek(long offset, SeekOrigin origin) => m_stream.Seek(offset, origin);
        public override void SetLength(long value) => m_stream.SetLength(value);

        public override string ToString() => m_stream.ToString();
        public override int GetHashCode() => m_stream.GetHashCode();
        public override bool Equals(object obj) => m_stream.Equals(obj);
        public override bool CanTimeout => m_stream.CanTimeout;
        public override int ReadTimeout {
            get => m_stream.ReadTimeout;
            set => m_stream.ReadTimeout = value;
        }
        public override int WriteTimeout {
            get => m_stream.WriteTimeout;
            set => m_stream.WriteTimeout = value;
        }


        public delegate void StreamReadHandler(object sender, StreamReadEventArgs e);
        public delegate void StreamReadByteHandler(object sender, StreamReadByteEventArgs e);
        public delegate void StreamWriteHandler(object sender, StreamWriteEventArgs e);
        public delegate void StreamWriteByteHandler(object sender, StreamWriteByteEventArgs e);
        public class StreamReadEventArgs : EventArgs {
            public readonly byte[] Buffer;
            public readonly int Offset;
            public readonly int Count;
            public readonly int Read;
            public StreamReadEventArgs(byte[] buffer, int offset, int count, int read) : base(){
                this.Buffer = buffer;
                this.Offset = offset;
                this.Count = count;
                this.Read = read;
            }
        }
        public class StreamReadByteEventArgs : EventArgs {
            public readonly int Value;
            public StreamReadByteEventArgs(int value) : base(){
                this.Value = value;
            }
        }
        public class StreamWriteEventArgs : EventArgs {
            public readonly byte[] Buffer;
            public readonly int Offset;
            public readonly int Count;
            public StreamWriteEventArgs(byte[] buffer, int offset, int count) : base(){
                this.Buffer = buffer;
                this.Offset = offset;
                this.Count = count;
            }
        }
        public class StreamWriteByteEventArgs : EventArgs {
            public readonly byte Value;
            public StreamWriteByteEventArgs(byte value) : base(){
                this.Value = value;
            }
        }
    }
}
