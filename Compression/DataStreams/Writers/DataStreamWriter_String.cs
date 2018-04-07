//#define USE_TWO_CHANNEL // if disabled, will use one channel

using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Text;
using System.Runtime.CompilerServices;


namespace TimeSeriesDB.DataStreams.Writers
{
    using Internal;
    using BaseClasses;

    public sealed class DataStreamWriter_String : VarSizedStreamWriterBase, IDataStreamWriter<string>, IResumableDataStreamWriter {
        private static readonly Encoding ENCODER = Encoding.UTF8;

        private const int BUFFER_SIZE            = BitMethods.ENCODESTRING_BUFFER_SIZE;
        private readonly byte[] m_dataBuffer     = new byte[BUFFER_SIZE];
#if USE_TWO_CHANNEL
        private int m_offset = 0;
#endif

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(string[] values, int offset, int count) {
            while(count >= 16) {
                this.Write(values[offset + 0]);
                this.Write(values[offset + 1]);
                this.Write(values[offset + 2]);
                this.Write(values[offset + 3]);
                this.Write(values[offset + 4]);
                this.Write(values[offset + 5]);
                this.Write(values[offset + 6]);
                this.Write(values[offset + 7]);
                this.Write(values[offset + 8]);
                this.Write(values[offset + 9]);
                this.Write(values[offset + 10]);
                this.Write(values[offset + 11]);
                this.Write(values[offset + 12]);
                this.Write(values[offset + 13]);
                this.Write(values[offset + 14]);
                this.Write(values[offset + 15]);
                offset += 16;
                count -= 16;
            }
            while(count-- > 0)
                this.Write(values[offset++]);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(string value) {
#if USE_TWO_CHANNEL
            BitMethods.EncodeString(m_dataBuffer, ref m_offset, m_dataStream, ENCODER, value, this.WriteNextLength);
#else
            BitMethods.EncodeString(m_dataBuffer, ref m_offset, m_dataStream, ENCODER, value);
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((string[])values, offset, count);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(object value) {
            this.Write((string)value);
        }

        public override void Commit() {
            if(m_offset > 0) {
                m_dataStream.Write(m_dataBuffer, 0, m_offset);
                m_offset = 0;
            }
            base.Commit();
        }

        public Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_String();
        }

        #region Resume()
        public override void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding<string>(resumableChannels, rowCount);

            #region invalid code - more efficient but requires knowing the uncompressed size
            //var channelsBackup = resumableChannels.ToList();
            //this.Init(channelsBackup.Select(o => o.WriteOnly));
            //
            //// need to recompress in case new data affects existing bytes
            //var dataStream = channelsBackup.First();
            //dataStream.ReadOnly.CopyTo(dataStream.WriteOnly);
#if USE_TWO_CHANNEL
            //((IResumableDataStreamWriter)m_sizes).Resume(channelsBackup, rowCount);
#endif
            #endregion
        }
        #endregion
    }
}