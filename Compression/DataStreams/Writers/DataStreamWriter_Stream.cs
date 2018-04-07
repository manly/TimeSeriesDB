//#define USE_TWO_CHANNEL // if disabled, will use one channel

using System;
using System.IO;
using System.Collections.Generic;
using System.Runtime.CompilerServices;


namespace TimeSeriesDB.DataStreams.Writers
{
    using Internal;
    using BaseClasses;

    public sealed class DataStreamWriter_Stream : VarSizedStreamWriterBase, IDataStreamWriter<Stream>, IResumableDataStreamWriter {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(Stream[] values, int offset, int count) {
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
        public void Write(Stream value) {
#if USE_TWO_CHANNEL
            BitMethods.EncodeStream(m_dataStream, value, this.WriteNextLength);
#else
            BitMethods.EncodeStream(m_buffer, ref m_offset, m_dataStream, value);
#endif
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((Stream[])values, offset, count);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(object value) {
            this.Write((Stream)value);
        }

        public Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_Stream();
        }

        #region Resume()
        public override void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding<Stream>(resumableChannels, rowCount);

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