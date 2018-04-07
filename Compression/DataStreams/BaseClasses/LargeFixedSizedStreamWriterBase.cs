using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;


namespace TimeSeriesDB.DataStreams.BaseClasses
{

    /// <summary>
    ///     Represents a stream writer whose items inside are fixed-sized and of large size (> 64 bits).
    /// </summary>
    public abstract class LargeFixedSizedStreamWriterBase {
        protected const int BUFFER_SIZE = 4096;

        protected readonly byte[] m_buffer = new byte[BUFFER_SIZE];
        protected int m_index = 0;

        protected Stream m_dataStream;

        public int ChannelCount => 1;

        public void Init(IEnumerable<Stream> channels) {
            m_dataStream = channels.First();
        }

        public void Reset() {
            m_index = 0;
            Helper.StreamReset(m_dataStream);
        }

        public void Commit() {
            // intentionally empty since large objects dont have mid-states in them
        }
        public void Flush() {
            m_dataStream.Flush();
        }

        public void Resume(IEnumerable<Stream> channels, long rowCount) {
            this.Init(channels);
            m_dataStream.Seek(0, SeekOrigin.End);
        }
        protected void InternalResume(IEnumerable<Writers.ResumableChannel> resumableChannels, long rowCount, int item_sizeof_in_bytes) {
            var channelsBackup = resumableChannels.ToList();
            this.Init(channelsBackup.Select(o => o.WriteOnly));

            var readStream = channelsBackup[0].ReadOnly;
            var writeStream = channelsBackup[0].WriteOnly;
            
            // need to recompress in case new data affects existing bytes
            const int BUFFER_SIZE = 4096;
            long remaining = rowCount * item_sizeof_in_bytes;
            var buffer = new byte[unchecked((int)Math.Min(remaining, BUFFER_SIZE))];

            while(remaining > 0) {
                int request = unchecked((int)Math.Min(remaining, BUFFER_SIZE));
                    
                int read = readStream.Read(buffer, 0, request);
                writeStream.Write(buffer, 0, read);
                remaining -= read;
            }
        }
    }
}