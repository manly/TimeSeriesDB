using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;


namespace TimeSeriesDB.DataStreams.BaseClasses
{

    /// <summary>
    ///     Optimized for storage of small static-sized items.
    /// </summary>
    public abstract class DoubleBufferedStreamWriterBase {
        protected const int PRIMARY_BUFFER_SIZE_IN_BITS  = sizeof(uint) * 8;
        protected const int SECONDARY_BUFFER_SIZE        = 4096;

        protected uint            m_primaryBuffer        = 0;
        protected int             m_primaryBufferIndex   = 0;

        protected readonly byte[] m_secondaryBuffer      = new byte[SECONDARY_BUFFER_SIZE];
        protected int             m_secondaryBufferIndex = 0;

        protected Stream m_stream;

        public int ChannelCount => 1;

        public void Init(IEnumerable<Stream> channels) {
            m_stream = channels.First();
        }

        public void Reset() {
            m_primaryBuffer = 0;
            m_primaryBufferIndex = 0;
            m_secondaryBufferIndex = 0;
            Helper.StreamReset(m_stream);
        }

        public void Flush() {
            m_stream.Flush();
        }

        public void Commit() {
            while(m_primaryBufferIndex > 0) {
                m_secondaryBuffer[m_secondaryBufferIndex++] = unchecked((byte)(m_primaryBuffer & 0xFF));
                m_primaryBuffer >>= 8;
                m_primaryBufferIndex -= 8;
            }

            // could be < 0, in the case of a partially completed buffer
            m_primaryBufferIndex = 0;

            if(m_secondaryBufferIndex > 0) {
                m_stream.Write(m_secondaryBuffer, 0, m_secondaryBufferIndex);
                m_secondaryBufferIndex = 0;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void FlushPrimaryBuffer(uint buffer) {
            m_secondaryBuffer[m_secondaryBufferIndex++] = unchecked((byte)((buffer >> 0) & 0xFF));
            m_secondaryBuffer[m_secondaryBufferIndex++] = unchecked((byte)((buffer >> 8) & 0xFF));
            m_secondaryBuffer[m_secondaryBufferIndex++] = unchecked((byte)((buffer >> 16) & 0xFF));
            m_secondaryBuffer[m_secondaryBufferIndex++] = unchecked((byte)((buffer >> 24) & 0xFF));

            if(m_secondaryBufferIndex == SECONDARY_BUFFER_SIZE) {
                m_secondaryBufferIndex = 0;
                m_stream.Write(m_secondaryBuffer, 0, SECONDARY_BUFFER_SIZE);
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void FlushPrimaryBuffer() {
            m_secondaryBuffer[m_secondaryBufferIndex++] = unchecked((byte)((m_primaryBuffer >> 0) & 0xFF));
            m_secondaryBuffer[m_secondaryBufferIndex++] = unchecked((byte)((m_primaryBuffer >> 8) & 0xFF));
            m_secondaryBuffer[m_secondaryBufferIndex++] = unchecked((byte)((m_primaryBuffer >> 16) & 0xFF));
            m_secondaryBuffer[m_secondaryBufferIndex++] = unchecked((byte)((m_primaryBuffer >> 24) & 0xFF));
            
            m_primaryBuffer      = 0;
            m_primaryBufferIndex = 0;

            if(m_secondaryBufferIndex == SECONDARY_BUFFER_SIZE) {
                m_secondaryBufferIndex = 0;
                m_stream.Write(m_secondaryBuffer, 0, SECONDARY_BUFFER_SIZE);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void InternalResume(IEnumerable<Stream> channels, long rowCount, int item_sizeof_in_bits) {
            this.Init(channels);

            int CHUNK_BYTE_SIZE = (item_sizeof_in_bits / 8) + ((item_sizeof_in_bits % 8) == 0 ? 0 : 1);
            int ITEMS_PER_CHUNK = (CHUNK_BYTE_SIZE * 8) / item_sizeof_in_bits;
            int items_in_last_chunk = unchecked((int)(rowCount % ITEMS_PER_CHUNK));

            if(items_in_last_chunk == 0)
                m_stream.Seek(0, SeekOrigin.End);
            else {
                m_stream.Seek(-CHUNK_BYTE_SIZE, SeekOrigin.End);
                uint chunk = 0;
                for(int i = 0; i < CHUNK_BYTE_SIZE; i++) {
                    var b = m_stream.ReadByte();
                    if(b < 0)
                        throw new InvalidOperationException();
                    chunk |= unchecked((byte)(b << (i * 8)));
                }
                m_stream.Seek(-CHUNK_BYTE_SIZE, SeekOrigin.Current);

                uint mask            = unchecked((uint)((1 << (item_sizeof_in_bits * items_in_last_chunk)) - 1));
                m_primaryBuffer      = chunk & mask;
                m_primaryBufferIndex = item_sizeof_in_bits * items_in_last_chunk;
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void InternalResume(IEnumerable<Writers.ResumableChannel> resumableChannels, long rowCount, int item_sizeof_in_bits) {
            var channelsBackup = resumableChannels.ToList();
            this.Init(channelsBackup.Select(o => o.WriteOnly));

            int CHUNK_BYTE_SIZE = (item_sizeof_in_bits / 8) + ((item_sizeof_in_bits % 8) == 0 ? 0 : 1);
            int ITEMS_PER_CHUNK = (CHUNK_BYTE_SIZE * 8) / item_sizeof_in_bits;
            int items_in_last_chunk = unchecked((int)(rowCount % ITEMS_PER_CHUNK));

            const int BUFFER_SIZE = 4096;
            var bytes_to_skip = (rowCount - items_in_last_chunk) * ITEMS_PER_CHUNK * CHUNK_BYTE_SIZE;
            var buffer = new byte[unchecked((int)Math.Min(bytes_to_skip, BUFFER_SIZE))];

            var readStream = channelsBackup[0].ReadOnly;
            var writeStream = channelsBackup[0].WriteOnly;

            while(bytes_to_skip > 0) {
                int request = unchecked((int)Math.Min(bytes_to_skip, BUFFER_SIZE));
                    
                int read = readStream.Read(buffer, 0, request);
                writeStream.Write(buffer, 0, read);
                bytes_to_skip -= read;
            }

            if(items_in_last_chunk != 0) {
                uint chunk = 0;
                for(int i = 0; i < CHUNK_BYTE_SIZE; i++) {
                    var b = readStream.ReadByte();
                    if(b < 0)
                        throw new InvalidOperationException();
                    chunk |= unchecked((byte)(b << (i * 8)));
                }
                uint mask            = unchecked((uint)((1 << (item_sizeof_in_bits * items_in_last_chunk)) - 1));
                m_primaryBuffer      = chunk & mask;
                m_primaryBufferIndex = item_sizeof_in_bits * items_in_last_chunk;
            } else {
                m_primaryBuffer      = 0;
                m_primaryBufferIndex = 0;
            }
        }
    }
}