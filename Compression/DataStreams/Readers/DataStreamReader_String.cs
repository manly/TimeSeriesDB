//#define USE_TWO_CHANNEL // if disabled, will use one channel

using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Runtime.CompilerServices;


namespace TimeSeriesDB.DataStreams.Readers
{
    using Internal;
    using BaseClasses;

    public sealed class DataStreamReader_String : VarSizedStreamReaderBase, IDataStreamReader<string> {
        private static readonly Encoding ENCODER = Encoding.UTF8;
        private const int CHAR_BUFFER_SIZE       = 4096;

#if USE_TWO_CHANNEL
        private const int BUFFER_SIZE            = 4096;
        private readonly byte[] m_buffer     = new byte[BUFFER_SIZE];
        private int m_offset;
        private int m_readCount = 0;
#endif

        private readonly Decoder m_decoder = ENCODER.GetDecoder();

        private readonly char[] m_decompressBuffer = new char[CHAR_BUFFER_SIZE];


        //public void Reset() {
        //    m_offset = 0;
        //    m_decoder.Reset();
        //}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Read(string[] items, int offset, int count) {
            var startCount = count;

            while(count >= 16) {
                items[offset + 0] = this.ReadOne();
                items[offset + 1] = this.ReadOne();
                items[offset + 2] = this.ReadOne();
                items[offset + 3] = this.ReadOne();
                items[offset + 4] = this.ReadOne();
                items[offset + 5] = this.ReadOne();
                items[offset + 6] = this.ReadOne();
                items[offset + 7] = this.ReadOne();
                items[offset + 8] = this.ReadOne();
                items[offset + 9] = this.ReadOne();
                items[offset + 10] = this.ReadOne();
                items[offset + 11] = this.ReadOne();
                items[offset + 12] = this.ReadOne();
                items[offset + 13] = this.ReadOne();
                items[offset + 14] = this.ReadOne();
                items[offset + 15] = this.ReadOne();
                offset += 16;
                count -= 16;
            }
            while(count > 0) {
                items[offset++] = this.ReadOne();
                count--;
            }
            
            return startCount - count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string ReadOne() {
#if USE_TWO_CHANNEL
            return BitMethods.DecodeString(m_dataStream, m_buffer, m_decompressBuffer, ref m_offset, ref m_read, m_decoder, this.ReadNextLength);
#else
            return BitMethods.DecodeString(m_dataStream, m_buffer, m_decompressBuffer, ref m_offset, ref m_read, m_decoder);
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Read(Array items, int offset, int count) {
            return this.Read((string[])items, offset, count);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        object IDataStreamReader.ReadOne() {
            return this.ReadOne();
        }

        public void Skip(int count) {
            while(count >= 16) {
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                this.InternalSkipOne();
                count -= 16;
            }
            while(count-- > 0)
                this.InternalSkipOne();
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InternalSkipOne() {
#if USE_TWO_CHANNEL
            BitMethods.SkipVarSizedObject(m_seekableDataStream, ref m_dataStreamPosition, this.ReadNextLength);
#else
            BitMethods.SkipVarSizedObject(m_buffer, ref m_offset, ref m_read, m_dataStream);
#endif
        }

        public Writers.IDataStreamWriter CreateWriter() {
            return new Writers.DataStreamWriter_String();
        }
    }
}