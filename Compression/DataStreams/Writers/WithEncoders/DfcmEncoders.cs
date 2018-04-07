using System;
using System.Linq;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using static System.Runtime.CompilerServices.MethodImplOptions;
using System.IO;


namespace TimeSeriesDB.DataStreams.Writers.WithEncoders
{
    using Internal;
    using BaseClasses;
    using IO;

    #region public class Dfcm_DataStreamWriter_Double
    public sealed class Dfcm_DataStreamWriter_Double : DataStreamWriterWrapperBase_Complex<double, ulong>, IResumableDataStreamWriter {
        private readonly DfcmPredictor_Double m_predictor = new DfcmPredictor_Double();

        // could use either DataStreamWriter_UInt64/DataStreamWriter_UInt64_LSB, technically should use DataStreamWriter_UInt64 
        // but since we don't have fast intrinsics in .NET for now we avoid it
        public Dfcm_DataStreamWriter_Double() : base(new DataStreamWriter_UInt64_LSB(), sizeof(ulong)) { }

        protected override ulong Convert(double value) {
            return this.InternalConvert(value);
        }

        protected override void ConvertToBuffer(double[] values, int offset, int count) {
            int write = 0;
            while(count >= 16) {
                m_buffer[write + 0] = this.InternalConvert(values[offset + 0]);
                m_buffer[write + 1] = this.InternalConvert(values[offset + 1]);
                m_buffer[write + 2] = this.InternalConvert(values[offset + 2]);
                m_buffer[write + 3] = this.InternalConvert(values[offset + 3]);
                m_buffer[write + 4] = this.InternalConvert(values[offset + 4]);
                m_buffer[write + 5] = this.InternalConvert(values[offset + 5]);
                m_buffer[write + 6] = this.InternalConvert(values[offset + 6]);
                m_buffer[write + 7] = this.InternalConvert(values[offset + 7]);
                m_buffer[write + 8] = this.InternalConvert(values[offset + 8]);
                m_buffer[write + 9] = this.InternalConvert(values[offset + 9]);
                m_buffer[write + 10] = this.InternalConvert(values[offset + 10]);
                m_buffer[write + 11] = this.InternalConvert(values[offset + 11]);
                m_buffer[write + 12] = this.InternalConvert(values[offset + 12]);
                m_buffer[write + 13] = this.InternalConvert(values[offset + 13]);
                m_buffer[write + 14] = this.InternalConvert(values[offset + 14]);
                m_buffer[write + 15] = this.InternalConvert(values[offset + 15]);
                write += 16;
                offset += 16;
                count -= 16;
            }
            while(count-- > 0)
                m_buffer[write++] = this.InternalConvert(values[offset++]);
        }

        public override void Reset() {
            base.Reset();
            m_predictor.Reset();
        }

        [MethodImpl(AggressiveInlining)]
        private ulong InternalConvert(double value) {
            var ulongValue = BitMethods.ConvertToBits(value);
            var predicted = m_predictor.PredictNext();
            m_predictor.Update(ulongValue);
            return predicted ^ ulongValue;
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.Dfcm_DataStreamReader_Double();
        }

        public override void Resume(IEnumerable<Stream> channels, long rowCount) {
            // entire re-read is necessary because of the dfcm table state
            var channelsBackup = channels.ToList();
            var reader = this.CreateReader();
            reader.Init(channelsBackup);

            var size = unchecked((int)Math.Min(rowCount, 4096));
            var buffer = new double[size];

            while(rowCount > 0) {
                int request = unchecked((int)Math.Min(rowCount, 4096));
                int read = reader.Read(buffer, 0, request);
                rowCount -= read;

                // process the data the same as if it was being written, but without performing any write
                for(int i = 0; i < read; i++)
                    this.InternalConvert(buffer[i]);
            }

            base.Resume(channelsBackup, rowCount);
        }
        public override void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            var resumable = this as IResumableDataStreamWriter;
            resumable.ResumeByRebuilding<double>(resumableChannels, rowCount);

            #region invalid code -- much more efficient but would require IDataStreamReader.GetChannelsPosition(long[] channels)
            //// non-obvious note:
            //// this code will crash if the rowCount differs from the actual amount of rows
            //// if trying to pass a rowCount smaller than the amount of rows to specify a resume point, it wont work
            //// the reason is that the reader can pre-buffer its reads, meaning its reading more than required
            //// this in turn affects the forwarding stream making us compress pre-buffered data that isnt meant to be added
            ////
            //// for this code to work, you need to add IDataStreamReader.GetChannelsPosition(long[] channels); and recopy the proper amount of bytes
            //// this cant be done otherwise as the code assumes that pre-fetched data is part of the stream, and since compressed streams are forward-only,
            //// they don't know ahead of time their total length, meaning they will attempt to wrongly decode the remainder bits in the final byte of the stream 
            //// this will cause crashes in the re-read as those extra decoded bytes make it through the resumed stream.
            //// or alternatively, you'd need compressed streams to have a decompressed size appended to every chunk
            //
            //// entire re-read is necessary because of the dfcm table state
            //var channelsBackup = resumableChannels.ToList();
            //var forwardingStreams = channelsBackup.Select(o => new ForwardingStream(o.ReadOnly) { UserValue = o.WriteOnly });
            //
            //// note: this code will fail spectacularly if the compressed stream returns zero-padded data when buffering past reading
            //foreach(var f in forwardingStreams) {
            //    f.Reading += (object sender, ForwardingStream.StreamReadEventArgs e) => {
            //        var source = sender as ForwardingStream;
            //        var destination = source.UserValue as Stream;
            //        destination.Write(e.Buffer, e.Offset, e.Read);
            //    };
            //}
            //
            //var reader = this.CreateReader();
            //reader.Init(forwardingStreams);
            //this.Init(channelsBackup.Select(o => o.WriteOnly));
            //
            //var size = unchecked((int)Math.Min(rowCount, 4096));
            //var buffer = new double[size];
            //
            //while(rowCount > 0) {
            //    int request = unchecked((int)Math.Min(rowCount, 4096));
            //    int read = reader.Read(buffer, 0, request);
            //    rowCount -= read;
            //
            //    // process the data the same as if it was being written, but without performing any write
            //    for(int i = 0; i < read; i++)
            //        this.InternalConvert(buffer[i]);
            //}
            ////base.Resume(channelsBackup, rowCount);
            #endregion
        }
    }
    #endregion
    #region public class Dfcm_DataStreamWriter_Float
    public sealed class Dfcm_DataStreamWriter_Float : DataStreamWriterWrapperBase_Complex<float, uint>, IResumableDataStreamWriter {
        private readonly DfcmPredictor_Float m_predictor = new DfcmPredictor_Float();

        public Dfcm_DataStreamWriter_Float() : base(new DataStreamWriter_UInt32(), sizeof(uint)) { }

        protected override uint Convert(float value) {
            return this.InternalConvert(value);
        }

        protected override void ConvertToBuffer(float[] values, int offset, int count) {
            int write = 0;
            while(count >= 16) {
                m_buffer[write + 0] = this.InternalConvert(values[offset + 0]);
                m_buffer[write + 1] = this.InternalConvert(values[offset + 1]);
                m_buffer[write + 2] = this.InternalConvert(values[offset + 2]);
                m_buffer[write + 3] = this.InternalConvert(values[offset + 3]);
                m_buffer[write + 4] = this.InternalConvert(values[offset + 4]);
                m_buffer[write + 5] = this.InternalConvert(values[offset + 5]);
                m_buffer[write + 6] = this.InternalConvert(values[offset + 6]);
                m_buffer[write + 7] = this.InternalConvert(values[offset + 7]);
                m_buffer[write + 8] = this.InternalConvert(values[offset + 8]);
                m_buffer[write + 9] = this.InternalConvert(values[offset + 9]);
                m_buffer[write + 10] = this.InternalConvert(values[offset + 10]);
                m_buffer[write + 11] = this.InternalConvert(values[offset + 11]);
                m_buffer[write + 12] = this.InternalConvert(values[offset + 12]);
                m_buffer[write + 13] = this.InternalConvert(values[offset + 13]);
                m_buffer[write + 14] = this.InternalConvert(values[offset + 14]);
                m_buffer[write + 15] = this.InternalConvert(values[offset + 15]);
                write += 16;
                offset += 16;
                count -= 16;
            }
            while(count-- > 0)
                m_buffer[write++] = this.InternalConvert(values[offset++]);
        }

        public override void Reset() {
            base.Reset();
            m_predictor.Reset();
        }

        [MethodImpl(AggressiveInlining)]
        private uint InternalConvert(float value) {
            var uintValue = BitMethods.ConvertToBits(value);
            var predicted = m_predictor.PredictNext();
            m_predictor.Update(uintValue);
            return predicted ^ uintValue;
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.WithDecoders.Dfcm_DataStreamReader_Float();
        }

        public override void Resume(IEnumerable<Stream> channels, long rowCount) {
            // entire re-read is necessary because of the dfcm table state
            var channelsBackup = channels.ToList();
            var reader = this.CreateReader();
            reader.Init(channelsBackup);

            var size = unchecked((int)Math.Min(rowCount, 4096));
            var buffer = new float[size];

            while(rowCount > 0) {
                int request = unchecked((int)Math.Min(rowCount, 4096));
                int read = reader.Read(buffer, 0, request);
                rowCount -= read;

                // process the data the same as if it was being written, but without performing any write
                for(int i = 0; i < read; i++)
                    this.InternalConvert(buffer[i]);
            }

            base.Resume(channelsBackup, rowCount);
        }
        public override void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            var resumable = this as IResumableDataStreamWriter;
            resumable.ResumeByRebuilding<float>(resumableChannels, rowCount);
        }
    }
    #endregion
}
