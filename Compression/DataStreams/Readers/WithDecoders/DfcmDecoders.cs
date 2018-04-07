using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using static System.Runtime.CompilerServices.MethodImplOptions;
using System.IO;


namespace TimeSeriesDB.DataStreams.Readers.WithDecoders
{
    using Internal;
    using BaseClasses;

    #region public class Dfcm_DataStreamReader_Double
    public sealed class Dfcm_DataStreamReader_Double : DataStreamReaderWrapperBase_Complex<double, ulong> {
        private readonly DfcmPredictor_Double m_predictor = new DfcmPredictor_Double();

        // could use either DataStreamWriter_UInt64/DataStreamWriter_UInt64_LSB, technically should use DataStreamWriter_UInt64 
        public Dfcm_DataStreamReader_Double() : base(new DataStreamReader_UInt64_LSB(), sizeof(ulong)) { }

        protected override double Convert(ulong value) {
            return this.InternalConvert(value);
        }

        protected override void ConvertFromBuffer(double[] values, int offset, int count) {
            int read = 0;
            while(count >= 16) {
                values[offset + 0] = this.InternalConvert(m_buffer[read + 0]);
                values[offset + 1] = this.InternalConvert(m_buffer[read + 1]);
                values[offset + 2] = this.InternalConvert(m_buffer[read + 2]);
                values[offset + 3] = this.InternalConvert(m_buffer[read + 3]);
                values[offset + 4] = this.InternalConvert(m_buffer[read + 4]);
                values[offset + 5] = this.InternalConvert(m_buffer[read + 5]);
                values[offset + 6] = this.InternalConvert(m_buffer[read + 6]);
                values[offset + 7] = this.InternalConvert(m_buffer[read + 7]);
                values[offset + 8] = this.InternalConvert(m_buffer[read + 8]);
                values[offset + 9] = this.InternalConvert(m_buffer[read + 9]);
                values[offset + 10] = this.InternalConvert(m_buffer[read + 10]);
                values[offset + 11] = this.InternalConvert(m_buffer[read + 11]);
                values[offset + 12] = this.InternalConvert(m_buffer[read + 12]);
                values[offset + 13] = this.InternalConvert(m_buffer[read + 13]);
                values[offset + 14] = this.InternalConvert(m_buffer[read + 14]);
                values[offset + 15] = this.InternalConvert(m_buffer[read + 15]);
                read += 16;
                offset += 16;
                count -= 16;
            }
            while(count-- > 0)
                values[offset++] = this.InternalConvert(m_buffer[read++]);
        }

        [MethodImpl(AggressiveInlining)]
        private double InternalConvert(ulong value) {
            value ^= m_predictor.PredictNext();
            m_predictor.Update(value);
            return BitMethods.ConvertToDouble(value);
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.Dfcm_DataStreamWriter_Double();
        }
    }
    #endregion
    #region public class Dfcm_DataStreamReader_Float
    public sealed class Dfcm_DataStreamReader_Float : DataStreamReaderWrapperBase_Complex<float, uint> {
        private readonly DfcmPredictor_Float m_predictor = new DfcmPredictor_Float();

        public Dfcm_DataStreamReader_Float() : base(new DataStreamReader_UInt32(), sizeof(uint)) { }

        protected override float Convert(uint value) {
            return this.InternalConvert(value);
        }

        protected override void ConvertFromBuffer(float[] values, int offset, int count) {
            int read = 0;
            while(count >= 16) {
                values[offset + 0] = this.InternalConvert(m_buffer[read + 0]);
                values[offset + 1] = this.InternalConvert(m_buffer[read + 1]);
                values[offset + 2] = this.InternalConvert(m_buffer[read + 2]);
                values[offset + 3] = this.InternalConvert(m_buffer[read + 3]);
                values[offset + 4] = this.InternalConvert(m_buffer[read + 4]);
                values[offset + 5] = this.InternalConvert(m_buffer[read + 5]);
                values[offset + 6] = this.InternalConvert(m_buffer[read + 6]);
                values[offset + 7] = this.InternalConvert(m_buffer[read + 7]);
                values[offset + 8] = this.InternalConvert(m_buffer[read + 8]);
                values[offset + 9] = this.InternalConvert(m_buffer[read + 9]);
                values[offset + 10] = this.InternalConvert(m_buffer[read + 10]);
                values[offset + 11] = this.InternalConvert(m_buffer[read + 11]);
                values[offset + 12] = this.InternalConvert(m_buffer[read + 12]);
                values[offset + 13] = this.InternalConvert(m_buffer[read + 13]);
                values[offset + 14] = this.InternalConvert(m_buffer[read + 14]);
                values[offset + 15] = this.InternalConvert(m_buffer[read + 15]);
                read += 16;
                offset += 16;
                count -= 16;
            }
            while(count-- > 0)
                values[offset++] = this.InternalConvert(m_buffer[read++]);
        }

        [MethodImpl(AggressiveInlining)]
        private float InternalConvert(uint value) {
            value ^= m_predictor.PredictNext();
            m_predictor.Update(value);
            return BitMethods.ConvertToFloat(value);
        }

        public override Writers.IDataStreamWriter CreateWriter() {
            return new Writers.WithEncoders.Dfcm_DataStreamWriter_Float();
        }
    }
    #endregion
}
