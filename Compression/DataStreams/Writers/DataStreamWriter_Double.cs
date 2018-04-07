#define NON_PORTABLE_CODE

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.IO;


namespace TimeSeriesDB.DataStreams.Writers
{
    using Internal;
    using BaseClasses;

    public sealed class DataStreamWriter_Double : DataStreamWriterWrapperBase_Complex<double, ulong>, IResumableDataStreamWriter {
        // dont use DataStreamWriter_UInt64_LSB for this because we are more likely to write on the high bytes (MSB)
        public DataStreamWriter_Double() : base(new DataStreamWriter_UInt64(), sizeof(ulong)) { }

        protected override ulong Convert(double value) {
            return BitMethods.ConvertToBits(value);
        }

        protected override void ConvertToBuffer(double[] values, int offset, int count) {
#if NON_PORTABLE_CODE
            Debug.Assert(BitConverter.IsLittleEndian);
            Buffer.BlockCopy(values, offset * sizeof(double), m_buffer, 0, count * sizeof(double));
#else
            if(BitConverter.IsLittleEndian)
                Buffer.BlockCopy(values, offset * sizeof(double), m_buffer, 0, count * sizeof(double));
            else {
                for(int i = 0; i < count; i++)
                    m_buffer[i] = BitMethods.ConvertToBits(values[offset++]);
            }
#endif
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_Double();
        }
    }
}
