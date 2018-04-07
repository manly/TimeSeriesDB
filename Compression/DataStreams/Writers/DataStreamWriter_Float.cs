#define NON_PORTABLE_CODE

using System;
using System.Runtime.CompilerServices;
using System.Diagnostics;
using System.IO;


namespace TimeSeriesDB.DataStreams.Writers
{
    using Internal;
    using BaseClasses;

    public sealed class DataStreamWriter_Float : DataStreamWriterWrapperBase_Complex<float, uint>, IResumableDataStreamWriter {
        public DataStreamWriter_Float() : base(new DataStreamWriter_UInt32(), sizeof(uint)) { }

        protected override uint Convert(float value) {
            return BitMethods.ConvertToBits(value);
        }

        protected override void ConvertToBuffer(float[] values, int offset, int count) {
#if NON_PORTABLE_CODE
            Debug.Assert(BitConverter.IsLittleEndian);
            Buffer.BlockCopy(values, offset * sizeof(float), m_buffer, 0, count * sizeof(float));
#else
            if(BitConverter.IsLittleEndian)
                Buffer.BlockCopy(values, offset * sizeof(float), m_buffer, 0, count * sizeof(float));
            else{
                for(int i = 0; i < count; i++)
                    m_buffer[i] = BitMethods.ConvertToBits(values[offset++]);
            }
#endif
        }

        public override Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_Float();
        }
    }
}
