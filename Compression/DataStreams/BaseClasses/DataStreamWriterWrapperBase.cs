using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using static System.Runtime.CompilerServices.MethodImplOptions;
using System.IO;
using System.Linq;


namespace TimeSeriesDB.DataStreams.BaseClasses
{
    using Writers;

    public abstract class DataStreamWriterWrapperBase<TItemType> : IDataStreamWriter<TItemType> {
        protected readonly IDataStreamWriter<TItemType> m_internal;

        public int ChannelCount => 1;

        public DataStreamWriterWrapperBase(IDataStreamWriter<TItemType> stream) {
            m_internal = stream ?? throw new ArgumentNullException(nameof(stream));
        }

        public void Init(IEnumerable<Stream> channels) {
            m_internal.Init(channels);
        }

        [MethodImpl(AggressiveInlining)]
        public void Write(TItemType[] values, int offset, int count) {
            this.GetNext(values, offset, count);
            m_internal.Write(values, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(TItemType value) {
            this.GetNext(ref value);
            m_internal.Write(value);
        }

        [MethodImpl(AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((TItemType[])values, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(object value) {
            this.Write((TItemType)value);
        }

        public void Commit() {
            m_internal.Commit();
        }
        public void Reset() {
            m_internal.Reset();
        }
        public void Flush() {
            m_internal.Flush();
        }

        protected abstract void GetNext(ref TItemType value);
        protected abstract void GetNext(TItemType[] values, int offset, int count);
        public abstract Readers.IDataStreamReader CreateReader();

        [MethodImpl(AggressiveInlining)]
        protected void InternalResume<T>(IEnumerable<Stream> channels, long rowCount, ref T m_prev) {
            var channelsBackup = channels.ToList();
            var reader = this.CreateReader();
            reader.Init(channelsBackup);

            var size = unchecked((int)Math.Min(rowCount, 4096));
            var buffer = new T[size];

            while(rowCount > 0) {
                int request = unchecked((int)Math.Min(rowCount, 4096));
                int read = reader.Read(buffer, 0, request);
                if(read == 0)
                    break;
                rowCount -= read;
                m_prev = buffer[read - 1];
            }
            //this.Init(channelsBackup);
            ((IResumableDataStreamWriter)m_internal).Resume(channelsBackup, rowCount);
        }
    }


    public abstract class DataStreamWriterWrapperBase_Complex<TItemType, TStorageType> : IDataStreamWriter<TItemType> {
        protected readonly int BUFFER_SIZE; // 4096 / sizeof(TStorageType)
        protected readonly TStorageType[] m_buffer;

        protected readonly IDataStreamWriter<TStorageType> m_internal;

        public int ChannelCount => 1;

        public DataStreamWriterWrapperBase_Complex(IDataStreamWriter<TStorageType> stream, int sizeof_storage) {
            m_internal = stream ?? throw new ArgumentNullException(nameof(stream));

            this.BUFFER_SIZE = 4096 / sizeof_storage;
            m_buffer = new TStorageType[BUFFER_SIZE];
        }

        public void Init(IEnumerable<Stream> channels) {
            m_internal.Init(channels);
        }

        [MethodImpl(AggressiveInlining)]
        public void Write(TItemType[] values, int offset, int count) {
            while(count > 0) {
                var write = Math.Min(count, this.BUFFER_SIZE);

                this.ConvertToBuffer(values, offset, write);

                m_internal.Write(m_buffer, 0, write);

                count -= write;
                offset += write;
            }
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(TItemType value) {
            m_internal.Write(this.Convert(value));
        }

        [MethodImpl(AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((TItemType[])values, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(object value) {
            this.Write((TItemType)value);
        }

        public void Commit() {
            m_internal.Commit();
        }
        public virtual void Reset() {
            m_internal.Reset();
        }
        public void Flush() {
            m_internal.Flush();
        }

        protected abstract TStorageType Convert(TItemType value);
        protected abstract void ConvertToBuffer(TItemType[] values, int offset, int count);
        public abstract Readers.IDataStreamReader CreateReader();

        public virtual void Resume(IEnumerable<Stream> channels, long rowCount) {
            //this.Init(channels);
            ((IResumableDataStreamWriter)m_internal).Resume(channels, rowCount);
        }
        public virtual void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            //this.Init(resumableChannels.Select(o => o.WriteOnly));
            ((IResumableDataStreamWriter)m_internal).Resume(resumableChannels, rowCount);
        }
    }


    /// <summary>
    ///     Stores the difference between the current value and the previous one minus common delta across 16 items frame.
    ///     This stores very efficiently cyclic timestamps.
    ///     
    ///     ex: [50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65]      -> [1,50,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    ///         [0,10,20,30,40,50,60,70,80,90,100,110,120,130,140,150] -> [10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    ///         [5,10,20,30,40,50,60,70,80,90,100,110,120,130,140,150] -> [5,5,0,5,5,5,5,5,5,5,5,5,5,5,5,5,5]
    /// </summary>
    public abstract class DataStreamWriterWrapperBase_DeltaDelta<TItemType> : IDataStreamWriter<TItemType> {
        protected const int FRAME_SIZE         = Constants_DeltaDelta.FRAME_SIZE;
        protected readonly TItemType[] m_frame = new TItemType[FRAME_SIZE];

        protected readonly IDataStreamWriter<TItemType> m_internal;

        protected int m_index              = 0;
        protected int m_partialResumeIndex = 0;

        public int ChannelCount => 1;

        public DataStreamWriterWrapperBase_DeltaDelta(IDataStreamWriter<TItemType> stream) {
            m_internal = stream ?? throw new ArgumentNullException(nameof(stream));
        }

        public void Init(IEnumerable<Stream> channels) {
            m_internal.Init(channels);
        }

        public void Write(TItemType[] values, int offset, int count) {
            // finish frame
            while(m_index != 0 && count-- > 0)
                this.Write(values[offset++]);

            while(count >= FRAME_SIZE) {
                //this.FlushFullFrame(values, offset);

                Array.Copy(values, offset, m_frame, 0, FRAME_SIZE);
                this.FlushFullFrame();

                count -= FRAME_SIZE;
                offset += FRAME_SIZE;
            }

            // then process whatever remains
            while(count-- > 0)
                this.Write(values[offset++]);
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(TItemType value) {
            m_frame[m_index++] = value;

            if(m_index == FRAME_SIZE) {
                if(m_partialResumeIndex == 0)
                    this.FlushFullFrame();
                else {
                    this.FlushPartialFrame();
                    m_partialResumeIndex = 0;
                }
                
                m_index = 0;
            }
        }

        [MethodImpl(AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((TItemType[])values, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(object value) {
            this.Write((TItemType)value);
        }

        public void Commit() {
            if(m_index > m_partialResumeIndex)
                this.FlushPartialFrame();

            m_internal.Commit();
        }
        public void Reset() {
            m_index = 0;
            m_partialResumeIndex = 0;
            m_internal.Reset();

            // reset private vars in derived classes
            this.InternalReset();
        }
        public void Flush() {
            m_internal.Flush();
        }

        protected abstract void FlushFullFrame();
        protected abstract void FlushPartialFrame();
        protected abstract void InternalReset();
        public abstract Readers.IDataStreamReader CreateReader();

        [MethodImpl(AggressiveInlining)]
        protected void InternalResume<T>(IEnumerable<Stream> channels, long rowCount, ref T m_prev) {
            var channelsBackup = channels.ToList();
            var reader = this.CreateReader();
            reader.Init(channelsBackup);

            var size = unchecked((int)Math.Min(rowCount, 4096));
            var buffer = new T[size];

            while(rowCount > 0) {
                int request = unchecked((int)Math.Min(rowCount, 4096));
                int read = reader.Read(buffer, 0, request);
                if(read == 0)
                    break;
                rowCount -= read;
                m_prev = buffer[read - 1];
            }
            //this.Init(channelsBackup);
            ((IResumableDataStreamWriter)m_internal).Resume(channelsBackup, rowCount);

            // if we have a partial frame
            int partial_frame_item_count = unchecked((int)(rowCount % FRAME_SIZE));

            m_index              = partial_frame_item_count;
            m_partialResumeIndex = partial_frame_item_count;
        }
    }


    /// <summary>
    ///     Stores the difference between the current value and the previous one minus common delta across 16 items frame.
    ///     This stores very efficiently cyclic timestamps.
    ///     This class extends the buffer to BUFFER_SIZE instead of FRAME_SIZE, allowing far more efficient SIMD code.
    ///     This leads to a 20% speed gain on with AVX-1 over the non-SIMD version of this class.
    ///     
    ///     ex: [50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65]      -> [1,50,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    ///         [0,10,20,30,40,50,60,70,80,90,100,110,120,130,140,150] -> [10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    ///         [5,10,20,30,40,50,60,70,80,90,100,110,120,130,140,150] -> [5,5,0,5,5,5,5,5,5,5,5,5,5,5,5,5,5]
    /// </summary>
    public abstract class DataStreamWriterWrapperBase_DeltaDelta_WithSIMD<TItemType> : IDataStreamWriter<TItemType> {
        protected const int FRAME_SIZE               = Constants_DeltaDelta.FRAME_SIZE;
        private const int FRAMES_PER_BUFFER          = 4096 / (sizeof(ulong) * FRAME_SIZE); // should be sizeof(TItemType) but want to keep this a const
        protected const int BUFFER_SIZE              = FRAMES_PER_BUFFER * FRAME_SIZE;
        protected const int WRITE_BUFFER_SIZE        = FRAMES_PER_BUFFER * (FRAME_SIZE + 1);
        protected readonly TItemType[] m_buffer      = new TItemType[BUFFER_SIZE];
        protected readonly TItemType[] m_writeBuffer = new TItemType[WRITE_BUFFER_SIZE];

        protected readonly IDataStreamWriter<TItemType> m_internal;

        protected int m_index              = 0;
        protected int m_partialResumeIndex = 0;

        public int ChannelCount => 1;

        public DataStreamWriterWrapperBase_DeltaDelta_WithSIMD(IDataStreamWriter<TItemType> stream) {
            m_internal = stream ?? throw new ArgumentNullException(nameof(stream));
        }

        public void Init(IEnumerable<Stream> channels) {
            m_internal.Init(channels);
        }

        public void Write(TItemType[] values, int offset, int count) {
            while(count > 0) {
                int write = Math.Min(count, BUFFER_SIZE - m_index);
                Array.Copy(values, offset, m_buffer, m_index, write);

                offset += write;
                m_index += write;
                count -= write;

                if(m_index == BUFFER_SIZE)
                    this.FlushBuffer();
            }
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(TItemType value) {
            m_buffer[m_index++] = value;

            if(m_index == BUFFER_SIZE)
                this.FlushBuffer();
        }

        [MethodImpl(AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((TItemType[])values, offset, count);
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(object value) {
            this.Write((TItemType)value);
        }

        public void Commit() {
            if(m_index > m_partialResumeIndex)
                this.FlushBuffer();

            m_internal.Commit();
        }
        public void Reset() {
            m_index = 0;
            m_partialResumeIndex = 0;
            m_internal.Reset();

            // reset private vars in derived classes
            this.InternalReset();
        }
        public void Flush() {
            m_internal.Flush();
        }

        private void FlushBuffer() {
            this.ApplyDeltaToBuffer();

            int read               = 0;
            int write              = 0;
            int remaining          = m_index;
            int partialResumeIndex = m_partialResumeIndex;
            
            // begin by finishing partial frame
            if(partialResumeIndex != 0) {
                var remaining_in_partial_frame = Math.Min(m_index, FRAME_SIZE - 1) - partialResumeIndex;

                // copy (and remove delta-delta if it applied on a full frame)
                for(int i = 0; i < remaining_in_partial_frame; i++)
                    m_writeBuffer[write + i] = m_buffer[read + i]; // - min;

                read      = partialResumeIndex + remaining_in_partial_frame;
                write     = read + 1;
                remaining = m_index - read;
            }
            
            while(remaining >= FRAME_SIZE) {
                this.ApplyDeltaToFullFrame(read, write);

                read      += FRAME_SIZE;
                write     += FRAME_SIZE + 1;
                remaining -= FRAME_SIZE;
            }

            // if we happen to finish with a partial frame
            if(remaining > 0) {
                m_writeBuffer[write++] = default;
                // copy (and remove delta-delta if it applied on a full frame)
                for(int i = 0; i < remaining; i++)
                    m_writeBuffer[write + i] = m_buffer[read + i]; // - min;

                write += remaining + 1;
            }

            m_internal.Write(m_writeBuffer, partialResumeIndex, write - partialResumeIndex);

            // if we have a partial frame
            m_index              = remaining;
            m_partialResumeIndex = remaining;
        }

        protected abstract void ApplyDeltaToBuffer();

        protected abstract void ApplyDeltaToFullFrame(int readOffset, int writeOffset);
        protected abstract void InternalReset();
        public abstract Readers.IDataStreamReader CreateReader();

        [MethodImpl(AggressiveInlining)]
        protected void InternalResume<T>(IEnumerable<Stream> channels, long rowCount, ref T m_prev) {
            var channelsBackup = channels.ToList();
            var reader = this.CreateReader();
            reader.Init(channelsBackup);

            var size = unchecked((int)Math.Min(rowCount, 4096));
            var buffer = new T[size];

            while(rowCount > 0) {
                int request = unchecked((int)Math.Min(rowCount, 4096));
                int read = reader.Read(buffer, 0, request);
                if(read == 0)
                    break;
                rowCount -= read;
                m_prev = buffer[read - 1];
            }
            //this.Init(channelsBackup);
            ((IResumableDataStreamWriter)m_internal).Resume(channelsBackup, rowCount);

            // if we have a partial frame
            int partial_frame_item_count = unchecked((int)(rowCount % FRAME_SIZE));

            m_index              = partial_frame_item_count;
            m_partialResumeIndex = partial_frame_item_count;
        }
    }
}
