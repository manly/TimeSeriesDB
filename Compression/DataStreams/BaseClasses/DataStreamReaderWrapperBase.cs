using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.IO;


namespace TimeSeriesDB.DataStreams.BaseClasses
{
    using Readers;

    public abstract class DataStreamReaderWrapperBase<TItemType> : IDataStreamReader<TItemType> {
        public ulong ItemCount { get; set; }
        public virtual int ChannelCount => 1;

        protected readonly IDataStreamReader<TItemType> m_internal;

        public DataStreamReaderWrapperBase(IDataStreamReader<TItemType> stream) {
            m_internal = stream ?? throw new ArgumentNullException(nameof(stream));
        }

        public void Init(IEnumerable<Stream> channels) {
            m_internal.Init(channels);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Read(TItemType[] items, int offset, int count) {
            int read = m_internal.Read(items, offset, count);
            this.GetNext(items, offset, read);
            return read;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TItemType ReadOne() {
            var item = m_internal.ReadOne();
            this.GetNext(ref item);
            return item;
        }

        public void Skip(int count) {
            m_internal.Skip(count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Read(Array items, int offset, int count) {
            return this.Read((TItemType[])items, offset, count);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        object IDataStreamReader.ReadOne() {
            return this.ReadOne();
        }
        
        protected abstract void GetNext(ref TItemType value);
        protected abstract void GetNext(TItemType[] values, int offset, int count);
        public abstract Writers.IDataStreamWriter CreateWriter();
    }


    public abstract class DataStreamReaderWrapperBase_Complex<TItemType, TStorageType> : IDataStreamReader<TItemType> {
        protected readonly int BUFFER_SIZE; // 4096 / sizeof(TStorageType)
        protected readonly TStorageType[] m_buffer;

        public ulong ItemCount { get; set; }
        public virtual int ChannelCount => 1;

        protected readonly IDataStreamReader<TStorageType> m_internal;

        public DataStreamReaderWrapperBase_Complex(IDataStreamReader<TStorageType> stream, int sizeof_storage) {
            m_internal = stream ?? throw new ArgumentNullException(nameof(stream));

            this.BUFFER_SIZE = 4096 / sizeof_storage;
            m_buffer = new TStorageType[BUFFER_SIZE];
        }

        public void Init(IEnumerable<Stream> channels) {
            m_internal.Init(channels);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Read(TItemType[] items, int offset, int count) {
            int total = 0;

            while(count > 0) {
                int request = Math.Min(count, this.BUFFER_SIZE);

                int read = m_internal.Read(m_buffer, 0, request);
                if(read == 0)
                    break;

                this.ConvertFromBuffer(items, offset, read);

                count -= read;
                offset += read;
                total += read;
            }

            return total;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TItemType ReadOne() {
            return this.Convert(m_internal.ReadOne());
        }

        public void Skip(int count) {
            m_internal.Skip(count);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Read(Array items, int offset, int count) {
            return this.Read((TItemType[])items, offset, count);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        object IDataStreamReader.ReadOne() {
            return this.ReadOne();
        }

        protected abstract TItemType Convert(TStorageType value);
        protected abstract void ConvertFromBuffer(TItemType[] values, int offset, int count);
        public abstract Writers.IDataStreamWriter CreateWriter();
    }


    /// <summary>
    ///     Stores the difference between the current value and the previous one minus common delta across 16 items frame.
    ///     This stores very efficiently cyclic timestamps.
    ///     
    ///     ex: [50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65]      -> [1,50,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    ///         [0,10,20,30,40,50,60,70,80,90,100,110,120,130,140,150] -> [10,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    ///         [5,10,20,30,40,50,60,70,80,90,100,110,120,130,140,150] -> [5,5,0,5,5,5,5,5,5,5,5,5,5,5,5,5,5]
    /// </summary>
    public abstract class DataStreamReaderWrapperBase_DeltaDelta<TItemType> : IDataStreamReader<TItemType> {
        protected const int FRAME_SIZE = Constants_DeltaDelta.FRAME_SIZE;
        protected readonly int BUFFER_SIZE; // 4096 / sizeof(TItemType)

        protected readonly TItemType[] m_buffer;

        public ulong ItemCount { get; set; }
        public virtual int ChannelCount => 1;

        private readonly IDataStreamReader<TItemType> m_internal;

        protected int m_bufferIndex = 0;
        protected int m_bufferCount = 0;
        protected int m_frameEndIndex = 0;

        public DataStreamReaderWrapperBase_DeltaDelta(IDataStreamReader<TItemType> stream, int sizeof_item) {
            this.BUFFER_SIZE = 4096 / sizeof_item;
            m_buffer = new TItemType[this.BUFFER_SIZE];

            m_internal = stream ?? throw new ArgumentNullException(nameof(stream));
        }

        public void Init(IEnumerable<Stream> channels) {
            m_internal.Init(channels);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Read(TItemType[] items, int offset, int count) {
            int total = 0;

            while(count >= FRAME_SIZE) {
                // finish frame
                while(m_bufferIndex != m_frameEndIndex && count-- > 0) {
                    if(!this.InternalReadOne(ref items[offset++]))
                        return total;

                    total++;
                }

                bool partialFrame = false;
                while(m_bufferIndex == m_frameEndIndex && count >= FRAME_SIZE) {
                    if(!this.RefreshFrame()) {
                        partialFrame = true;
                        break;
                    }

                    Array.Copy(m_buffer, m_bufferIndex, items, offset, FRAME_SIZE);

                    count -= FRAME_SIZE;
                    total += FRAME_SIZE;
                    offset += FRAME_SIZE;
                    m_bufferIndex += FRAME_SIZE;
                }
                if(partialFrame)
                    break;
            }

            // then process whatever remains
            while(count-- > 0) {
                if(!this.InternalReadOne(ref items[offset++]))
                    return total;
                total++;
            }

            return total;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TItemType ReadOne() {
            TItemType res = default;
            this.InternalReadOne(ref res);
            return res;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Read(Array items, int offset, int count) {
            return this.Read((TItemType[])items, offset, count);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        object IDataStreamReader.ReadOne() {
            return this.ReadOne();
        }

        #region Skip()
        public void Skip(int count) {
            TItemType temp = default;

            while(count >= FRAME_SIZE) {
                // finish frame
                while(m_bufferIndex != m_frameEndIndex && count-- > 0) {
                    if(!this.InternalReadOne(ref temp))
                        return;
                }

                bool partialFrame = false;
                while(m_bufferIndex == m_frameEndIndex && count >= FRAME_SIZE) {
                    if(!this.RefreshFrame()) {
                        partialFrame = true;
                        break;
                    }

                    count -= FRAME_SIZE;
                    m_bufferIndex += FRAME_SIZE;
                }
                if(partialFrame)
                    break;
            }

            // then process whatever remains
            while(count-- > 0) {
                if(!this.InternalReadOne(ref temp))
                    return;
            }
        }
        #endregion

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool InternalReadOne(ref TItemType value) {
            if(m_bufferIndex == m_frameEndIndex) {
                this.RefreshFrame();

                if(m_bufferCount == 0) {
                    //value = default; // this was here when 'value' was an 'out' parameter, which was wrongly writing a result
                    return false;
                }
            }

            value = m_buffer[m_bufferIndex++];
            return true;
        }

        /// <summary>
        ///     Read FRAME_SIZE+1 items and apply deltadelta starting at m_bufferIndex+1 for FRAME_SIZE items.
        /// </summary>
        protected abstract void ReadFullFrame();
        /// <summary>
        ///     Read m_frameEndIndex-m_bufferIndex items and apply deltadelta starting at m_bufferIndex+1 for (m_frameEndIndex-m_bufferIndex -1) items.
        /// </summary>
        protected abstract void ReadPartialFrame();
        public abstract Writers.IDataStreamWriter CreateWriter();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected bool RefreshFrame() {
            int remaining = m_bufferCount - m_bufferIndex;

            if(remaining < FRAME_SIZE + 1) {
                if(remaining != 0)
                    Array.Copy(m_buffer, m_bufferIndex, m_buffer, 0, remaining);

                m_bufferIndex = 0;
                m_bufferCount = m_internal.Read(m_buffer, remaining, this.BUFFER_SIZE - remaining) + remaining;
                remaining = m_bufferCount;
            }

            if(remaining >= FRAME_SIZE + 1) {
                m_frameEndIndex = m_bufferIndex + FRAME_SIZE + 1;
                this.ReadFullFrame();
                m_bufferIndex++;
                return true;
            } else {
                m_frameEndIndex = m_bufferCount; // Math.Min(m_bufferIndex + FRAME_SIZE + 1, m_bufferCount);
                this.ReadPartialFrame();
                m_bufferIndex++;
                return false;
            }
        }
    }
}
