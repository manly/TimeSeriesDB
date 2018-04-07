using System;
using System.IO;
using System.Runtime.CompilerServices;


namespace TimeSeriesDB.IO
{
    using DataStreams.BaseClasses;

    /// <summary>
    ///     Represents a collection of sub-streams (stream segments).
    ///     A stream within a stream.
    ///     The stream cannot write past Capacity (aka no resizing).
    /// </summary>
    public sealed class SegmentedStream : Stream {
        private const IncreaseBehavior DEFAULT_INCREASE_BEHAVIOR = IncreaseBehavior.WriteZeroes;

        private long m_position;
        private long m_length;           // always <= capacity

        private Segment m_current;
        private long m_currentRemaining; // m_current.Length - m_currentIndex

        private long m_internalStreamPosition;

        public readonly Stream InternalStream;
        #region FirstSegment
        private Segment m_firstSegment;
        /// <summary>
        ///     Linked-list of sub segments which forms the Capacity.
        /// </summary>
        public Segment FirstSegment {
            get {
                return m_firstSegment;
            }
            set {
                m_firstSegment = value ?? throw new ArgumentNullException(nameof(value));
                // automatically sets the entire linked list
                value.Owner = this;
                this.RecalculateSegmentsAndCapacity(value);
            }
        }
        #endregion

        #region constructors
        public SegmentedStream(Stream stream, Segment capacity) {
            if(stream == null)
                throw new ArgumentNullException(nameof(stream));
            if(!stream.CanSeek)
                throw new ArgumentException($"The stream must support seeking in order to be used in {this.GetType().Name}.", nameof(stream));
            
            this.InternalStream      = stream;
            m_current                = capacity;
            m_position               = 0;
            m_length                 = 0;
            this.Capacity            = capacity.Length;
            m_currentRemaining       = capacity.Length;
            m_internalStreamPosition = capacity.Position;
            capacity.CumulativeIndex = 0;
            this.FirstSegment        = capacity;

            this.SetLengthToCapacity(IncreaseBehavior.KeepData);
        }
        public SegmentedStream(Stream stream, long capacity_segment_index, long capacity_segment_length) : this(stream, new Segment(capacity_segment_index, capacity_segment_length)) {
        }
        #endregion

        public override bool CanRead => this.InternalStream.CanRead;
        public override bool CanSeek => true;
        public override bool CanWrite => this.InternalStream.CanWrite;
        public override long Length => m_length;

        /// <summary>
        ///     Represents the sum(Segments.Length).
        ///     The capacity will not increase without adding new segments or changing their .Length.
        /// </summary>
        public long Capacity { get; private set; }

        public override long Position {
            get => m_position;
            set => this.Seek(value, SeekOrigin.Begin);
        }


        #region Read()
        public override int Read(byte[] buffer, int offset, int count) {
            if(buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if(offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if(count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if(offset + count > buffer.Length)
                throw new ArgumentException();
            if(!this.CanRead)
                throw new NotSupportedException($"The {this.GetType().Name} does not support reading.");

            var startOffset = offset;
            // clip count to remaining
            count = unchecked((int)Math.Min(count, Math.Max(m_length - m_position, 0)));

            while(count > 0) {
                int request = unchecked((int)Math.Min(count, m_currentRemaining));
                if(request == 0)
                    break;

                if(this.InternalStream.Position != m_internalStreamPosition)
                    this.InternalStream.Position = m_internalStreamPosition;

                int read = this.InternalStream.Read(buffer, offset, request);

                count -= read;
                offset += read;
                m_position += read;
                m_currentRemaining -= read;
                m_internalStreamPosition += read;

                if(m_currentRemaining == 0) {
                    if(m_current.Next != null) {
                        var next                 = m_current.Next;
                        m_current                = next;
                        m_currentRemaining       = next.Length;
                        m_internalStreamPosition = next.Position;
                    } else
                        break;
                }
            }

            return offset - startOffset;
        }
        #endregion
        #region ReadByte()
        public override int ReadByte() {
            if(!this.CanRead)
                throw new NotSupportedException($"The {this.GetType().Name} does not support reading.");

            if(m_position >= m_length)
                return -1;

            if(this.InternalStream.Position != m_internalStreamPosition)
                this.InternalStream.Position = m_internalStreamPosition;

            int read = this.InternalStream.ReadByte();

            if(read >= 0) {
                m_position++;
                m_internalStreamPosition++;
                m_currentRemaining--;

                if(m_currentRemaining == 0 && m_current.Next != null) {
                    var next                 = m_current.Next;
                    m_current                = next;
                    m_currentRemaining       = next.Length;
                    m_internalStreamPosition = next.Position;
                }
            }

            return read; 
        }
        #endregion
        #region Write()
        /// <summary>
        ///     Writes the data.
        ///     This will not expand the stream past the Capacity.
        /// </summary>
        public override void Write(byte[] buffer, int offset, int count) {
            if(buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if(offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if(count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if(offset + count > buffer.Length)
                throw new ArgumentException();
            if(!this.CanWrite)
                throw new NotSupportedException($"The {this.GetType().Name} does not support writing.");

            // if we can't expand the stream to fit everything, we must throw before we even attempt to write anything
            if(m_position + count > this.Capacity)
                throw new EndOfStreamException();

            // if we were past the buffers, then clear between m_length to m_position
            if(m_position > m_length)
                this.InternalSetLength(m_position + count, count, DEFAULT_INCREASE_BEHAVIOR);
            
            while(count > 0) {
                int write = count <= m_currentRemaining ?
                    count :
                    unchecked((int)Math.Min(m_currentRemaining, int.MaxValue));

                if(this.InternalStream.Position != m_internalStreamPosition)
                    this.InternalStream.Position = m_internalStreamPosition;

                this.InternalStream.Write(buffer, offset, write);

                count -= write;
                offset += write;
                m_position += write;
                m_currentRemaining -= write;
                m_internalStreamPosition += write;

                if(m_length < m_position)
                    m_length = m_position;

                if(m_currentRemaining == 0) {
                    if(m_current.Next != null) {
                        var next                 = m_current.Next;
                        m_current                = next;
                        m_currentRemaining       = next.Length;
                        m_internalStreamPosition = next.Position;
                    } else
                        break;
                }
            }
        }
        #endregion
        #region WriteByte()
        /// <summary>
        ///     Writes the data.
        ///     This will not expand the stream past the Capacity.
        /// </summary>
        public override void WriteByte(byte value) {
            if(!this.CanWrite)
                throw new NotSupportedException($"The {this.GetType().Name} does not support writing.");

            // if we can't expand the stream to fit everything, we must throw before we even attempt to write anything
            if(m_position + 1 > this.Capacity)
                throw new EndOfStreamException();

            // if we were past the buffers, then clear between m_length to m_position
            if(m_position > m_length)
                this.InternalSetLength(m_position + 1, 1, DEFAULT_INCREASE_BEHAVIOR);

            if(this.InternalStream.Position != m_internalStreamPosition)
                this.InternalStream.Position = m_internalStreamPosition;

            this.InternalStream.WriteByte(value);

            m_position++;
            m_currentRemaining--;
            m_internalStreamPosition++;

            if(m_length < m_position)
                m_length = m_position;

            if(m_currentRemaining == 0 && m_current.Next != null) {
                var next                 = m_current.Next;
                m_current                = next;
                m_currentRemaining       = next.Length;
                m_internalStreamPosition = next.Position;
            }
        }
        #endregion

        #region Seek()
        public override long Seek(long offset, SeekOrigin origin) {
            // note: As per MemoryStream, setting the position past the Length is OK, 
            // as allocation is only done upon Write()

            //if(!this.CanSeek)
            //    throw new NotSupportedException($"The {this.GetType().Name} does not support seeking.");

            switch(origin) {
                case SeekOrigin.Begin:   break;
                case SeekOrigin.Current: offset = m_position + offset; break;
                case SeekOrigin.End:     offset = m_length + offset;   break;
                default:                 throw new NotImplementedException();
            }

            if(offset == m_position)
                return offset;
            if(offset < 0)
                throw new IOException();

            return this.InternalSeek(offset);
        }
        private long InternalSeek(long offset) {
            // note: As per MemoryStream, setting the position past the Length is OK, 
            // as allocation is only done upon Write()

            //if(!this.CanSeek)
            //    throw new NotSupportedException($"The {this.GetType().Name} does not support seeking.");
            //if(offset == m_position) // intentionally not enabled
            //    return offset;

            var current = this.FirstSegment;

            // fast init
            if(offset > m_position) {
                // if we are already past the buffers, then no need to search for the current segment
                if(m_currentRemaining == 0) {
                    m_position = offset;
                    return offset;
                }
                current = m_current;
            }

            do {
                long start = current.CumulativeIndex;

                // if within current segment
                if(start + current.Length > offset) {
                    long pos = offset - start;
                    // if we were clipping to m_length
                    //long pos = Math.Min(offset - start, Math.Min(m_length - start, 0));

                    m_currentRemaining       = current.Length - pos;
                    m_internalStreamPosition = current.Position + pos;
                    break;
                }
                // if this is the last segment, and we dont fit in it
                if(current.Next == null) {
                    m_currentRemaining       = 0;
                    m_internalStreamPosition = current.Position + current.Length; // set to end of last segment
                    break;
                }
                current = current.Next;
            } while(current != null);

            m_current = current;
            m_position = offset;
            return offset;
        }
        #endregion

        #region SetLength()
        /// <summary>
        ///     Set the stream to a given length.
        ///     Be aware that we cannot expand past Capacity.
        /// </summary>
        public override void SetLength(long value) {
            if(value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), $"Cannot set the {this.GetType().Name} Length ({value}) to a negative value.");
            
            this.InternalSetLength(value, 0, DEFAULT_INCREASE_BEHAVIOR);
        }
        /// <summary>
        ///     Set the stream to a given length.
        ///     Be aware that we cannot expand past Capacity.
        /// </summary>
        public void SetLength(long value, IncreaseBehavior behavior = DEFAULT_INCREASE_BEHAVIOR) {
            if(value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), $"Cannot set the {this.GetType().Name} Length ({value}) to a negative value.");

            this.InternalSetLength(value, 0, behavior);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InternalSetLength(long value, int dontClearLastBytes, IncreaseBehavior behavior) {
            if(m_length == value)
                return;
            if(value > this.Capacity)
                throw new EndOfStreamException($"Cannot expand the {this.GetType().Name} Length from ({m_length}) to ({value}) because the {nameof(this.Capacity)} = {this.Capacity}. Consider adding a new segment beforehands.");

            if(behavior == IncreaseBehavior.WriteZeroes && m_length < value) {
                long count = value - m_length - dontClearLastBytes;
                if(count > 0)
                    this.ClearBytes(m_length, count);
            }

            m_length = value;
        }
        #endregion
        #region SetLengthToCapacity()
        public void SetLengthToCapacity(IncreaseBehavior behavior = IncreaseBehavior.KeepData) {
            this.InternalSetLength(this.Capacity, 0, behavior);
        }
        #endregion

        #region ClearBytes()
        /// <summary>
        ///     Clears bytes in the stream within a given range.
        ///     This allows writing past .Length but not past the .Capacity.
        ///     This will not touch .Position or .Length.
        /// </summary>
        public void ClearBytes(long offset, long count) {
            if(offset < 0 || offset >= this.Capacity)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if(count < 0 || count >= this.Capacity)
                throw new ArgumentOutOfRangeException(nameof(count));
            if(offset + count > this.Capacity)
                throw new ArgumentException();

            // backup state
            var tempLength                 = m_length;
            var tempCurrent                = m_current;
            var tempPosition               = m_position;
            var tempCurrentRemaining       = m_currentRemaining;
            var tempInternalStreamPosition = m_internalStreamPosition;

            this.Position = offset;

            Helper.StreamZero(this, count);

            // restore state
            m_length                 = tempLength;
            m_current                = tempCurrent;
            m_position               = tempPosition;
            m_currentRemaining       = tempCurrentRemaining;
            m_internalStreamPosition = tempInternalStreamPosition;
        }
        #endregion

        #region AddCapacity()
        /// <summary>
        ///     Appends capacity at the end of the list of segments.
        /// </summary>
        public void AddCapacity(Segment capacity) {
            var last = m_current;
            while(last.Next != null)
                last = last.Next;

            last.Next = capacity ?? throw new ArgumentException(nameof(capacity));
        }
        #endregion
        #region InsertCapacityAfter()
        /// <summary>
        ///     Inserts capacity after the given segment.
        ///     Set the segment to null to set as root.
        /// </summary>
        public void InsertCapacityAfter(Segment capacity, Segment insertCapacityAfterSegment) {
            if(capacity == null)
                throw new ArgumentNullException(nameof(capacity));

            var last = capacity;
            while(last.Next != null)
                last = last.Next;

            if(insertCapacityAfterSegment != null) {
                if(insertCapacityAfterSegment.Owner != this)
                    throw new ArgumentException(nameof(insertCapacityAfterSegment));

                var temp = insertCapacityAfterSegment.Next;
                insertCapacityAfterSegment.Next = capacity;
                last.Next = temp;
            } else {
                var first = this.FirstSegment;
                this.FirstSegment = capacity;
                last.Next = first;
            }
        }
        #endregion
        #region RemoveCapacity()
        public void RemoveCapacity(Segment capacity) {
            if(capacity == null)
                throw new ArgumentNullException(nameof(capacity));
            if(capacity.Owner != this)
                throw new ArgumentException(nameof(capacity));
            
            capacity.Remove();
        }
        #endregion

        #region Flush()
        /// <summary>
        ///     Does not forward the flush to the InternalStream.
        /// </summary>
        public override void Flush() {
            // do not forward the flush
            //this.InternalStream.Flush();
        }
        #endregion
        #region Close()
        public override void Close() {
            base.Close();
            this.InternalStream.Close();
        }
        #endregion
        #region ToString()
        public override string ToString() {
            return string.Format("{0} Pos={1}, Len={2}", this.GetType().Name, this.Position.ToString(System.Globalization.CultureInfo.InvariantCulture), this.Length.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }
        #endregion
        #region ToArray()
        public byte[] ToArray() {
            if(this.Length >= int.MaxValue)
                throw new NotSupportedException("The Length is too large to support Read() operations on that position.");

            int remaining = unchecked((int)this.Length);
            var res = new byte[remaining];
            int index = 0;

            var current = m_firstSegment;

            while(current != null) {
                if(this.InternalStream.Position != current.Position)
                    this.InternalStream.Position = current.Position;

                int request = unchecked((int)Math.Min(Math.Min(current.Length, remaining), int.MaxValue));
                int read = this.InternalStream.Read(res, index, request);

                index += read;
                remaining -= read;

                current = current.Next;
            }
            return res;
        }
        #endregion

        #region private RecalculateSegmentsAndCapacity()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void RecalculateSegmentsAndCapacity(Segment start) {
            var current = start;
            long length = start.CumulativeIndex + current.Length;

            while(current.Next != null) {
                current = current.Next;
                current.CumulativeIndex = length;
                length += current.Length;
            }

            this.Capacity = length;
            if(m_length > length)
                m_length = length;

            // recalc m_current and m_currentRemaining and internalstreampos
            this.InternalSeek(m_position);
        }
        #endregion


        /// <summary>
        ///     Represent a block within a stream.
        ///     This is not part of the Owner class .Length, but only its Capacity.
        /// </summary>
        public sealed class Segment {
            #region Owner
            private SegmentedStream m_owner;
            public SegmentedStream Owner {
                get {
                    return m_owner;
                }
                internal set {
                    //m_owner = value;

                    var current = this;
                    do {
                        current.m_owner = value;
                        current = current.Next;
                    } while(current != null);
                }
            }
            #endregion
            internal long CumulativeIndex;
            public readonly long Position;
            #region Length
            internal long m_length;
            public long Length {
                get {
                    return m_length;
                }
                set {
                    if(value <= 0)
                        throw new ArgumentOutOfRangeException(nameof(value));

                    m_length = value;

                    if(this.Owner != null)
                        this.Owner.RecalculateSegmentsAndCapacity(this);
                }
            }
            #endregion
            #region Next
            private Segment m_next;
            public Segment Next {
                get {
                    return m_next;
                }
                set {
                    if(value != null) {
                        // check if the new segment is consecutive to the current one, if yes, merge
                        if(this.Position + this.Length == value.Position) {
                            this.Length += value.Length; // force recalculate
                            return;
                        }

                        // sets recursively to all linked items
                        value.Owner = this.Owner;
                    }

                    m_next = value;

                    if(this.Owner != null) {
                        value.CumulativeIndex = this.CumulativeIndex + this.Length;
                        this.Owner.RecalculateSegmentsAndCapacity(value);
                    }
                }
            }
            #endregion

            #region constructors
            public Segment(long position, long length) {
                if(position < 0)
                    throw new ArgumentOutOfRangeException(nameof(position));
                this.Position = position;
                this.Length = length;
            }
            #endregion

            #region Remove()
            /// <summary>
            ///     Removes the current segment.
            ///     This will link the previous with the next segment together.
            /// </summary>
            public void Remove() {
                if(this.Owner == null) {
                    m_next = null;
                } else if(this.Owner.FirstSegment == this) {
                    if(this.Next == null)
                        throw new InvalidOperationException($"Cannot remove the only segment on a {this.Owner.GetType().Name}");
                    this.Owner.FirstSegment = this.Next;
                    m_next = null;
                } else {
                    var prev = this.Owner.FirstSegment;
                    while(prev.Next != this)
                        prev = prev.Next;
                    prev.Next = this.Next;
                    m_next = null;
                }
            }
            #endregion

            #region ToString()
            public override string ToString() {
                return string.Format("[@{0}] {1} b", this.Position, this.Length);
            }
            #endregion
        }

        public enum IncreaseBehavior {
            WriteZeroes,
            KeepData,
        }
    }
}
