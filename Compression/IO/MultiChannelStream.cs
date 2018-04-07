using System;
using System.IO;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

#region FILE FORMAT
// MULTI-CHANNEL STREAM FORMAT
// -------------------------------------
// The multi-channel stream format is meant to support efficient write without needing to store any cache.
// As such, the data is written WAL-style (write-ahead log).
//
// indexed_data_pointer          uint64 (not var-sized because we want to begin writing immediately)
//                                      (contains 0xFFFFFFFFFFFFFFFF until the index is built)
//                                      (since this is always written last, it is assumed that if this value is set, it means the full index is built.)
// segment_data                  n      (the data, format specified below)
//
// indexed_data format
// -------------------
// index_size                    uint64 (not including itself)
// DataStream_UInt64_LSB (efficient uint64[] encoding)   (could use DFCM for more space savings)
//  -> channel_count             uint64
//  -> segment_count             uint64
//  -> channel_n_id              uint64 (n times) -> segment_count entries
//  -> segment_n_length          uint64 (n times) -> segment_count entries, includes segment_header_size + segment_data_length
//
// segment_data format
// -------------------
// channel_id                    var uint64
// segment_data_length           uint24      (3 bytes LE)  (see MAX_SEGMENT_DATA_ENCODING_SIZE_IN_BYTES)
// data                          n           (of segment_data_length)
#endregion


namespace TimeSeriesDB.IO
{
    using Internal;
    using DataStreams.Readers;
    using DataStreams.Writers;
    using DataStreams.BaseClasses;

    /// <summary>
    ///     An increase-only stream that contains multiple channels (sub-streams).
    ///     The data is written WAL-style (write-ahead log); partially saved channels are recoverable.
    ///     This is meant for fast multi-channel streaming.
    /// </summary>
    /// <remarks>
    ///     To clarify; WAL-logs only work when AutoFlush is on. 
    ///     With AutoFlush off, the data is written similarly to a BufferedStream(~WRITE_CACHE_SIZE) in WAL-style.
    ///     
    ///     This means in the event of a shutdown:
    ///         -> With AutoFlush=true: full recovery
    ///         -> With AutoFlush=false: full recovery up to the last buffer (only applies when writing on end of stream, any other writes are automatically committed)
    /// </remarks>
    public sealed class MultiChannelStream {
        private const int MAX_SEGMENT_DATA_ENCODING_SIZE_IN_BYTES = 3;
        private const int MAX_SEGMENT_DATA_SIZE = (1 << (MAX_SEGMENT_DATA_ENCODING_SIZE_IN_BYTES * 8)) - 1;
        private const int WRITE_CACHE_SIZE = 65536; // 64KB, too big = delayed writes and less recoverable data
        private const int ALLOC_SIZE_ALIGNMENT = 4096;
        private const int MIN_ALLOC_SIZE = 64;
        private const int HEADER_SIZE = 8; // the uint64 pointer preceding the first segment
        private const ulong NO_HEADER = 0xFFFFFFFF_FFFFFFFFul;

        private long m_headerPosition;

        private long m_writePosition = -1;
        private bool m_writeIndexNeeded = false;

        public Stream Stream { get; private set; }
        #region List
        private readonly List<ChannelStream> m_channels = new List<ChannelStream>();
        public IEnumerable<ChannelStream> List => m_channels;
        #endregion
        #region AutoFlush
        public bool AutoFlush { get; set; }
        #endregion

        #region constructors
        static MultiChannelStream() {
            System.Diagnostics.Debug.Assert(WRITE_CACHE_SIZE <= MAX_SEGMENT_DATA_SIZE);
            System.Diagnostics.Debug.Assert(WRITE_CACHE_SIZE >= ALLOC_SIZE_ALIGNMENT);
            System.Diagnostics.Debug.Assert(MIN_ALLOC_SIZE > BitMethods.MAX_VAR_UINT64_ENCODED_LENGTH + MAX_SEGMENT_DATA_ENCODING_SIZE_IN_BYTES);
            System.Diagnostics.Debug.Assert(MIN_ALLOC_SIZE < ALLOC_SIZE_ALIGNMENT);
        }
        private MultiChannelStream(Stream stream) {
            if(stream == null)
                throw new ArgumentNullException(nameof(stream));
            if(!stream.CanSeek)
                throw new ArgumentException("The stream must be able to do seeking.", nameof(stream));

            this.Stream = stream;
            m_headerPosition = stream.Position;
            //m_writePosition = m_headerPosition + HEADER_SIZE;
        }
        #endregion

        #region NewChannel()
        /// <summary>
        ///     Creates a new channel/substream.
        /// </summary>
        public ChannelStream NewChannel() {
            var channel = new ChannelStream(this, m_channels.Count);
            m_channels.Add(channel);
            return channel;
        }
        #endregion
        #region GetOrCreateChannel()
        public ChannelStream GetOrCreateChannel(int channel_id) {
            while(m_channels.Count <= channel_id)
                this.NewChannel();

            return m_channels[channel_id];
        }
        #endregion

        #region Save()
        /// <summary>
        ///     Flushes all channels (sub-streams) and writes the index.
        ///     This leaves the streams usable.
        /// </summary>
        public void Save() {
            for(int i = 0; i < m_channels.Count; i++)
                m_channels[i].Flush();

            if(m_writeIndexNeeded)
                this.WriteIndex();
        }
        #endregion
        #region Close()
        public void Close() {
            for(int i = 0; i < m_channels.Count; i++)
                m_channels[i].Close();

            if(m_writeIndexNeeded)
                this.WriteIndex();

            this.Stream.Close();
        }
        #endregion

        #region static Load()
        public static MultiChannelStream Load(Stream stream) {
            var res = new MultiChannelStream(stream);

            if(!res.TryLoadIndex())
                res.RebuildIndexFromWAL();
            
            return res;
        }
        #endregion
        #region static New()
        public static MultiChannelStream New(Stream stream) {
            var res = new MultiChannelStream(stream);
            res.m_writePosition = res.m_headerPosition + HEADER_SIZE;
            res.WriteHeader(NO_HEADER);
            return res;
        }
        #endregion

        #region private TryLoadIndex()
        /// <summary>
        ///     Attempts to load the index if it was built.
        ///     Returns true if the index was fully loaded.
        ///     Returns false if the index is not fully accessible.
        /// </summary>
        private bool TryLoadIndex() {
            const int BUFFER_SIZE = 4096;
            var buffer = new byte[BUFFER_SIZE];

            // whether this succeeds or not, we have to assume the intent was to start a new multi-channel stream
            m_writeIndexNeeded = false;

            if(this.Stream.Position != m_headerPosition)
                this.Stream.Position = m_headerPosition;

            int read = this.Stream.Read(buffer, 0, HEADER_SIZE);
            if(read != HEADER_SIZE)
                throw new FormatException($"The stream for {this.GetType().Name} is unable to read the header information (pos={m_headerPosition}, expected={HEADER_SIZE}, read={read}).");

            int index = 0;
            var indexed_data_pointer = BitMethods.ReadUInt64(buffer, ref index);
            if(indexed_data_pointer == NO_HEADER)
                return false;

            var index_position = unchecked((long)indexed_data_pointer);

            if(this.Stream.Position != index_position)
                this.Stream.Position = index_position;
            read = this.Stream.Read(buffer, 0, BUFFER_SIZE);
            if(read == 0)
                return false;
            
            index = 0;
            var index_size = BitMethods.ReadUInt64(buffer, ref index);

            // make sure the index_size is fully readable
            if(index > read)
                return false;
            // if the index is not fully stored
            if(this.Stream.Length < unchecked(index_position + (long)index + (long)index_size))
                return false;

            var reader = new DataStreamReader_UInt64_LSB();
            reader.Init(new[] { this.Stream });
            reader.SetBuffer(buffer, index, read); // replace default buffer with this already read part

            const int DATA_BUFFER_SIZE = BUFFER_SIZE / sizeof(ulong);
            System.Diagnostics.Debug.Assert(DATA_BUFFER_SIZE % 2 == 0);

            var data = new ulong[DATA_BUFFER_SIZE];

            if(reader.Read(data, 0, 2) != 2)
                return false;

            m_channels.Clear();

            var channel_count = unchecked((int)data[0]);
            var segment_count = data[1];

            if(channel_count > 0) {
                reader.ItemCount = segment_count * 2; //+ 2

                var channels = new ChannelStream[channel_count];
                for(int i = 0; i < channel_count; i++)
                    channels[i] = new ChannelStream(this, i);

                index = 0;
                read = 0;
                var remaining = segment_count * 2;
                var position = m_headerPosition + HEADER_SIZE;
                while(remaining > 0) {
                    if(index == read) {
                        int request = unchecked((int)Math.Min(remaining, DATA_BUFFER_SIZE));
                        read = reader.Read(data, 0, request);
                        if(read != request)
                            return false;
                        index = 0;
                        remaining -= unchecked((ulong)read);
                    }

                    while(index != read) {
                        var channel_id     = unchecked((int)data[index++]);
                        var segment_length = unchecked((int)data[index++]);

                        var header_size = Segment.CalculateHeaderSize(channel_id);
                        var channel = channels[channel_id];
                        channel.Segments.Add(new Segment(channel_id, header_size, position, segment_length - header_size, channel.CalculateLength()));

                        position += segment_length;
                    }
                }

                for(int i = 0; i < channel_count; i++)
                    channels[i].Init();

                m_writePosition = position;
                m_channels.AddRange(channels);
            }

            return true;
        }
        #endregion
        #region private RebuildIndexFromWAL()
        /// <summary>
        ///     Rebuilds the index based on WAL (Write-Ahead Log) data.
        ///     This rebuilds the index based on the segments found.
        /// </summary>
        private void RebuildIndexFromWAL() {
            const int BUFFER_SIZE = 4096;
            int MAX_POSSIBLE_SEGMENT_HEADER_SIZE = BitMethods.MAX_VAR_UINT64_ENCODED_LENGTH + MAX_SEGMENT_DATA_ENCODING_SIZE_IN_BYTES;

            // whether this succeeds or not, we have to assume the intent was to start a new multi-channel stream
            m_writeIndexNeeded = false;

            var buffer = new byte[BUFFER_SIZE]; // always read the entire page; should be just as fast anyway
            
            int read = 0;
            var position = m_headerPosition + HEADER_SIZE;
            long buffer_position = long.MinValue + MAX_POSSIBLE_SEGMENT_HEADER_SIZE;


            // pre-read the header too on purpose in order to potentially know the stopping point
            if(this.Stream.Position != m_headerPosition)
                this.Stream.Position = m_headerPosition;
            read = this.Stream.Read(buffer, 0, BUFFER_SIZE);
            if(read < HEADER_SIZE) // if can't even read the header, can't read any data following
                return;
            buffer_position = m_headerPosition;

            int temp = 0;
            var indexed_data_pointer = BitMethods.ReadUInt64(buffer, ref temp);
            long stopping_point_position = indexed_data_pointer == NO_HEADER ? long.MaxValue : unchecked((long)indexed_data_pointer);
            

            m_channels.Clear();
            
            while(position < stopping_point_position) {
                // if the segment isn't contained in the same chunk we already read
                // most cases should fit in this case, but if you used AutoFlush=true when saving the stream the code might avoid a number of needless seeks/reads and skip this
                if(buffer_position + read - MAX_POSSIBLE_SEGMENT_HEADER_SIZE >= position) {
                    if(this.Stream.Position != position)
                        this.Stream.Position = position;
                    read = this.Stream.Read(buffer, 0, BUFFER_SIZE);
                    buffer_position = position;
                }
                
                int start_index         = unchecked((int)(position - buffer_position));
                int index               = start_index;
                var channel_id          = unchecked((int)BitMethods.ReadVarUInt64(buffer, ref index));
                var segment_data_length = unchecked((int)BitMethods.ReadUInt32(buffer, ref index, MAX_SEGMENT_DATA_ENCODING_SIZE_IN_BYTES));
                int segment_header_size = index - start_index;

                // make sure the header is fully readable
                if(index < read)
                    break;
            
                // make sure the segment data is also fully available
                if(this.Stream.Length < position + segment_header_size + segment_data_length)
                    break;
            
                var channel = (ChannelStream)this.GetOrCreateChannel(channel_id);
                channel.Segments.Add(new Segment(channel_id, segment_header_size, position, segment_data_length, channel.CalculateLength()));
            
                position += segment_header_size + segment_data_length;
            }
            
            for(int i = 0; i < m_channels.Count; i++)
                m_channels[i].Init();
            
            m_writePosition = position;
        }
        #endregion
        #region private WriteIndex()
        private void WriteIndex() {
            var index_position = m_writePosition;
            int channel_count = m_channels.Count;
            ulong segment_count = 0;
            for(int i = 0; i < channel_count; i++)
                segment_count += unchecked((ulong)m_channels[i].Segments.Count);

            if(this.Stream.Position != index_position + 8)
                this.Stream.Position = index_position + 8;
            var writer = new DataStreamWriter_UInt64_LSB();
            writer.Init(new[] { this.Stream });

            const int DATA_BUFFER_SIZE = 4096 / sizeof(ulong);

            var data = new ulong[DATA_BUFFER_SIZE];

            data[0] = unchecked((ulong)channel_count);
            data[1] = segment_count;
            writer.Write(data, 0, 2);

            using(var segments = this.GetOrderedSegments().GetEnumerator()) {
                ulong remaining = segment_count;
                while(remaining > 0) {
                    int index = 0;
                    var request = unchecked((int)Math.Min(remaining, DATA_BUFFER_SIZE / 2));

                    for(int i = 0; i < request; i++) {
                        var readNextResult = segments.MoveNext();
                        System.Diagnostics.Debug.Assert(readNextResult);

                        var current = segments.Current;

                        data[index++] = unchecked((ulong)current.ChannelID);
                        data[index++] = unchecked((ulong)(current.DataLength + current.HeaderSize));
                    }

                    writer.Write(data, 0, index);

                    remaining -= unchecked((ulong)request);
                }
            }

            writer.Commit();
            writer.Flush();

            // write index_size
            var index_size = unchecked((ulong)(this.Stream.Position - (index_position + 8)));
            var buffer = new byte[8];
            int tempIndex = 0;
            BitMethods.WriteUInt64(buffer, ref tempIndex, index_size);
            if(this.Stream.Position != index_position)
                this.Stream.Position = index_position;
            this.Stream.Write(buffer, 0, 8);

            // once all written, put the pointer to our index to indicate the data is complete
            this.WriteHeader(unchecked((ulong)m_writePosition));

            this.Stream.Flush();

            m_writeIndexNeeded = false;
        }
        #endregion

        #region private GetOrderedSegments()
        /// <summary>
        ///     Returns the segments from all channels (sub-streams).
        /// </summary>
        private IEnumerable<Segment> GetOrderedSegments() {
            // in order to support efficient multi-threading, we don't want to recopy the segments from ChannelStreams to MultiChannelStreams.
            // besides, we know that individual ChannelStreams have their segments stored in order so we can make an efficient merge/sort.

            int remainingChannels = m_channels.Count;
            var streams = new ChannelStreamSegmentEnumerable[remainingChannels];
            var count = streams.Length;
            int i = 0;
            for(; i < count; i++) {
                if(m_channels[i].Segments.Count == 0) {
                    remainingChannels--;
                    continue;
                }

                var segments = m_channels[i].Segments;
                streams[i] = new ChannelStreamSegmentEnumerable {
                    Segments = segments,
                    Current = segments[0],
                    //Index = 0,
                };
            }

            i = -1;
            var position = m_headerPosition + HEADER_SIZE;
            while(true){
                i = (i + 1) % streams.Length;

                var current = streams[i];
                if(current == null)
                    continue;

                // try to optimise cache lines
                // this might seem ridiculous because theoretically you wouldn't get repetitive/consecutive segments for the same stream
                // however, in practice you want forward-only writes for efficiency and that means potentially sub-optimal write strategies (leading to consecutive writes)
                // besides, when writing a lot of data, you wouldn't want to delay the writes indefinitely, as that would be bad for crash recovery
                // as such, there are cases where consecutive segments of the same stream will occur
                while(current.Current.Position == position) {
                    position += current.Current.HeaderSize + current.Current.DataLength;
                    yield return current.Current;

                    if(!current.Next()) {
                        if(--remainingChannels == 0)
                            yield break;
                        streams[i] = null;
                        break;
                    }
                }
            }
        }
        private class ChannelStreamSegmentEnumerable {
            public Segment Current;
            public List<Segment> Segments;
            public int Index;

            public bool Next() {
                if(++this.Index >= this.Segments.Count)
                    return false;
                this.Current = this.Segments[this.Index];
                return true;
            }
        }
        #endregion
        #region private WriteHeader()
        private void WriteHeader(ulong indexed_data_pointer) {
            var buffer = new byte[HEADER_SIZE];
            int index = 0;
            BitMethods.WriteUInt64(buffer, ref index, indexed_data_pointer);

            if(this.Stream.Position != m_headerPosition)
                this.Stream.Position = m_headerPosition;
            this.Stream.Write(buffer, 0, HEADER_SIZE);
        }
        #endregion

        #region private Alloc()
        /// <summary>
        ///     Reserves space.
        ///     Returns the starting index.
        /// </summary>
        private long Alloc(ChannelStream caller, int length) {
            System.Diagnostics.Debug.Assert(length >= MAX_SEGMENT_DATA_ENCODING_SIZE_IN_BYTES + 1); // 1 byte is min possible size for channel id
            System.Diagnostics.Debug.Assert(length <= MAX_SEGMENT_DATA_SIZE);

            if(!m_writeIndexNeeded) {
                m_writeIndexNeeded = true;
                this.WriteHeader(NO_HEADER);
            }

            var res = System.Threading.Interlocked.Add(ref m_writePosition, length);
            caller.WriteSegmentHeader(length);
            return res - length;
        }
        #endregion
        #region private AllocUpTo()
        /// <summary>
        ///     Reserves up to the amount of bytes requested.
        ///     This favors page-alignments to optimized read/writes.
        ///     
        ///     Reserves the full size if AutoFlush is set to true.
        /// </summary>
        private void AllocUpTo(ChannelStream caller, int length, out PositionLength result) {
            System.Diagnostics.Debug.Assert(length >= ALLOC_SIZE_ALIGNMENT);
            System.Diagnostics.Debug.Assert(length <= MAX_SEGMENT_DATA_SIZE);

            if(this.AutoFlush) {
                result = new PositionLength() {
                    Position = this.Alloc(caller, length),
                    Length = length,
                };
                return;
            }

            if(!m_writeIndexNeeded) {
                m_writeIndexNeeded = true;
                this.WriteHeader(NO_HEADER);
            }

            long start;
            int request;

            do {
                start = m_writePosition;

                if((start % ALLOC_SIZE_ALIGNMENT) == 0) {
                    // if address is aligned
                    // round down to boundary
                    
                    // length & ~(ALLOC_SIZE_ALIGNMENT - 1);
                    request = (length / ALLOC_SIZE_ALIGNMENT) * ALLOC_SIZE_ALIGNMENT;
                } else {
                    int remaining = unchecked((int)(ALLOC_SIZE_ALIGNMENT - (start % ALLOC_SIZE_ALIGNMENT)));
                    request = remaining + (((length - remaining) / ALLOC_SIZE_ALIGNMENT) * ALLOC_SIZE_ALIGNMENT);

                    // if the alignments would create a segment that contains no free capacity, 
                    // then don't bother trying to optimize alignments
                    if(request < MIN_ALLOC_SIZE)
                        request = length;
                }
            } while(start != System.Threading.Interlocked.CompareExchange(ref m_writePosition, start + request, start));

            caller.WriteSegmentHeader(request);

            result = new PositionLength() {
                Position = start,
                Length = request,
            };
        }
        private struct PositionLength {
            public long Position;
            public int Length;
        }
        #endregion

        #region DebugDump()
        public string DebugDump() {
            var sb = new StringBuilder();

            const int BUFFER_SIZE = 4096;
            var buffer = new byte[BUFFER_SIZE];
            if(this.Stream.Position != m_headerPosition)
                this.Stream.Position = m_headerPosition;
            int read = this.Stream.Read(buffer, 0, BUFFER_SIZE);
            long indexed_data_pointer = 0;
            if(read >= HEADER_SIZE){
                int temp = 0;
                indexed_data_pointer = unchecked((long)BitMethods.ReadUInt64(buffer, ref temp));
            }

            sb.AppendLine($"multi-channel stream");
            sb.AppendLine($"------------------------");
            foreach(var c in this.List)
                sb.AppendLine(c.DebugDump());
            sb.AppendLine();
            sb.AppendLine();

            sb.AppendLine($"header"); // HEADER_SIZE
            sb.AppendLine($"------------------------");
            sb.AppendLine($"indexed_data_pointer (uint64):   [@{m_headerPosition}] {(m_writeIndexNeeded ? "null" : indexed_data_pointer.ToString())} (this part is not dumped)");
            sb.AppendLine($"segment data         (segments): [detailed further]");
            sb.AppendLine();

            foreach(var c in this.List) {
                sb.AppendLine();
                sb.AppendLine();
                sb.AppendLine(c.DebugDump());
                sb.AppendLine($"------------------------");
                foreach(var s in c.Segments)
                    sb.AppendLine(s.DebugDump(false) + string.Format("   - {0,12}-{1,12}", s.CumulativeIndex, s.CumulativeIndex + s.DataLength));
            }

            sb.AppendLine();
            sb.AppendLine();
            sb.AppendLine($"all channels, ordered segments ({this.GetOrderedSegments().Count()})");
            sb.AppendLine($"------------------------");
            foreach(var s in this.GetOrderedSegments())
                sb.AppendLine(s.DebugDump(true));

            return sb.ToString();
        }
        #endregion
        #region DebugDumpContent()
        public void DebugDumpContent(string directory) {
            if(!Directory.Exists(directory))
                Directory.CreateDirectory(directory);

            foreach(var c in this.List) {
                var filename = Path.Combine(directory, string.Format("channel-{0}.bin", c.ID));
                using(var fs = new FileStream(filename, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)) {
                    c.Position = 0;
                    c.CopyTo(fs);
                }
            }
        }
        #endregion


        /// <summary>
        ///     A channel within MultiChannelStream.
        /// </summary>
        public sealed class ChannelStream : Stream {
            private readonly MultiChannelStream m_owner;
            private readonly Stream m_stream;
            internal readonly List<Segment> Segments;

            private long m_length = 0;
            private long m_position = 0;                 // this may be beyond length, see write()

            private Segment m_current = null;
            private int m_currentIndex = 0;
            private int m_currentRemaining = 0;          // for reading
            private long m_internalStreamPosition = 0;

            // write cache
            private readonly byte[] m_writeCache;
            private readonly int m_writeCacheHeaderSize;
            private int m_writeCacheLength = 0;          // not an index, writing data starts at m_writeCacheHeaderSize

            #region constructors
            public ChannelStream(MultiChannelStream owner, int channel_id) {
                m_owner = owner;
                m_id = channel_id;
                m_stream = owner.Stream;
                this.Segments = new List<Segment>();

                m_writeCache = new byte[WRITE_CACHE_SIZE];
                int index = 0;
                BitMethods.WriteVarUInt64(m_writeCache, ref index, unchecked((ulong)this.ID));
                BitMethods.WriteUInt32(m_writeCache, ref index, 0, MAX_SEGMENT_DATA_ENCODING_SIZE_IN_BYTES);
                m_writeCacheHeaderSize = index;
                m_writeCacheLength = 0;

                // the other values are set in Init()
            }
            #endregion

            #region ID
            private readonly int m_id;
            public int ID => m_id;
            #endregion
            public override bool CanRead => true;
            public override bool CanSeek => true;
            public override bool CanWrite => true;
            public override long Length => m_length;

            public override long Position {
                get => m_position + m_writeCacheLength;
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

                if(m_writeCacheLength > 0)
                    this.Flush();

                var startOffset = offset;
                // clip count to remaining
                count = unchecked((int)Math.Min(count, Math.Max(m_length - m_position, 0)));

                while(count > 0) {
                    int request = Math.Min(count, m_currentRemaining);
                    if(request == 0)
                        break;

                    if(m_stream.Position != m_internalStreamPosition)
                        m_stream.Position = m_internalStreamPosition;

                    int read = m_stream.Read(buffer, offset, request);

                    count -= read;
                    offset += read;
                    m_position += read;
                    m_currentRemaining -= read;
                    m_internalStreamPosition += read;

                    if(m_currentRemaining == 0) {
                        if(m_currentIndex < this.Segments.Count - 1) {
                            var next                 = this.Segments[++m_currentIndex];
                            m_current                = next;
                            m_currentRemaining       = next.DataLength;
                            m_internalStreamPosition = next.Position + m_writeCacheHeaderSize;
                        } else
                            break;
                    }
                }

                return offset - startOffset;
            }
            #endregion
            #region ReadByte()
            public override int ReadByte() {
                if(m_position >= m_length)
                    return -1;

                if(m_writeCacheLength > 0)
                    this.Flush();

                if(m_stream.Position != m_internalStreamPosition)
                    m_stream.Position = m_internalStreamPosition;

                int read = m_stream.ReadByte();

                if(read >= 0) {
                    m_position++;
                    m_internalStreamPosition++;
                    m_currentRemaining--;

                    if(m_currentRemaining == 0 && m_currentIndex < this.Segments.Count - 1) {
                        var next                 = this.Segments[++m_currentIndex];
                        m_current                = next;
                        m_currentRemaining       = next.DataLength;
                        m_internalStreamPosition = next.Position + m_writeCacheHeaderSize;
                    }
                }

                return read; 
            }
            #endregion
            #region Write()
            public override void Write(byte[] buffer, int offset, int count) {
                if(buffer == null)
                    throw new ArgumentNullException(nameof(buffer));
                if(offset < 0)
                    throw new ArgumentOutOfRangeException(nameof(offset));
                if(count < 0)
                    throw new ArgumentOutOfRangeException(nameof(count));
                if(offset + count > buffer.Length)
                    throw new ArgumentException();

                if(m_position < m_length)
                    // 1- if were not at end of stream, write directly to allocated bytes with no caching
                    this.WriteOnPreAllocatedBytes(buffer, ref offset, ref count);
                else if(m_position > m_length)
                    // 2- if the position is past the allocated bytes, then alloc/clear between m_length to this.Position
                    // this may write some of the buffer (on stream or write cache) if it can be done with no side effect
                    this.AllocUpToBufferStart(buffer, ref offset, ref count);

                // 3- all writes here are at end of stream/in write cache
                while(count > 0) {
                    // if buffer can't be filled, then just fill what you can
                    if(count < WRITE_CACHE_SIZE - m_writeCacheHeaderSize - m_writeCacheLength) {
                        Buffer.BlockCopy(buffer, offset, m_writeCache, m_writeCacheHeaderSize + m_writeCacheLength, count);
                        //m_position += count; // do this in Flush()
                        m_length += count;
                        m_writeCacheLength += count;
                        break;
                    }

                    // try to fill the write cache buffer
                    var allocRequest = Math.Min(
                        m_writeCacheLength + m_writeCacheHeaderSize + count,
                        MAX_SEGMENT_DATA_SIZE);

                    m_owner.AllocUpTo(this, allocRequest, out PositionLength alloc);

                    int writeable = Math.Min(alloc.Length, WRITE_CACHE_SIZE) - m_writeCacheLength - m_writeCacheHeaderSize;

                    if(writeable > 0) {
                        Buffer.BlockCopy(buffer, offset, m_writeCache, m_writeCacheHeaderSize + m_writeCacheLength, writeable);
                        offset += writeable;
                        count -= writeable;
                        m_writeCacheLength += writeable;
                    }

                    var segment = this.GenerateSegment(alloc);

                    if(m_stream.Position != alloc.Position)
                        m_stream.Position = alloc.Position;

                    int written = Math.Min(alloc.Length, WRITE_CACHE_SIZE);
                    m_stream.Write(m_writeCache, 0, written);

                    m_writeCacheLength = 0;
                    m_position        += written - m_writeCacheHeaderSize;
                    m_length           = m_position;

                    //// if there was leftover data, then down-shift it
                    //int leftover = m_writeCacheLength - written;
                    //if(leftover <= 0)
                    //    m_writeCacheLength = 0;
                    //else {
                    //    Buffer.BlockCopy(m_writeCache, written, m_writeCache, m_writeCacheHeaderSize, leftover);
                    //    m_writeCacheLength = leftover;
                    //    continue; // is this valid? break? assert(count==0) ?
                    //}

                    // finish writing the allocated bytes
                    int remaining = alloc.Length - written;
                    if(remaining > 0) { // implicit: leftover==0
                        m_stream.Write(buffer, offset, remaining);

                        offset     += remaining;
                        count      -= remaining;
                        m_position += remaining;
                        m_length   += remaining;

                        // whatever remains from the write request add to cache
                        if(count > 0) {
                            Buffer.BlockCopy(buffer, offset, m_writeCache, m_writeCacheHeaderSize, count);
                            m_writeCacheLength = count;
                            m_length += count;
                            count = 0;
                        }
                    }
                    
                    this.Segments.Add(segment);

                    m_current                = segment;
                    m_currentIndex           = this.Segments.Count - 1;
                    m_currentRemaining       = 0;
                    m_internalStreamPosition = segment.Position + segment.DataLength + m_writeCacheHeaderSize;
                }

                if(m_owner.AutoFlush)
                    this.Flush();
            }
            #endregion
            #region WriteByte()
            //public override void WriteByte(byte value) {
            //    base.WriteByte(value);
            //}
            #endregion
            
            #region Seek()
            public override long Seek(long offset, SeekOrigin origin) {
                // note: As per MemoryStream, setting the position past the Length is OK, 
                // as allocation is only done upon Write()

                //if(!this.CanSeek)
                //    throw new NotSupportedException($"The {this.GetType().Name} does not support seeking.");

                switch(origin) {
                    case SeekOrigin.Begin:   break;
                    case SeekOrigin.Current: offset = this.Position + offset; break;
                    case SeekOrigin.End:     offset = m_length + offset; break;
                    default:                 throw new NotImplementedException();
                }

                if(offset == this.Position)
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
                //if(offset == this.Position) // intentionally not enabled
                //    return offset;

                // do not remove this line
                // if you do intend to remove it, be aware that m_position would be off in the code below
                if(m_writeCacheLength > 0)
                    this.Flush();

                Segment current;
                int currentIndex;

                // fast init
                if(offset > m_position) {
                    // if we are already past the buffers, then no need to search for the current segment
                    if(m_currentRemaining == 0) {
                        m_position = offset;
                        return offset;
                    }
                    current = m_current;
                    currentIndex = m_currentIndex;
                } else {
                    current = this.Segments.Count > 0 ? this.Segments[0] : null;
                    currentIndex = 0;
                }

                while(current != null) {
                    // if within current segment
                    if(current.CumulativeIndex + current.DataLength > offset) {
                        m_currentRemaining = unchecked(current.DataLength - (int)(offset - current.CumulativeIndex));
                        m_internalStreamPosition = current.Position + m_writeCacheHeaderSize + offset - current.CumulativeIndex;
                        break;
                    }
                    // if this is the last segment, and we dont fit in it
                    if(currentIndex + 1 == this.Segments.Count) {
                        m_currentRemaining = 0;
                        m_internalStreamPosition = current.Position + m_writeCacheHeaderSize + current.DataLength; // set to end of last segment
                        break;
                    }
                    current = this.Segments[++currentIndex];
                }

                m_current = current;
                m_currentIndex = currentIndex;
                m_position = offset;
                return offset;
            }
            #endregion
            #region SetLength()
            public override void SetLength(long value) {
                if(value < m_length)
                    throw new NotSupportedException($"{this.GetType().Name} does not support downsizing.");
                if(value == m_length)
                    return;

                //if(m_writeCacheLength > 0)
                //    this.Flush(); // handled by Seek() if necessary

                // snapshot
                var pos = m_position;
                var current = m_current;
                var currentIndex = m_currentIndex;
                var currentRemaining = m_currentRemaining;
                var streampos = m_internalStreamPosition;
                var cachelen = m_writeCacheLength;

                var isPositionEndOfStream = this.Position == this.Length;
                if(!isPositionEndOfStream)
                    this.Seek(0, SeekOrigin.End);

                Helper.StreamZero(this, value - m_length);

                // the code does not expect the position to be anywhere else than end of stream and have a write cache
                if(!isPositionEndOfStream)
                    this.Flush();

                // restore
                m_position = pos;
                m_current = current;
                m_currentIndex = currentIndex;
                m_currentRemaining = currentRemaining;
                m_internalStreamPosition = streampos;
                m_writeCacheLength = cachelen;

                m_length = value;
            }
            #endregion

            #region Flush()
            public override void Flush() {
                if(m_writeCacheLength == 0)
                    return;

                var totalWriteLength = m_writeCacheLength + m_writeCacheHeaderSize;
                var pos = m_owner.Alloc(this, totalWriteLength);
                if(m_stream.Position != pos)
                    m_stream.Position = pos;
                m_stream.Write(m_writeCache, 0, totalWriteLength);
                
                var segment = this.GenerateSegment(pos, m_writeCacheLength);
                this.Segments.Add(segment);

                m_position += m_writeCacheLength;
                m_writeCacheLength = 0;

                this.InternalSeek(m_position);
            }
            #endregion
            #region Close()
            //public override void Close() {
            //    base.Close();
            //}
            #endregion
            #region ToString()
            public override string ToString() {
                return string.Format("{0} Pos={1}, Len={2}, Segments={3}", this.GetType().Name, this.Position, this.Length, this.Segments.Count);
            }
            #endregion
            #region ToArray()
            public byte[] ToArray() {
                if(this.Length >= int.MaxValue)
                    throw new NotSupportedException("The Length is too large to support Read() operations on that position.");

                if(m_writeCacheLength > 0)
                    this.Flush();

                var remaining = this.Length;
                var res = new byte[remaining];
                int offset = 0;
                for(int i = 0; i < this.Segments.Count; i++) {
                    var current = this.Segments[i];
                    var request = unchecked((int)Math.Min(current.DataLength, remaining));
                    if(request <= 0)
                        break;

                    var pos = current.Position + current.HeaderSize;
                    if(m_stream.Position != pos)
                        m_stream.Position = pos;

                    int read = m_stream.Read(res, offset, request);

                    offset += read;
                    remaining -= read;
                }
                return res;
            }
            #endregion

            #region internal Init()
            internal void Init() {
                m_length = this.CalculateLength();
                m_position = 0;

                if(this.Segments.Count > 0) {
                    var current = this.Segments[0];
                    m_current = current;
                    m_currentIndex = 0;
                    m_currentRemaining = current.DataLength;
                    m_internalStreamPosition = current.Position + m_writeCacheHeaderSize;
                } else {
                    m_current = null;
                    m_currentIndex = 0;
                    m_currentRemaining = 0;
                    m_internalStreamPosition = 0;
                }
            }
            #endregion
            #region internal CalculateLength()
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal long CalculateLength() {
                var count = this.Segments.Count;
                if(count == 0)
                    return 0;
                var last = this.Segments[count - 1];
                return last.CumulativeIndex + last.DataLength;
            }
            #endregion
            #region internal WriteSegmentHeader()
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void WriteSegmentHeader(int allocLength) {
                int index = m_writeCacheHeaderSize - MAX_SEGMENT_DATA_ENCODING_SIZE_IN_BYTES;
                BitMethods.WriteUInt32(m_writeCache, ref index, unchecked((uint)(allocLength - m_writeCacheHeaderSize)), MAX_SEGMENT_DATA_ENCODING_SIZE_IN_BYTES);
            }
            #endregion
            #region private GenerateSegment()
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private Segment GenerateSegment(PositionLength alloc) {
                return this.GenerateSegment(alloc.Position, alloc.Length - m_writeCacheHeaderSize);
            }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private Segment GenerateSegment(long pos, int dataLength) {
                return new Segment(this.ID, m_writeCacheHeaderSize, pos, dataLength, this.CalculateLength());
            }
            #endregion

            #region private WriteOnPreAllocatedBytes()
            private void WriteOnPreAllocatedBytes(byte[] buffer, ref int offset, ref int count) {
                while(count > 0) {
                    int write = Math.Min(count, m_currentRemaining);

                    if(m_stream.Position != m_internalStreamPosition)
                        m_stream.Position = m_internalStreamPosition;

                    m_stream.Write(buffer, offset, write);

                    count -= write;
                    offset += write;
                    m_position += write;
                    m_currentRemaining -= write;
                    m_internalStreamPosition += write;

                    //if(m_length < m_position)
                    //    m_length = m_position;

                    if(m_currentRemaining == 0) {
                        if(m_currentIndex < this.Segments.Count - 1) {
                            var next = this.Segments[++m_currentIndex];
                            m_current = next;
                            m_currentRemaining = next.DataLength;
                            m_internalStreamPosition = next.Position + m_writeCacheHeaderSize;
                        } else
                            break;
                    }
                }
            }
            #endregion
            #region private AllocUpToBufferStart()
            /// <summary>
            ///     Allocates the stream (with zeroes) until we reach at least the start of the buffer.
            /// </summary>
            private void AllocUpToBufferStart(byte[] buffer, ref int offset, ref int count) {
                while(m_position > m_length && count > 0){ // implicit: m_writeCacheLength==0, m_currentRemaining==0
                    // try to fill the write cache buffer
                    var allocRequest = unchecked((int)Math.Min(
                        m_position - m_length + count + m_writeCacheHeaderSize, 
                        MAX_SEGMENT_DATA_SIZE));

                    // if we could put everything in the write cache and it would not be filled
                    if(allocRequest < WRITE_CACHE_SIZE) {
                        Array.Clear(m_writeCache, m_writeCacheHeaderSize, unchecked((int)(m_position - m_length)));
                        Buffer.BlockCopy(buffer, offset, m_writeCache, unchecked(m_writeCacheHeaderSize + (int)(m_position - m_length)), count);
                        m_writeCacheLength = allocRequest - m_writeCacheHeaderSize;
                        m_length = m_position + m_writeCacheLength; // this.Position;

                        offset += count;
                        count = 0;
                        return;
                    }

                    m_owner.AllocUpTo(this, allocRequest, out PositionLength alloc);

                    int zeroes = unchecked((int)Math.Min(
                        m_position - m_length,
                        Math.Min(alloc.Length, WRITE_CACHE_SIZE) - m_writeCacheHeaderSize));

                    Array.Clear(m_writeCache, m_writeCacheHeaderSize, zeroes);
                    int writeable = Math.Min(alloc.Length, WRITE_CACHE_SIZE) - m_writeCacheHeaderSize - zeroes;

                    if(writeable > 0) {
                        Buffer.BlockCopy(buffer, offset, m_writeCache, m_writeCacheHeaderSize + zeroes, writeable);
                        offset += writeable;
                        count -= writeable;
                    }
                    //m_writeCacheLength = alloc.Length - m_writeCacheHeaderSize;

                    int zeroesAfterWriteCache = unchecked((int)Math.Min(m_position - m_length, alloc.Length)) - WRITE_CACHE_SIZE - m_writeCacheHeaderSize;
                    int writeableAfterWriteCache = alloc.Length - WRITE_CACHE_SIZE - zeroesAfterWriteCache;

                    m_length += alloc.Length - m_writeCacheHeaderSize;
                    if(m_position < m_length)
                        m_position = m_length;

                    var segment = this.GenerateSegment(alloc);

                    if(m_stream.Position != alloc.Position)
                        m_stream.Position = alloc.Position;

                    m_stream.Write(m_writeCache, 0, Math.Min(alloc.Length, WRITE_CACHE_SIZE));

                    // finish writing the allocated bytes
                    int remaining = alloc.Length - WRITE_CACHE_SIZE;
                    if(remaining > 0) {
                        // use stream.Write() over this.SetLength() because SetLength() might not work great with multi-threading
                        Helper.StreamZero(this, zeroesAfterWriteCache);

                        // write also the buffer until allocated space runs out
                        if(writeableAfterWriteCache > 0) {
                            m_stream.Write(buffer, offset, writeableAfterWriteCache);
                            offset += writeableAfterWriteCache;
                            count -= writeableAfterWriteCache;
                        }
                    }

                    this.Segments.Add(segment);

                    m_current                = segment;
                    m_currentIndex           = this.Segments.Count - 1;
                    m_currentRemaining       = 0;
                    m_internalStreamPosition = segment.Position + m_writeCacheHeaderSize + segment.DataLength;
                }
            }
            #endregion

            #region DebugDump()
            public string DebugDump() {
                return string.Format("[{0}] channel: {1}x segments, pos={2}, len={3}", this.ID, this.Segments.Count, this.Position, this.Length);
            }
            #endregion
        }

        #region internal class Segment
        internal sealed class Segment {
            public readonly int ChannelID;
            /// <summary>
            ///     The WAL (write-ahead log) header.
            /// </summary>
            public readonly int HeaderSize;
            /// <summary>
            ///     The position of the header.
            /// </summary>
            public readonly long Position;
            /// <summary>
            ///     The total data length excluding the header.
            /// </summary>
            public readonly int DataLength;
            public readonly long CumulativeIndex;

            #region constructors
            public Segment(int channel_id, int headerSize, long position, int dataLength, long cumulativeIndex) {
                if(dataLength > MAX_SEGMENT_DATA_SIZE)
                    throw new ArgumentOutOfRangeException(nameof(dataLength));

                this.ChannelID = channel_id;
                this.HeaderSize = headerSize;
                this.Position = position;
                this.DataLength = dataLength;
                this.CumulativeIndex = cumulativeIndex;
            }
            #endregion
            
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static int CalculateHeaderSize(int channel_id) {
                return BitMethods.CalculateVarInt64Length((ulong)channel_id) + MAX_SEGMENT_DATA_ENCODING_SIZE_IN_BYTES;
            }

            #region ToString()
            public override string ToString() {
                return $"[{this.ChannelID}] <@{this.Position}+{this.HeaderSize}> {this.DataLength}";
            }
            #endregion

            #region DebugDump()
            public string DebugDump(bool includeID) {
                return string.Format("{3}@{0,12}+{1} {2,12}", this.Position, this.HeaderSize, this.DataLength, includeID ? $"[{this.ChannelID}] " : null);
            }
            #endregion
        }
        #endregion
    }
}
