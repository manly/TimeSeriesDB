using System;
using System.IO;
using System.Text;
using System.Linq;
using System.Collections.Generic;

#region PAGE FORMAT
// PAGE FORMAT
// -----------------------------
// magic_signature    uint32                (TSDB)
// serie_unique_id    uint32                (for fast page filtering -- assumes that all serie with the same id will have identical serie definition data)
//                                          (this is intended in case your code keeps the list of all series emitted, allowing quick filtering)
// page_version       uint8                 (always 0)
// row_count          uint64                (not variable sized in order to allow writing data before finalizing the page)
//                                          contains ulong.MaxValue until the Page is finalized
// serie              string encoding       (optional UTF-8 encoded string)
//                                          see SerieDefinition format.
//                                          see BitMethods.EncodeString() for encoding; uses BitMethods.WriteVarUInt64() followed by UTF-8 raw data
//                                                                                      encoded_utf8_length uses 0 for null, 1 for string.Empty, otherwise string.Length - 1
//                                          ex: "colevel room=42 building=2 floor=1 wing=East sensor=A\nTimeStamp DateTime,64,Delta\nValue Double,64,DFCM"
// raw data           multi-channel stream  (see multi-channel stream format)
//                                          the multi-channel stream format allows storing multiple streams together efficiently, assigning a simple ChannelID for each sub-stream
//                                          The channel 0 is reserved for statistics, and its sizing is pre-reserved in order to make sure the first segment will
//                                          always contain contiguous statistics data. This allows reading statistics without having to parse the multi-channel stream indices.
//                                          The columns ChannelID(s) follows a simple rule of ID assignment as follows;
//
//                                          ex: serie="FileName string,,\nContent byte[],,"
//                                          [FileName column 0].ChannelCount = 2
//                                              -> ChannelID = 1
//                                              -> ChannelID = 2
//                                          [Content column 1].ChannelCount = 2
//                                              -> ChannelID = 3
//                                              -> ChannelID = 4
#endregion


namespace TimeSeriesDB
{
    using Internal;
    using IO;

    /// <summary>
    ///     Represent a collection of rows (stored in columnar format for efficiency).
    /// </summary>
    public sealed class Page : IDisposable {
        public const uint MAGIC_SIGNATURE             = 0x42445354; // TSDB
        public const byte PAGE_VERSION                = 0;
        public const int RESERVED_CHANNELS            = 1;
        private const int STATISTICS_CHANNEL_ID       = 0;
        private const ulong UNFINALIZED_PAGE_ROWCOUNT = ulong.MaxValue;
        private const int ROWCOUNT_OFFSET             = 9; // position of row_count in header

        public DataSerieDefinition SerieDefinition { get; private set; }
        public MultiChannelStream Channels { get; private set; }

        public ulong RowCount { get; set; }
        public KeyOrIndexCollection<string, Column> Columns { get; private set; }
        public PageMode Mode { get; private set; }

        /// <summary>
        ///     The position of the Page header within Channels.Stream.
        /// </summary>
        public long HeaderPosition { get; private set; }

        #region constructors
        private Page(PageMode mode) {
            this.Mode = mode;
            this.HeaderPosition = -1;
        }
        #endregion

        #region static CreateNew()
        /// <summary>
        ///     Create a new empty Page in write access.
        /// </summary>
        public static Page CreateNew(DataSerieDefinition serie, string file_path, FileMode fileMode = FileMode.Create) {
            return CreateNew(serie, new FileStream(file_path, fileMode, FileAccess.Write, FileShare.ReadWrite));
        }
        /// <summary>
        ///     Create a new empty Page in write access.
        /// </summary>
        public static Page CreateNew(DataSerieDefinition serie, Stream destination) {
            if(destination == null)
                 throw new ArgumentNullException(nameof(destination));

            var header_position = destination.Position;

            // put an unfinalized header signalling the page is being built up
            SaveHeader(serie, UNFINALIZED_PAGE_ROWCOUNT, destination);

            // multichannel stream needs to be initialized at the proper position
            var channels = MultiChannelStream.New(destination);
            var res = new Page(PageMode.Write) {
                Channels        = channels,
                SerieDefinition = serie,
                HeaderPosition  = header_position,
            };

            int channel_id   = RESERVED_CHANNELS;
            int column_count = serie.Columns.Length;
            res.Columns      = new KeyOrIndexCollection<string, Column>(column_count);

            for(int i = 0; i < column_count; i++) {
                var column = new Column(res.SerieDefinition.Columns[i]);
                
                column.Create(channels, channel_id);
                channel_id += column.WriteOnly.ChannelCount;

                res.Columns.Add(column.Definition.Name, column);
            }
            
            return res;
        }
        #endregion
        #region static Load()
        public static Page Load(string file_path, FileMode fileMode = FileMode.Open) {
            return Load(new FileStream(file_path, fileMode, FileAccess.Read, FileShare.ReadWrite));
        }
        public static Page Load(Stream stream) {
            var res = new Page(PageMode.Read);

            res.LoadHeader(stream);

            // stream is positioned at the proper position at this point
            res.Channels = MultiChannelStream.Load(stream);

            int column_count = res.SerieDefinition.Columns.Length;
            int channel_id   = RESERVED_CHANNELS;
            res.Columns      = new KeyOrIndexCollection<string, Column>(column_count);

            for(int i = 0; i < column_count; i++) {
                var column = new Column(res.SerieDefinition.Columns[i]);
                
                column.Load(res.Channels, channel_id, res.RowCount);
                channel_id += column.ReadOnly.ChannelCount;

                res.Columns.Add(column.Definition.Name, column);
            }

            //res.LoadStatistics();

            return res;
        }
        #endregion

        #region Save()
        public void Save() {
            if(this.Mode != PageMode.Write)
                return;

            this.WriteStatistics();

            int column_count = this.Columns.Count;
            for(int i = 0; i < column_count; i++) {
                var writer = this.Columns[i].WriteOnly;
                writer.Commit();
                writer.Flush();
            }
            
            this.Channels.Save();

            // at the very end, to finalize the page, we write the rowcount

            // re-saves the entire header to store the rowcount
            //SaveHeader(this.SerieDefinition, this.RowCount, this.Channels.Stream);
            this.SaveRowCount();

            this.Channels.Stream.Flush();
        }
        #endregion
        #region Close()
        public void Close() {
            if(this.Mode != PageMode.Write)
                return;
            
            // if this is enabled, likely to double save since caller might so save()/close()
            // make sure to prevent that double save
            //this.Save();

            this.Channels.Close();
            // done by this.Channels.Close()
            //this.Channels.Stream.Close();

            // not writing to those so that the optimizer will not think those object are not read-only
            //this.Channels = null;
            //this.Columns = null;
            //this.SerieDefinition = null;
        }
        #endregion
        #region Reset()
        /// <summary>
        ///     Resets the page into an empty page with zero RowCount.
        ///     Makes the data stream into a write-only stream.
        ///     Will not touch the stream position.
        /// </summary>
        public void Reset() {
            if(this.Mode != PageMode.Write)
                return;

            //todo: this.HeaderPosition = this.Channels.Stream.Position;
            // also fix multichannel headerposition

            this.RowCount = 0;
            //this.Channels.Reset();

            for(int i = 0; i < this.Columns.Count; i++)
                this.Columns[i].Reset();

            //this.Statistics.Reset();
        }
        #endregion

        #region Dispose()
        private bool disposedValue = false;
        private void Dispose(bool disposing) {
            if(!disposedValue) {
                if(disposing)
                    this.Close();
                disposedValue = true;
            }
        }
        public void Dispose() {
            Dispose(true);
        }
        #endregion

        #region internal SaveRowCount()
        /// <summary>
        ///     Updates the RowCount stored on the stream.
        /// </summary>
        internal void SaveRowCount() {
            var buffer = new byte[8];

            int offset = 0;
            BitMethods.WriteUInt64(buffer, ref offset, this.RowCount);

            var write_position = this.HeaderPosition + ROWCOUNT_OFFSET;
            var destination = this.Channels.Stream;
            
            if(destination.Position != write_position)
                destination.Position = write_position;

            destination.Write(buffer, 0, 8);
        }
        #endregion
        #region private WriteStatistics()
        /// <summary>
        ///     Writes the statistics on their channel.
        /// </summary>
        private void WriteStatistics() {
            // intentionally empty for now

            var statistics_channel = this.Channels.List.FirstOrDefault(o => o.ID == STATISTICS_CHANNEL_ID);
        }
        #endregion
        #region private LoadHeader()
        /// <summary>
        ///     Loads everything up to raw data.
        /// </summary>
        private void LoadHeader(Stream stream) {
            const int BUFFER_SIZE = 4096;
            var buffer = new byte[BUFFER_SIZE];
            int offset = 0;

            this.HeaderPosition = stream.Position;

            int read = stream.Read(buffer, 0, BUFFER_SIZE);

            uint magic_signature = BitMethods.ReadUInt32(buffer, ref offset);
            uint serie_unique_id = BitMethods.ReadUInt32(buffer, ref offset);
            byte page_version = buffer[offset++];
            ulong row_count = BitMethods.ReadUInt64(buffer, ref offset);

            if(read <= offset)
                throw new FormatException($"Read Page header could not be read entirely (read {read} bytes, expected {offset}+ bytes).");
            if(magic_signature != MAGIC_SIGNATURE)
                throw new FormatException($"Read Page magic_signature (0x{magic_signature.ToString("X8")}) doesn't match expected value (0x{MAGIC_SIGNATURE.ToString("X8")}).");
            if(page_version != PAGE_VERSION)
                throw new NotSupportedException($"Read Page page_version (0x{page_version.ToString("X2")}) doesn't match expected value (0x{PAGE_VERSION.ToString("X2")}).");
            if(RowCount == UNFINALIZED_PAGE_ROWCOUNT)
                throw new NotSupportedException($"Read Page was never finalized ({this.GetType().Name}.Save() was not called). Row count is unknown. This data is restorable, however you'll need to manually restore it since it is impossible to know how many fragments of column data was stored (on top of potentially compressed channels). Each channel can have it's own different 'item count' due to batched saves (unless {this.GetType().Name}.AutoFlush=true), thus making this hard to support genericly.");
            //if(unchecked((ulong)(stream.Length - streamStartPos)) < page_size)
            //    throw new NotSupportedException($"The Page page_size ({page_size.ToString()}) is bigger than the remaining size on the stream ({stream.Length - streamStartPos}).");

            this.RowCount = row_count;

            // throws if EOS
            this.SerieDefinition = this.LoadSerieFromCache(serie_unique_id, buffer, ref offset, ref read, stream);
        }
        #endregion
        #region private static SaveHeader()
        /// <summary>
        ///     Saves everything up to raw data.
        /// </summary>
        private static void SaveHeader(DataSerieDefinition serie, ulong row_count, Stream destination) {
            // hard requirement due to complex string encoding
            const int BUFFER_SIZE = BitMethods.ENCODESTRING_BUFFER_SIZE;
            var buffer = new byte[BUFFER_SIZE];
            int offset = 0;

            BitMethods.WriteUInt32(buffer, ref offset, MAGIC_SIGNATURE);
            BitMethods.WriteUInt32(buffer, ref offset, serie.UniqueID);
            buffer[offset++] = PAGE_VERSION;
            BitMethods.WriteUInt64(buffer, ref offset, row_count);

            var serie_string = serie.ToString(); // cached
            BitMethods.EncodeString(buffer, ref offset, destination, Encoding.UTF8, serie_string);

            if(offset > 0)
                destination.Write(buffer, 0, offset);
        }
        #endregion

        #region private LoadSerieFromCache()
        private DataSerieDefinition LoadSerieFromCache(uint serie_unique_id, byte[] buffer, ref int offset, ref int read, Stream stream) {
            // this code could be sped up by loading the serie from a "return Dict[serie_unique_id]"
            //bool is_cached = true;
            //if(is_cached) {
            //    var cache = dict[serie_unique_id];
            //    // need to adjust the stream position to skip this
            //    BitMethods.SkipVarSizedObject(buffer, ref offset, ref read, stream);
            //    return cache;
            //}

            char[] decompressBuffer = new char[buffer.Length];

            // throws if EOS
            var serie_string = BitMethods.DecodeString(stream, buffer, decompressBuffer, ref offset, ref read, Encoding.UTF8.GetDecoder());

            // need to adjust the stream position to right after the string
            var adjustment = read - offset;
            if(adjustment > 0)
                stream.Seek(-adjustment, SeekOrigin.Current);

            return new DataSerieDefinition(serie_string) {
                UniqueID = serie_unique_id,
            };
        }
        #endregion

        public enum PageMode {
            Read,
            Write,
        }
    }
}
