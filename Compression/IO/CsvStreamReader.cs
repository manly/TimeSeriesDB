using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Linq;
using static System.Runtime.CompilerServices.MethodImplOptions;


namespace TimeSeriesDB.IO
{
    /// <summary>
    ///     Efficient CSV file reader.
    ///     This is significantly faster than StreamReader/StringReader because no convertion takes place and all reads are hand-coded for speed.
    ///     Everything is decoded in UTF-8.
    /// </summary>
    public sealed class CsvStreamReader : IDisposable {
        private static readonly CultureInfo FORMAT = CultureInfo.InvariantCulture;

        private const int BUFFER_SIZE   = 4096;
        private const int CSVVALUE_SIZE = 64;

        private readonly Stream m_stream;
        private byte[] m_buffer  = new byte[BUFFER_SIZE];
        private byte[] m_buffer2 = new byte[BUFFER_SIZE];
        private int m_offset     = 0; // in m_buffer
        private int m_read       = 0; // in m_buffer

        private readonly byte m_bomPreambleLength = 0; // the byte order mark, ie: encoding preamble

        private readonly List<byte[]> m_rowBuffers = new List<byte[]>(); // in order, from oldest to newest

        private readonly Decoder m_decoder; // even though this is detected by BOM preamble, everything assumes UTF-8 and wont detect end of string "..." properly
        private readonly char[] m_charBuffer     = new char[BUFFER_SIZE];
        private readonly byte[] m_csvValueBuffer = new byte[CSVVALUE_SIZE];
        private readonly bool m_closeStreamOnDispose;
        public readonly byte ColumnSeparator     = (byte)',';

        private CsvValue[] m_current;
        public CsvValue[] Current => m_current;
        /// <summary>
        ///     Returns the number of columns in the current row.
        /// </summary>
        public int ColumnCount { get; private set; }

        #region constructors
        static CsvStreamReader() {
            if(CSVVALUE_SIZE < 38)
                throw new ArgumentOutOfRangeException($"{nameof(CSVVALUE_SIZE)} must be >= 38 to fit a full Guid.");
            if(BUFFER_SIZE < CSVVALUE_SIZE)
                throw new ArgumentOutOfRangeException($"{nameof(BUFFER_SIZE)} must be >= {nameof(CSVVALUE_SIZE)}");

            m_rootPreamble = BuildEncodingPreambleTree();
        }
        /// <param name="auto_detect_column_separator">If true, will read the first line of the stream and assume its a header row, and look at the non-alphanumeric characters to detect which re-occurs the most and use that one.</param>
        public CsvStreamReader(Stream stream, bool close_stream_on_dispose = true, bool auto_detect_column_separator = true) {
            m_stream               = stream ?? throw new ArgumentNullException(nameof(stream));
            m_current              = new CsvValue[32];
            m_closeStreamOnDispose = close_stream_on_dispose;

            for(int i = 0; i < m_current.Length; i++)
                m_current[i].Owner = this;

            m_decoder = Encoding.UTF8.GetDecoder();
            if(this.ReadNextBuffer()) {
                var preamble = this.DetectEncodingPreamble();
                if(preamble == null)
                    m_bomPreambleLength = 0;
                else {
                    m_bomPreambleLength = preamble.Length;
                    m_decoder = preamble.Encoding.GetDecoder(); // beware that this only works for strings, as all type interpretations assume UTF-8
                    m_offset += preamble.Length;
                    m_read   -= preamble.Length;
                }

                if(auto_detect_column_separator)
                    this.ColumnSeparator = AutoDetectColumnSeparator(m_buffer, m_offset, m_read);
            }
        }
        /// <param name="auto_detect_column_separator">If true, will read the first line of the stream and assume its a header row, and look at the non-alphanumeric characters to detect which re-occurs the most and use that one.</param>
        public CsvStreamReader(string path, bool close_stream_on_dispose = true, bool auto_detect_column_separator = true) 
            : this(new System.IO.FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite), close_stream_on_dispose, auto_detect_column_separator) {
        }
        #endregion

        #region MoveNext()
        /// <summary>
        ///     Moves to the next row/record.
        ///     Returns false if theres no more rows.
        /// </summary>
        public bool MoveNext() {
            this.ColumnCount = 0;
            bool containsNonWhitespace = false;
            int rowBufferIndex         = 0;
            int bufferIndex            = m_offset;
            int length                 = 0;
            bool isQuoted              = false;
            bool isDoubleQuoted        = false;

            if(m_rowBuffers.Count > 1) {
                m_rowBuffers.Clear();
                m_rowBuffers.Add(m_buffer);
            }

            while(true) {
                // search only within buffer, do not extend past it
                while(m_offset < m_read) {
                    var b = m_buffer[m_offset];

                    if(b == ColumnSeparator) {
                        if(this.ColumnCount == this.Current.Length)
                            this.ResizeColumns();
                        this.AddColumn(rowBufferIndex, bufferIndex, length, isQuoted, isDoubleQuoted);
                        m_offset++;
                        rowBufferIndex        = m_rowBuffers.Count - 1;
                        bufferIndex           = m_offset;
                        length                = 0;
                        containsNonWhitespace = false;
                        isQuoted              = false;
                        isDoubleQuoted        = false;
                        continue;
                    }
                    if(b == '\n' || b == '\r') {
                        // skip empty rows  (ie: \r\n)
                        if(this.ColumnCount == 0 && length == 0) {
                            m_offset++;
                            bufferIndex = m_offset;
                            continue;
                        }

                        if(this.ColumnCount == this.Current.Length)
                            this.ResizeColumns();
                        this.AddColumn(rowBufferIndex, bufferIndex, length, isQuoted, isDoubleQuoted);
                        m_offset++;
                        return true;
                    }

                    if(b == '\'' || b == '"') {
                        // need to handle the ' differently depending on the cases:
                        // "1,O'Clock place,True"
                        // "1,'O''Clock place',True"

                        if(!containsNonWhitespace) {
                            // pre left-trim the quoted content
                            rowBufferIndex = m_rowBuffers.Count - 1;
                            bufferIndex    = m_offset;
                            length         = 1;
                            isQuoted       = true;
                            isDoubleQuoted = true;
                            var is_success = this.ReadDoubleQuotedContent(b, rowBufferIndex, bufferIndex, ref length, out bool contains_escaped_quote);
                            // if the quoted content contains no escaped quote, then treat it as-if there was no quote (ex: "text")
                            if(!contains_escaped_quote && is_success) { // check for success as otherwise reducing the length gets complicated
                                if(++bufferIndex == BUFFER_SIZE && rowBufferIndex < m_rowBuffers.Count - 1) // skip initial quote
                                    rowBufferIndex++;
                                length -= 2; // remove the 2 quotes
                                //isQuoted     = false;
                                isDoubleQuoted = false;
                            }
                            continue;
                        }
                    } else if(b != ' ' && b != '\t') // char.IsWhitespace() is too slow
                        containsNonWhitespace = true;

                    m_offset++;
                    length++;
                }

                // if the buffer is all read
                // implicit: m_offset >= m_read
                if(!this.ReadNextBuffer()) {
                    // end of stream
                    if(this.ColumnCount > 0 || length > 0) {
                        if(this.ColumnCount == this.Current.Length)
                            this.ResizeColumns();
                        this.AddColumn(rowBufferIndex, bufferIndex, length, isQuoted, isDoubleQuoted);
                        return true;
                    } else
                        // dont return empty row with no data
                        return false;
                }
            }
        }
        private bool ReadDoubleQuotedContent(byte quoteChar, int rowBufferIndex, int bufferIndex, ref int length, out bool contains_escaped_quote) {
            m_offset++;
            //length++;

            contains_escaped_quote             = false;
            bool stateLookingForFollowingQuote = false;

            while(true) {
                while(m_offset < m_read) {
                    var b = m_buffer[m_offset];
                    if(b != quoteChar) {
                        if(stateLookingForFollowingQuote)
                            return true;
                    } else {
                        if(!stateLookingForFollowingQuote)
                            stateLookingForFollowingQuote = true;
                        else {
                            stateLookingForFollowingQuote = false;
                            contains_escaped_quote        = true;
                        }
                    }

                    m_offset++;
                    length++;
                }

                // if the buffer is all read
                // implicit: m_offset >= m_read
                if(!this.ReadNextBuffer()) {
                    //// end of stream
                    //if(this.ColumnCount == this.Current.Length)
                    //    this.ResizeColumns();
                    //this.AddColumn(rowBufferIndex, bufferIndex, length, true);
                    return stateLookingForFollowingQuote;
                }
            }
        }
        #endregion

        #region GetRows()
        /// <summary>
        ///     Shortcut to read all rows.
        ///     Call GetColumnNames() first if you wish to try and read the header.
        ///     By default, will return the same row instances.
        /// </summary>
        public IEnumerable<object[]> GetRows(bool returnNewRowsOnly = false) {
            object[][] cache = null;

            //this.GetColumnNames(); // to force skipping the first column if they look like column names

            while(this.MoveNext()) {
                object[] row;

                if(returnNewRowsOnly)
                    row = new object[this.ColumnCount];
                else {
                    if(cache == null || cache.Length < this.ColumnCount + 1) {
                        var old_size = cache?.Length ?? 0;
                        if(cache == null)
                            cache = new object[this.ColumnCount + 1][];
                        else if(cache.Length < this.ColumnCount + 1)
                            Array.Resize(ref cache, this.ColumnCount + 1);
                        for(int i = old_size; i < cache.Length; i++)
                            cache[i] = new object[i];
                    }
                    row = cache[this.ColumnCount];
                }

                for(int i = 0; i < this.ColumnCount; i++)
                    row[i] = this.Current[i].GetValue();

                yield return row;
            }
        }
        #endregion
        #region GetRowsAsStrings()
        /// <summary>
        ///     Shortcut to read all rows as strings.
        ///     Call GetColumnNames() first if you wish to try and read the header.
        ///     By default, will return the same row instances.
        /// </summary>
        public IEnumerable<string[]> GetRowsAsStrings(bool returnNewRowsOnly = false) {
            string[][] cache = null;

            //this.GetColumnNames(); // to force skipping the first column if they look like column names

            while(this.MoveNext()) {
                string[] row;

                if(returnNewRowsOnly)
                    row = new string[this.ColumnCount];
                else {
                    if(cache == null || cache.Length < this.ColumnCount + 1) {
                        var old_size = cache?.Length ?? 0;
                        if(cache == null)
                            cache = new string[this.ColumnCount + 1][];
                        else if(cache.Length < this.ColumnCount + 1)
                            Array.Resize(ref cache, this.ColumnCount + 1);
                        for(int i = old_size; i < cache.Length; i++)
                            cache[i] = new string[i];
                    }
                    row = cache[this.ColumnCount];
                }

                for(int i = 0; i < this.ColumnCount; i++)
                    row[i] = this.Current[i];

                yield return row;
            }
        }
        #endregion
        #region ParseRows()
        /// <summary>
        ///     Parses all the rows into column names matching properties of TEntity.
        ///     This will read and rely on the column header being present to match the properties/field names.
        /// </summary>
        public IEnumerable<TEntity> ParseRows<TEntity>(Func<TEntity> generator) {
            var columnNames = this.GetColumnNames();
            var mappings    = new List<ColumnMapping<TEntity>>();

            if(columnNames != null) {
                var properties = typeof(TEntity).GetProperties().ToDictionary(
                p => p.Name,
                p => new ColumnMapping<TEntity>(){
                    ColumnIndex      = -1,
                    PropertyType     = p.PropertyType,
                    SetUnparsedValue = null,
                    Setter           = (TEntity source, object value) => p.SetValue(source, value),
                });
                var fields = typeof(TEntity).GetFields().ToDictionary(
                p => p.Name,
                p => new ColumnMapping<TEntity>(){
                    ColumnIndex      = -1,
                    PropertyType     = p.FieldType,
                    SetUnparsedValue = null,
                    Setter           = (TEntity source, object value) => p.SetValue(source, value),
                });

                for(int i = 0; i < columnNames.Length; i++) {
                    var h = columnNames[i];
                    if(string.IsNullOrEmpty(h))
                        continue;
                    if(properties.TryGetValue(h, out var map)) {
                        map.ColumnIndex = i;
                        mappings.Add(map);
                    } else if(fields.TryGetValue(h, out map)) {
                        map.ColumnIndex = i;
                        mappings.Add(map);
                    }
                }
            }

            return this.ParseRows(true, mappings, generator);
        }
        public sealed class ColumnMapping<TEntity> {
            public int ColumnIndex;
            public Action<TEntity, object> Setter;
            public Action<TEntity, string> SetUnparsedValue;
            public Type PropertyType;
        }
        /// <summary>
        ///     Parses all the rows into properly interpreted values.
        /// </summary>
        public IEnumerable<TEntity> ParseRows<TEntity>(bool read_header, IEnumerable<ColumnMapping<TEntity>> mappings, Func<TEntity> generator) {
            var mappingsBackup = mappings.ToArray();

            // skip header if applicable
            if(!read_header && this.GetColumnNames() == null)
                yield break;

            while(this.MoveNext()) {
                bool contains_values = false;
                var _new = generator();

                for(int i = 0; i < mappingsBackup.Length; i++) {
                    var mapping = mappingsBackup[i];

                    if(mapping.ColumnIndex >= this.Current.Length) {
                        // notify that the value could not be read
                        mapping.SetUnparsedValue?.Invoke(_new, null);
                        continue;
                    }

                    // only if the value can be properly read that we write it
                    var currentColumn = this.Current[mapping.ColumnIndex];
                    if(currentColumn.TryGetValue(mapping.PropertyType, out var csv_value_in_proper_type)) {
                        mapping.Setter(_new, csv_value_in_proper_type);
                        contains_values = true;
                    } else if(mapping.SetUnparsedValue != null) {
                        // if the value cant be converted
                        if(currentColumn.TryReadString(out var unparsed_value)) // this can't really fail
                            // notify that the value could not be read
                            mapping.SetUnparsedValue(_new, (string)unparsed_value);
                    }
                }

                // this check is mostly to skip the column headers if youre not reading any string
                // it incomplete but makes sense regardless
                if(contains_values)
                    yield return _new;
            }
        }
        #endregion
        #region GetColumnNames()
        /// <summary>
        ///     Returns the column names if they are available, and default (Column1, Column2, ...) names if not.
        ///     Note that following this call, if the header row wasnt column headers, the next MoveNext() will return the first row as expected.
        ///     Returns null if there is no rows.
        /// </summary>
        public string[] GetColumnNames() {
            if(!this.MoveNext())
                // special case: there is no rows within the file, ie: its empty
                return null;

            bool allColumnsHaveValidNames = true;
            var res                       = new string[this.ColumnCount];
            var avoidDuplicateColumnNames = new HashSet<string>();

            for(int i = 0; i < this.ColumnCount; i++) {
                if(!this.Current[i].TryReadString(out res[i]) || !avoidDuplicateColumnNames.Add(res[i]) || !IsValidColumnName(res[i])) {
                    allColumnsHaveValidNames = false;
                    break;
                }
            }

            if(!allColumnsHaveValidNames) {
                for(int i = 0; i < this.ColumnCount; i++)
                    res[i] = GetDefaultColumnName(i);

                this.UndoMoveNext();
            }
            
            return res;

            bool IsValidColumnName(string value) {
                int len = value.Length;
                for(int i = 0; i < len; i++) {
                    var c = value[i];
                    if((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'))
                        return true;
                }
                return false;
            }
            string GetDefaultColumnName(int index) {
                return string.Format("Column{0}", (index + 1).ToString(CultureInfo.InvariantCulture));
            }
        }
        #endregion

        #region private ReadNextBuffer()
        /// <summary>
        ///     Reads the next buffer and updates the rowbuffers accordingly.
        ///     Returns true if a buffer was read.
        /// </summary>
        private bool ReadNextBuffer() {
            if(m_read == BUFFER_SIZE) {
                // 99% of cases
                if(m_rowBuffers.Count == 1) {
                    var swap  = m_buffer;
                    m_buffer  = m_buffer2;
                    m_buffer2 = swap;
                } else // implicit: m_rowBuffers.Count > 1
                    m_buffer = new byte[BUFFER_SIZE];
                m_rowBuffers.Add(m_buffer);
            } else if(m_read == 0 && m_rowBuffers.Count == 0)
                // reading for the first time
                m_rowBuffers.Add(m_buffer);
            else {
                // attempting to re-read data
                m_read   = 0;
                m_offset = 0;
                return false;
            }

            m_read   = m_stream.Read(m_buffer, 0, BUFFER_SIZE);
            m_offset = 0;
            return m_read != 0;
        }
        #endregion
        #region private AddColumn()
        private void AddColumn(int rowBufferIndex, int bufferIndex, int length, bool isQuoted, bool isDoubleQuoted) {
            ref var column        = ref this.Current[this.ColumnCount++];
            column.RowBufferIndex = rowBufferIndex;
            column.BufferIndex    = bufferIndex;
            column.Length         = length;
            column.IsQuoted       = isQuoted;
            column.IsDoubleQuoted = isDoubleQuoted;
        }
        #endregion
        #region private ResizeColumns()
        private void ResizeColumns() {
            var length = this.Current.Length;
            Array.Resize(ref m_current, length * 2);

            for(int i = length; i < m_current.Length; i++)
                m_current[i].Owner = this;
        }
        #endregion
        #region private UndoMoveNext()
        private void UndoMoveNext() {
            if(m_rowBuffers.Count == 1) // implicit: m_buffer == m_rowBuffers[0]
                m_offset = m_current[0].BufferIndex;
            else if(m_rowBuffers.Count > 1) {
                var pos = m_stream.Position -
                    ((m_rowBuffers.Count - 1) * BUFFER_SIZE) -
                    m_read +
                    BUFFER_SIZE;

                m_offset  = m_current[0].BufferIndex;
                m_read    = BUFFER_SIZE;
                m_buffer  = m_rowBuffers[0];
                m_buffer2 = m_rowBuffers[1];

                m_stream.Position = pos;
            }
        }
        #endregion
        #region private DetectEncodingPreamble()
        private PreambleNode DetectEncodingPreamble() {
            var c = m_rootPreamble;

            for(int i = 0; i < m_read; i++) {
                var b = m_buffer[m_offset + i];
                if(!c.Children.TryGetValue(b, out c))
                    break;
                if(c.Encoding != null)
                    return c;
            }
            return null;
        }
        #endregion
        #region private static BuildEncodingPreambleTree()
        private static readonly PreambleNode m_rootPreamble;
        private static PreambleNode BuildEncodingPreambleTree() {
            var encodings = Encoding.GetEncodings()
                .Select(o => {
                    var encoding = o.GetEncoding();
                    return new { Encoding = encoding, Preamble = encoding.GetPreamble() };
                });

            // build hash tree
            var root = new PreambleNode();
            foreach(var encoding in encodings) {
                var c = root;
                foreach(var b in encoding.Preamble) {
                    if(!c.Children.TryGetValue(b, out var child)) {
                        child = new PreambleNode();
                        c.Children.Add(b, child);
                    }
                    c = child;
                }
                c.Encoding = encoding.Encoding;
                c.Length   = (byte)encoding.Preamble.Length;
            }
            // remove "leafs"
            foreach(var node in Recurse(root))
                if(node.Children.Count == 0)
                    node.Children = null;

            return root;

            IEnumerable<PreambleNode> Recurse(PreambleNode n) {
                yield return n;
                if(n.Children != null) {
                    foreach(var child in n.Children.Values)
                        foreach(var item in Recurse(child))
                            yield return item;
                }
            }
        }
        private class PreambleNode {
            public Dictionary<byte, PreambleNode> Children = new Dictionary<byte, PreambleNode>();
            public byte Length; // depth
            public Encoding Encoding;
        }
        #endregion

        #region private static AutoDetectColumnSeparator()
        private static byte AutoDetectColumnSeparator(byte[] buffer, int index, int length) {
            // special note: cannot try to read quoted strings since we need to know the separator for it to make sense
            //      --------- BAD -----------------        ----------- GOOD -----------
            // ex:  row = "12121212,O'Clock,444444"        row = "12121212,'O''Clock',444444"       
            //                       *************                         **********
            // since we can't differentiate without knowing the column separator, we dont try to interpret quotes as strings

            const string SEPARATOR_CANDIDATES_ORDER = ",;|\t:";
            var candidates = new Dictionary<byte, int>(16);

            for(int i = 0; i < length; i++) {
                var b = buffer[index + i];
                if(b == '\n' || b == '\r')
                    break;

                if(SEPARATOR_CANDIDATES_ORDER.IndexOf((char)b) >= 0) {
                    if(candidates.TryGetValue(b, out int count))
                        candidates[b] = count + 1;
                    else
                        candidates.Add(b, 1);
                }
            }

            var most_reoccuring_separator = candidates
                .OrderByDescending(o => o.Value)
                .ThenBy(o => SEPARATOR_CANDIDATES_ORDER.IndexOf((char)o.Key))
                .Take(1)
                .ToList();

            return most_reoccuring_separator.Count == 1 ? most_reoccuring_separator[0].Key : unchecked((byte)SEPARATOR_CANDIDATES_ORDER[0]);
        }
        #endregion
        #region static GetSupportedTypes()
        public static Type[] GetSupportedTypes() {
            return new[] { 
                typeof(string),
                typeof(byte[]),
                typeof(Stream),

                typeof(bool),
                typeof(char),
                typeof(sbyte),
                typeof(short),
                typeof(int),
                typeof(long),
                typeof(byte),
                typeof(ushort),
                typeof(uint),
                typeof(ulong),
                typeof(float),
                typeof(double),
                typeof(decimal),
                typeof(Guid),
                typeof(DateTime),
                typeof(TimeSpan),

                typeof(bool?),
                typeof(char?),
                typeof(sbyte?),
                typeof(short?),
                typeof(int?),
                typeof(long?),
                typeof(byte?),
                typeof(ushort?),
                typeof(uint?),
                typeof(ulong?),
                typeof(float?),
                typeof(double?),
                typeof(decimal?),
                typeof(Guid?),
                typeof(DateTime?),
                typeof(TimeSpan?),
            };
        }
        #endregion
        #region DetectColumnTypes()
        public class ColumnDetectOptions {
            public ColumnDetectMode Mode;
            /// <summary>
            ///     If Mode = Partial, Process up to that amount of bytes.
            ///     Should be a multiple of 4096.
            /// </summary>
            public int PartialMode_BytesToProcess = ushort.MaxValue;
            public ColumnDetectOptions(ColumnDetectMode mode = ColumnDetectMode.Full) {
                this.Mode = mode;
            }
        }
        public enum ColumnDetectMode {
            /// <summary>
            ///     All rows are read.
            /// </summary>
            Full,
            /// <summary>
            ///     All rows within a given size are read.
            /// </summary>
            Partial,
        }
        public class DetectedColumns {
            public DetectedColumn[] Columns;

            public int RowCount;
            /// <summary>
            ///     Checks # of read bytes vs size of file to give an idea of how many rows in total there are.
            /// </summary>
            public int ApproximateTotalRowCount;
        }
        public class DetectedColumn {
            /// <summary>
            ///     Either the column name or an auto-assigned name (Column1, etc)
            /// </summary>
            public string ColumnName;

            /// <summary>
            ///     The type recommended to use.
            ///     This will take into account the number of empty/null values and suggest a type with 50%+ successfull reads.
            ///     ie: this will return Nullable[type] even though Nullable[type] isnt in candidates. 
            ///     And may return {string} even if {string} was not in the candidates (ex: column contains GUIDs, integers, datetimes).
            /// </summary>
            public Type RecommendedType;

            /// <summary>
            ///     The candidates, in most likely order.
            /// </summary>
            public DetectedColumnType[] Candidates;

            /// <summary>
            ///     Rows containing empty values (ie ',,')
            /// </summary>
            public int EmptyCount;
            /// <summary>
            ///     Rows containing explicitly 'null'
            /// </summary>
            public int NullCount;
        }
        public class DetectedColumnType {
            public Type Type;

            /// <summary>
            ///     The number of time that type or any smaller type that can be fully encapsulated (ex: int = int+short+ushort+sbyte+byte) was returned as most likely data type.
            /// </summary>
            public int MostLikelyCountInGroup;
            /// <summary>
            ///     The number of time that type was returned as most likely data type.
            /// </summary>
            public int MostLikelyCount;
            /// <summary>
            ///     The number of time the column could be interpreted losslessly into the given type.
            /// </summary>
            public int ValidCount;
            /// <summary>
            ///     The number of time the column could be interpreted into the given type but with some data loss (ie: double -> float).
            /// </summary>
            public int PotentialCount;
        }
        /// <summary>
        ///     Determines the column types in the CSV stream.
        ///     This will also tell you the potential candidates as well.
        /// </summary>
        public DetectedColumns DetectColumnTypes(ColumnDetectOptions options = null) {
            if(options == null)
                options = new ColumnDetectOptions();

            var pos = m_stream.Position;
            var res = new DetectedColumns();

            try {
                m_stream.Position = m_bomPreambleLength;
                using(var copy = new CsvStreamReader(m_stream, false)) {
                    // copy column names (which may be auto-assigned ie: Column1)
                    var columnNames = copy.GetColumnNames();
                    if(columnNames == null)
                        return res;
                    res.Columns = new DetectedColumn[columnNames.Length];
                    for(int i = 0; i < columnNames.Length; i++)
                        res.Columns[i].ColumnName = columnNames[i];

                    var columnDicts = new Dictionary<Type, DetectedColumnType>[columnNames.Length];
                    for(int i = 0; i < columnNames.Length; i++) {
                        columnDicts[i] = new Dictionary<Type, DetectedColumnType>();
                        foreach(var type in GetSupportedTypes())
                            columnDicts[i].Add(type, new DetectedColumnType() { Type = type });
                    }

                    var emptyValues = new int[columnNames.Length];
                    var nullValues  = new int[columnNames.Length];

                    long processed_bytes    = m_stream.Position;
                    int processed_row_count = 0;
                    while(copy.MoveNext()) {
                        if(options.Mode == ColumnDetectMode.Partial && m_stream.Position > options.PartialMode_BytesToProcess)
                            break;

                        for(int i = 0; i < copy.ColumnCount; i++) {
                            var columnDict = columnDicts[i];
                            
                            // this is the generic parser, which is fairly robust
                            // however it will assume like that most numbers are int/long/decimal and wont do checks for smaller types
                            // also it wont check special bool encodings (0/1, t/f)
                            var current = copy.Current[i];
                            if(current.IsEmpty)
                                emptyValues[i]++;
                            else {
                                // this is where the magic happens
                                var values  = current.GetValues();
                                var metrics = values.Metrics;

                                if(metrics != null) {
                                    // the returned order matters
                                    for(int j = 0; j < metrics.Length; j++) {
                                        var metric = metrics[j];
                                        var column = columnDict[metric.Type];
                                        if(j == 0)
                                            column.MostLikelyCount++;
                                        if(metric.IsLossless)
                                            column.ValidCount++;
                                        else
                                            column.PotentialCount++;
                                    }
                                } else
                                    nullValues[i]++;
                            }
                        }

                        processed_row_count++;
                        processed_bytes = m_stream.Position;
                    }

                    foreach(var columnDict in columnDicts) {
                        foreach(var item in columnDict.ToList()) {
                            if(item.Value.ValidCount == 0 && item.Value.PotentialCount == 0)
                                columnDict.Remove(item.Key);
                        }
                    }

                    // then fixup by type groups
                    // in other words, I'm trying here to 'promote' most results being detected as a 'int' into a 'long' if a few results were found to be longs.
                    // the idea is that most values may appear to be within the 'int' range, but could be representing a larger range of value, which this is fixing
                    // note that this isnt the same as look at the number of valid/potential rows, but the same principle could be applied as well
                    // note: ordering matters
                    var groups_ordered_by_priority = new []{
                        // integers
                        new []{ typeof(long), typeof(ulong), typeof(int), typeof(uint), typeof(short), typeof(ushort), typeof(byte), typeof(sbyte), },
                        // floats
                        new []{ typeof(decimal), typeof(double), typeof(float), },
                    };

                    for(int i = 0; i < columnNames.Length; i++) {
                        foreach(var detectedColumn in columnDicts[i]) {
                            var group_ordered_by_priority = groups_ordered_by_priority
                                .Where(o => o.Any(k => k == detectedColumn.Key))
                                .FirstOrDefault();
                            if(group_ordered_by_priority == null)
                                continue;
                            detectedColumn.Value.MostLikelyCountInGroup = group_ordered_by_priority
                                .SkipWhile(o => o != detectedColumn.Key)
                                .Sum(o => columnDicts[i][o].MostLikelyCount);
                        }
                    }

                    for(int i = 0; i < columnNames.Length; i++) {
                        res.RowCount                 = processed_row_count;
                        res.ApproximateTotalRowCount = (int)((double)processed_row_count * processed_bytes / m_stream.Length);
                        res.Columns[i].EmptyCount    = emptyValues[i];
                        res.Columns[i].NullCount     = nullValues[i];
                        res.Columns[i].Candidates    = columnDicts[i].Values
                            .OrderByDescending(v => Math.Max(v.MostLikelyCountInGroup, v.MostLikelyCount))
                            .ThenByDescending(v => v.MostLikelyCount)
                            .ThenByDescending(v => v.ValidCount)
                            .ThenByDescending(v => v.PotentialCount)
                            .ToArray();

                        // determine the recommended type
                        // the rule of thumb is we want 50% of the data to fit the given type.
                        // the column may contain disparate fields (GUIDs, datetimes, integers) so in this case you want to recommend a string anyway
                        if(res.Columns[i].Candidates.Length > 0) {
                            var main_candidate = res.Columns[i].Candidates[0];

                            var is_nullable_type = GetSupportedTypes()
                            .Where(t => !t.IsValueType || Nullable.GetUnderlyingType(t) == null)
                            .Contains(main_candidate.Type);

                            if(main_candidate.ValidCount + main_candidate.PotentialCount >= (res.RowCount - res.Columns[i].EmptyCount - (is_nullable_type ? res.Columns[i].NullCount : 0)) / 2) {
                                // if supports Nullable<T>
                                res.Columns[i].RecommendedType = Nullable.GetUnderlyingType(main_candidate.Type) != null && (res.Columns[i].EmptyCount > 0 || res.Columns[i].NullCount > 0) ?
                                    typeof(Nullable<>).MakeGenericType(main_candidate.Type) :
                                    main_candidate.Type;
                            } else
                                res.Columns[i].RecommendedType = typeof(string);
                        }
                    }
                }
            } finally {
                m_stream.Position = pos;
            }

            return res;
        }
        #endregion

        // read the raw values
        #region private static ReadValue()
        /// <summary>
        ///     Reads the entire value and trims the start/end.
        ///     Also will remove the start/end double quote if present, but wont un-doublequote the data.
        ///     This is intended so that data stored such as: "-78.32478","45.82734"  will be read as-if they had no quotes.
        ///     
        ///     note: DO NOT CALL THIS ON VARIABLE-LENGTH CONTENT.
        /// </summary>
        private static RawCsvValue ReadValue(in CsvValue value) {
            // dont use this as it is too convoluted
            //value.TrimStart();
            //value.TrimEnd();

            byte[] buffer;
            int index;
            int length;

            if(value.BufferIndex + value.Length <= BUFFER_SIZE) {
                // 99% of cases
                buffer = value.Owner.m_rowBuffers[value.RowBufferIndex];
                index  = value.BufferIndex;
                length = value.Length;
            } else if(value.Length <= CSVVALUE_SIZE) {
                // if the value falls between 2 buffers
                buffer        = value.Owner.m_csvValueBuffer;
                index         = 0;
                length        = value.Length;
                int remainder = BUFFER_SIZE - value.BufferIndex;
                Buffer.BlockCopy(value.Owner.m_rowBuffers[value.RowBufferIndex], value.BufferIndex, buffer, 0, remainder);
                Buffer.BlockCopy(value.Owner.m_rowBuffers[value.RowBufferIndex + 1], 0, buffer, remainder, length - remainder);
            } else {
                // if the value doesnt fit in the usual buffer
                buffer             = new byte[value.Length];
                index              = 0;
                length             = value.Length;
                int remainder      = length;
                int rowBufferIndex = value.RowBufferIndex;
                int bufferIndex    = value.BufferIndex;
                int writeIndex     = 0;
                while(remainder > 0) {
                    var requested = Math.Min(remainder, BUFFER_SIZE);
                    Buffer.BlockCopy(value.Owner.m_rowBuffers[rowBufferIndex++], bufferIndex, buffer, writeIndex, requested);
                    bufferIndex = 0;
                    writeIndex += requested;
                    remainder  -= requested;
                }
            }

            // trimstart()
            while(length > 0) {
                var b = buffer[index];
                if(b != ' ' && b != '\t')
                    break;
                else {
                    index++;
                    length--;
                }
            }
            // trimend()
            while(length > 0) {
                var b = buffer[index + length - 1];
                if(b != ' ' && b != '\t')
                    break;
                else
                    length--;
            }

            if(value.IsDoubleQuoted) {
                index++;
                length -= 2;
            }

            return new RawCsvValue(buffer, index, length);
        }
        private readonly ref struct RawCsvValue {
            public readonly byte[] Data;
            public readonly int Index;
            public readonly int Length;
            public RawCsvValue(byte[] data, int index, int length) {
                this.Data = data;
                this.Index = index;
                this.Length = length;
            }
            public string ConvertToString(Decoder decoder, char[] value) {
                decoder.Reset();
                var len = decoder.GetChars(this.Data, this.Index, this.Length, value, 0);
                return new string(value, 0, len);
            }
        }
        #endregion
        #region private static TryReadValueVariableLength()
        /// <summary>
        ///     Reads the entire value and trims the start/end.
        ///     Also will remove the start/end double quote if present, and will un-doublequote the data.
        /// </summary>
        private static bool TryReadValueVariableLength(in CsvValue value, out RawCsvValue result) {
            //if(value.IsNull) {
            //    result = new RawCsvValue();
            //    return true;
            //}

            // double quoted values are automatically left-trimmed
            //if(value.IsDoubleQuoted)
            //    value.TrimStart();

            byte[] res;
            var rowBuffers     = value.Owner.m_rowBuffers;
            int writeIndex     = 0;
            var rowBufferIndex = value.RowBufferIndex;
            var remaining      = value.Length;
            var index          = value.BufferIndex;

            if(!value.IsDoubleQuoted) {
                res = new byte[value.Length];
                while(remaining > 0) {
                    var rowBuffer = rowBuffers[rowBufferIndex++];
                    var bytes     = Math.Min(remaining, BUFFER_SIZE - index);
                    Buffer.BlockCopy(rowBuffer, index, res, writeIndex, bytes);
                    writeIndex += bytes;
                    index = 0;
                    remaining -= bytes;
                }
            } else {
                res = new byte[value.Length - 2]; // remove the quotes
                byte quote = rowBuffers[rowBufferIndex][index++];
                remaining--;

                if(index == BUFFER_SIZE) {
                    rowBufferIndex++;
                    index = 0;
                }

                bool stateLookingForFollowingQuote = false;

                while(remaining > 0) {
                    var rowBuffer  = rowBuffers[rowBufferIndex++];
                    var bytes      = Math.Min(remaining, BUFFER_SIZE - index);
                    int index2     = index;
                    int remaining2 = bytes;

                    while(remaining2 > 0) {
                        var b = rowBuffer[index2];
                        if(b != quote) {
                            if(stateLookingForFollowingQuote) {
                                // if we have an early finish (ie: 'allo'xxxxx, ignore xxxxx)
                                //throw new FormatException(); // dont throw because the following chars are likely just whitespaces
                                result = new RawCsvValue(res, 0, writeIndex);
                                return true;
                            }
                        } else {
                            if(remaining2 != 1 || remaining - bytes != 0) {
                                if(!stateLookingForFollowingQuote) {
                                    Buffer.BlockCopy(rowBuffer, index, res, writeIndex, index2 - index);
                                    writeIndex += index2 - index;
                                    index       = index2 + 1;
                                    stateLookingForFollowingQuote = true;
                                } else {
                                    index++;
                                    res[writeIndex++] = quote;
                                    stateLookingForFollowingQuote = false;
                                }
                            } else {
                                // if this is the last character of the column and it is a quote, then all is good
                                if(stateLookingForFollowingQuote) { // if we finish like:      'allo''
                                    //throw new FormatException("Unterminated double-quote.");
                                    result = default;
                                    return false;
                                }
                                Buffer.BlockCopy(rowBuffer, index, res, writeIndex, index2 - index);
                                writeIndex += index2 - index;
                                result = new RawCsvValue(res, 0, writeIndex);
                                return true;
                            }
                        }
                        index2++;
                        remaining2--;
                    }

                    if(index < BUFFER_SIZE) {
                        var futureWriteIndex = writeIndex + index2 - index;
                        if(futureWriteIndex > res.Length) { // can only happen at end-of-stream if you have 'aaaaaa    it tries to find the last quote but it cant
                            //throw new FormatException("Unterminated double-quote at end-of-stream.");
                            result = default;
                            return false;
                        }
                        Buffer.BlockCopy(rowBuffer, index, res, writeIndex, index2 - index);
                        writeIndex = futureWriteIndex;
                    }
                    index = 0;
                    remaining -= bytes;
                }
            }

            result = new RawCsvValue(res, 0, writeIndex);
            return true;
        }
        #endregion
        #region private static TryReadValueString()
        /// <summary>
        ///     Reads the entire string value and trims the start/end.
        /// </summary>
        private static bool TryReadValueString(in CsvValue value, out string result) {
            if(value.IsEmpty) {
                result = null;
                return true;
            }

            // double quoted values are automatically left-trimmed
            //if(value.IsDoubleQuoted)
            //    value.TrimStart();

            char[] charBuffer;
            var decoder        = value.Owner.m_decoder;
            var rowBuffers     = value.Owner.m_rowBuffers;
            int writeIndex     = 0;
            var rowBufferIndex = value.RowBufferIndex;
            var remaining      = value.Length;
            var index          = value.BufferIndex;

            // in case of wrong inputs
            decoder.Reset();

            if(!value.IsDoubleQuoted) {
                charBuffer = value.Length <= BUFFER_SIZE ? value.Owner.m_charBuffer : new char[value.Length];
                while(remaining > 0) {
                    var rowBuffer = rowBuffers[rowBufferIndex++];
                    var bytes     = Math.Min(remaining, BUFFER_SIZE - index);
                    writeIndex   += decoder.GetChars(rowBuffer, index, bytes, charBuffer, writeIndex);
                    index         = 0;
                    remaining    -= bytes;
                }
            } else {
                charBuffer = value.Length - 2 <= BUFFER_SIZE ? value.Owner.m_charBuffer : new char[value.Length - 2];
                byte quote = rowBuffers[rowBufferIndex][index++];
                remaining--;

                if(index == BUFFER_SIZE) {
                    rowBufferIndex++;
                    index = 0;
                }

                bool stateLookingForFollowingQuote = false;

                while(remaining > 0) {
                    var rowBuffer  = rowBuffers[rowBufferIndex++];
                    var bytes      = Math.Min(remaining, BUFFER_SIZE - index);
                    int index2     = index;
                    int remaining2 = bytes;

                    while(remaining2 > 0) {
                        var b = rowBuffer[index2];
                        if(b != quote) {
                            if(stateLookingForFollowingQuote) {
                                // if we have an early finish (ie: 'allo'xxxxx, ignore xxxxx)
                                //throw new FormatException(); // dont throw because the following chars are likely just whitespaces
                                result = new string(charBuffer, 0, writeIndex);
                                return true;
                            }
                        } else {
                            if(remaining2 != 1 || remaining - bytes != 0) {
                                if(!stateLookingForFollowingQuote) {
                                    writeIndex                   += decoder.GetChars(rowBuffer, index, index2 - index, charBuffer, writeIndex);
                                    index                         = index2 + 1;
                                    stateLookingForFollowingQuote = true;
                                } else {
                                    index++;
                                    charBuffer[writeIndex++]      = (char)quote;
                                    stateLookingForFollowingQuote = false;
                                }
                            } else {
                                // if this is the last character of the column and it is a quote, then all is good
                                if(stateLookingForFollowingQuote) { // if we finish like:      'allo''
                                    //throw new FormatException("Unterminated double-quote.");
                                    result = null;
                                    return false;
                                }
                                writeIndex += decoder.GetChars(rowBuffer, index, index2 - index, charBuffer, writeIndex);
                                result = new string(charBuffer, 0, writeIndex);
                                return true;
                            }
                        }
                        index2++;
                        remaining2--;
                    }

                    if(index < BUFFER_SIZE)
                        writeIndex += decoder.GetChars(rowBuffer, index, index2 - index, charBuffer, writeIndex);
                    index = 0;
                    remaining -= bytes;
                }
            }

            result = new string(charBuffer, 0, writeIndex);
            return true;
        }
        #endregion
        #region private static TryReadValueHex()
        /// <summary>
        ///     Reads the entire string value and trims the start/end.
        /// </summary>
        private static bool TryReadValueHex(in CsvValue value, out byte[] result) {
            if(value.IsEmpty) {
                result = null;
                return true;
            }

            if(!value.IsDoubleQuoted) {
                value.TrimStart();
                value.TrimEnd();
            }

            // double quoted values are automatically left-trimmed
            //if(value.IsDoubleQuoted)
            //    value.TrimStart();

            HexDecoder decoder;
            var rowBuffers     = value.Owner.m_rowBuffers;
            var rowBufferIndex = value.RowBufferIndex;
            var remaining      = value.Length;
            var index          = value.BufferIndex;

            if(!value.IsDoubleQuoted) {
                decoder = new HexDecoder(value.Length / 2);
                while(remaining > 0) {
                    var rowBuffer = rowBuffers[rowBufferIndex++];
                    var bytes     = Math.Min(remaining, BUFFER_SIZE - index);
                    decoder.Add(rowBuffer, index, bytes);
                    index      = 0;
                    remaining -= bytes;
                }
            } else {
                decoder    = new HexDecoder((value.Length - 2) / 2); // remove the quotes
                byte quote = rowBuffers[rowBufferIndex][index++];
                remaining--;

                if(index == BUFFER_SIZE) {
                    rowBufferIndex++;
                    index = 0;
                }

                try {
                    while(remaining > 0) {
                        var rowBuffer = rowBuffers[rowBufferIndex++];
                        var bytes     = Math.Min(remaining, BUFFER_SIZE - index);

                        if(!decoder.AddUntilQuote(rowBuffer, index, bytes, quote))
                            break;

                        index = 0;
                        remaining -= bytes;
                    }
                } catch(IndexOutOfRangeException) {
                    // due to the HexDecoder max_size that removes the double quotes, if you have an unterminated doublequote it will crash
                    // this just returns a more appropriate error
                    //throw new FormatException("Unterminated double-quote at end-of-stream.");
                    result = null;
                    return false;
                }
            }

            result = decoder.Flush();
            return !decoder.IsError;
        }
        private ref struct HexDecoder {
            private byte m_prev;
            private bool m_hasPrev;
            private byte[] m_buffer;
            private int m_bufferOffset;
            public bool IsError;

            public HexDecoder(int maxBufferSize) {
                m_prev         = 0;
                m_hasPrev      = false;
                m_bufferOffset = 0;
                m_buffer       = new byte[maxBufferSize];
                this.IsError   = false;
            }

            public void Add(byte[] source, int offset, int count) {
                if(this.IsError)
                    return;

                // align
                if(m_hasPrev) {
                    m_buffer[m_bufferOffset++] = unchecked((byte)((m_prev << 4) | HexDecode(source[offset++])));
                    count--;
                    m_hasPrev = false;
                }

                while(count >= 2) {
                    m_buffer[m_bufferOffset++] = unchecked((byte)((HexDecode(source[offset + 0]) << 4) | HexDecode(source[offset + 1])));
                    offset += 2;
                    count  -= 2;
                }

                if(count == 1) {
                    m_prev    = unchecked((byte)HexDecode(source[offset]));
                    m_hasPrev = true;
                }
            }
            /// <summary>
            ///     Returns false if quote found.
            /// </summary>
            public bool AddUntilQuote(byte[] source, int offset, int count, byte quote) {
                if(this.IsError)
                    return false;

                // align
                if(m_hasPrev) {
                    var b = source[offset++];
                    if(b == quote)
                        return false;
                    m_buffer[m_bufferOffset++] = unchecked((byte)((m_prev << 4) | HexDecode(b)));
                    count--;
                    m_hasPrev = false;
                }

                while(count >= 2) {
                    var b1 = source[offset + 0];
                    if(b1 == quote)
                        return false;

                    var b2 = source[offset + 1];
                    if(b2 == quote) {
                        m_prev    = b1;
                        m_hasPrev = false;
                        return false;
                    }

                    m_buffer[m_bufferOffset++] = unchecked((byte)((HexDecode(b1) << 4) | HexDecode(b2)));
                    offset += 2;
                    count  -= 2;
                }

                if(count == 1) {
                    var b = source[offset];
                    if(b == quote)
                        return false;
                    m_prev = unchecked((byte)HexDecode(b));
                    m_hasPrev = true;
                }

                return !IsError;
            }
            public byte[] Flush() {
                if(m_hasPrev)
                    //throw new FormatException($"Invalid HEX encoded value, contains non-pair amount of bytes ({m_bufferOffset * 2 + 1}).");
                    this.IsError = true;

                if(this.IsError)
                    return null;

                if(m_bufferOffset != m_buffer.Length)
                    Array.Resize(ref m_buffer, m_bufferOffset);

                return m_buffer;
            }
            [MethodImpl(AggressiveInlining)]
            private int HexDecode(byte c) {
                if(c <= '9' && c >= '0')
                    return c - '0';
                if(c <= 'F' && c >= 'A')
                    return c - 'A' + 10;
                if(c <= 'f' && c >= 'a')
                    return c - 'a' + 10;

                //throw new FormatException($"Invalid HEX character {c}.");
                this.IsError = true;
                return 0;
            }
        }
        #endregion

        // BitMethods imported functions
        #region private static ParseObject()
        /// <summary>
        ///     Tries to guess what the data contained in the text is.
        ///     Defaults to string UTF8 encoded if unknown.
        ///     Will not look for quoting and interpret all values directly.
        /// </summary>
        /// <param name="charBuffer">Should be >= count.</param>
        private static object ParseObject(byte[] buffer, int offset, int count, char[] charBuffer) {
            if(count == 0)
                return null;

            var b = buffer[offset];

            // special values:
            // <nothing>, null, true, false, NaN, Inf, -Inf, -<timestamp>, "..."
            if(count == 4) {
                var b2 = buffer[offset + 1];
                var b3 = buffer[offset + 2];
                var b4 = buffer[offset + 3];

                // check for 'null'
                if((b == 'n' || b == 'N') && (b2 == 'u' || b2 == 'U') && (b3 == 'l' || b3 == 'L') && (b4 == 'l' || b4 == 'L'))
                    return null;
                // check for 'true'
                if((b == 't' || b == 'T') && (b2 == 'r' || b2 == 'R') && (b3 == 'u' || b3 == 'U') && (b4 == 'e' || b4 == 'E'))
                    return true;
                // check for '-Inf'
                if(b == '-' && (b2 == 'i' || b2 == 'I') && (b3 == 'n' || b3 == 'N') && (b4 == 'f' || b4 == 'F'))
                    return double.NegativeInfinity;
            } else if(count == 5) {
                var b2 = buffer[offset + 1];
                var b3 = buffer[offset + 2];
                var b4 = buffer[offset + 3];
                var b5 = buffer[offset + 4];

                // check for 'false'
                if((b == 'f' || b == 'F') && (b2 == 'a' || b2 == 'A') && (b3 == 'l' || b3 == 'L') && (b4 == 's' || b4 == 'S') && (b5 == 'e' || b5 == 'E'))
                    return false;
            } else if(count == 3) {
                var b2 = buffer[offset + 1];
                var b3 = buffer[offset + 2];

                // check for 'NaN'
                if((b == 'N' || b == 'n') && (b2 == 'a' || b2 == 'A') && (b3 == 'N' || b3 == 'n'))
                    return double.NaN;
                // check for 'Inf'
                if((b == 'i' || b == 'I') && (b2 == 'n' || b2 == 'N') && (b3 == 'f' || b3 == 'F'))
                    return double.PositiveInfinity;
            } else if(count == 1) {
                if(b == 't' || b == 'T' || b == '1')
                    return true;
                if(b == 'f' || b == 'F' || b == '0')
                    return false;
                if(b <= '9' && b >= '2')
                    return unchecked((int)(b - '0'));
                // else assume its just a char
                return (char)b;
            }

            // count the number of '-', '/', ':', '.'
            // and only [0-9 ,./-] are valid

            int digit_count        = 0;
            int dash_count         = 0;
            int slash_count        = 0;
            int spacer_count       = 0;
            int dot_index          = -1;
            int doubledot_count    = 0;
            int uppercase_T_count  = 0; // in case of 20100101T000000
            bool contains_exponent = false;

            for(int i = 0; i < count; i++) {
                b = buffer[offset + i];

                if(b <= '9' && b >= '0')
                    digit_count++;
                else if(b == '.' && dot_index == -1)
                    dot_index = offset + i;
                else if(b == '-')
                    dash_count++;
                else if(b == '/')
                    slash_count++;
                else if(b == ':')
                    doubledot_count++;
                else if(b == ' ' || b == '+')
                    spacer_count++;
                else if(b == 'T' && uppercase_T_count == 0)
                    uppercase_T_count++;
                else if(count >= 32 && ((b <= 'f' && b >= 'a') || (b <= 'F' && b >= 'A') || b == '{' || b == '}' || b == '(' || b == ')')) // GUID
                    continue;
                else if(count < 32 && (b == 'E' || b == 'e') && !contains_exponent)
                    contains_exponent = true;
                else
                    return ConvertToString(buffer, offset, count, charBuffer);
            }

            if(count >= 32 && dot_index < 0 && slash_count == 0 && spacer_count == 0 && uppercase_T_count == 0 && (dash_count == 0 || dash_count == 4)) {
                var index = offset;
                if(TryParseGuid(buffer, ref index, count, out var guid))
                    return guid;
                // note: if this isn't a GUID, it can still be a very precise decimal that we truncate the precision at the end of it
                // ex: 0.1234567890123456789012345678901234567890
            } else {
                bool triedParseDateTime = false;

                if(dash_count > 1 || slash_count > 0) {
                    var index = offset;
                    if(TryParseDateTime(buffer, ref index, out var dt))
                        return dt;
                    triedParseDateTime = true;
                }
                if(doubledot_count > 0) {
                    var index = offset;
                    if(TryParseTimeSpan(buffer, ref index, dot_index >= 0, out var ts))
                        return ts;
                }
                if(uppercase_T_count == 1 && !triedParseDateTime) {
                    var index = offset;
                    if(TryParseDateTime(buffer, ref index, out var dt))
                        return dt;
                }
            }

            if(doubledot_count == 0 &&
                slash_count == 0 &&
                uppercase_T_count == 0 &&
                spacer_count == 0 &&
                (dash_count <= 1 || (dash_count == 2 && contains_exponent)) &&
                TryParseNumber(buffer, offset, count, digit_count, dash_count > 0, dot_index, contains_exponent, charBuffer, out var res))
                return res;

            // if we dont know what the data type is, then treat it as UTF-8 string
            return ConvertToString(buffer, offset, count, charBuffer);
        }
        private static string ConvertToString(byte[] buffer, int offset, int count, char[] charBuffer) {
            var len = Encoding.UTF8.GetChars(buffer, offset, count, charBuffer, 0);
            return new string(charBuffer, 0, len);
        }
        private static bool TryParseNumber(byte[] buffer, int offset, int count, int digit_count, bool has_negative_number, int dot_index, bool contains_exponent, char[] charBuffer, out object result) {
            if(contains_exponent) {
                var parse_result = double.TryParse(
                    ConvertToString(buffer, offset, count, charBuffer),
                    NumberStyles.AllowCurrencySymbol | NumberStyles.AllowLeadingSign | NumberStyles.AllowExponent | NumberStyles.AllowDecimalPoint,
                    FORMAT,
                    out var res);
                if(parse_result) {
                    result = res;
                    return true;
                }
            } else if(dot_index < 0) {
                if(count <= 9) {
                    if(TryParseInt32(buffer, offset, count, out var res)) {
                        result = res;
                        return true;
                    }
                } else if(count <= 18) {
                    if(TryParseInt64(buffer, offset, count, out var res)) {
                        if(res <= int.MaxValue && res >= int.MinValue)
                            result = unchecked((int)res);
                        else
                            result = res;
                        return true;
                    }
                } else {
                    decimal res;
                    bool parse_result;

                    if(digit_count == count - (has_negative_number ? 1 : 0)) {
                        // use fast decimal parser if everything is just -99999999999999999999999999
                        parse_result = TryParseDecimal(buffer, offset, count, -1, out res);
                    } else {
                        // since this has no floating points, the highest number available in .net is a decimal
                        parse_result = decimal.TryParse(
                            ConvertToString(buffer, offset, count, charBuffer),
                            NumberStyles.AllowCurrencySymbol | NumberStyles.AllowLeadingSign, // | NumberStyles.AllowDecimalPoint
                            FORMAT,
                            out res);
                    }

                    if(parse_result) {
                        if(res <= long.MaxValue && res >= long.MinValue)
                            result = unchecked((long)res);
                        else if(res <= ulong.MaxValue && res >= ulong.MinValue)
                            result = unchecked((ulong)res);
                        else
                            result = res;

                        return true;
                    }
                }
            } else {
                int magnitude = dot_index - offset - (has_negative_number ? 1 : 0); // -68.34567 = 2
                int precision = count - (dot_index - offset) - 1;                   // -68.34567 = 5

                //          precision
                // float       ~6-9
                // double      ~15-17
                // decimal     ~28-29
                bool use_double_parser = magnitude + precision <= 15;

                // if everything is just -9999999999999.9999999999999, then use the fast parser
                if(digit_count == count - (has_negative_number ? 1 : 0) - 1) { // -1 for dot
                    if(TryParseDecimal(buffer, offset, count, dot_index, out var res)) {
                        if(use_double_parser)
                            result = (double)res;
                        else
                            result = res;
                        return true;
                    }
                } else {
                    if(use_double_parser) {
                        var parse_result = double.TryParse(
                            ConvertToString(buffer, offset, count, charBuffer),
                            NumberStyles.AllowCurrencySymbol | NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint,
                            FORMAT,
                            out var res);
                        if(parse_result) {
                            result = res;
                            return true;
                        }
                    } else {
                        var parse_result = decimal.TryParse(
                            ConvertToString(buffer, offset, count, charBuffer),
                            NumberStyles.AllowCurrencySymbol | NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint,
                            FORMAT,
                            out var res);
                        if(parse_result) {
                            result = (decimal)(double)res == res ? (object)(double)res : (object)res;
                            return true;
                        }
                    }
                }
            }
            result = null;
            return false;
        }
        #endregion
        #region private static ParseObjectAndMetrics()
        private struct ParsedObject {
            public bool IsEmpty;  // contains no data (ie ',,')
            public bool IsQuoted; // indicates the data is surrounded by quotation marks
            
            public BasicMetric[] Metrics; // null if the value was explicitly 'null'

            public ParsedObject(bool is_quoted, IEnumerable<BasicMetric> metrics) {
                this.IsEmpty  = false;
                this.IsQuoted = is_quoted;
                this.Metrics  = metrics?.ToArray();
            }
        }
        private struct BasicMetric {
            public Type Type;
            public object Value;
            public MatchType Match;

            public BasicMetric(object value, Type type) : this(value, type, MatchType.Lossless) { }
            public BasicMetric(object value, Type type, MatchType match) {
                this.Value = value;
                this.Type  = type;
                this.Match = match;
            }
        }
        private enum MatchType {
            /// <summary>
            ///     ex: converting byte to int.
            /// </summary>
            Lossless,
            /// <summary>
            ///     ex: converting double to float, some precision may be lost.
            /// </summary>
            Lossy,
        }
        /// <summary>
        ///     Tries to guess the possible data types contained.
        ///     Will not look for quoting and interpret all values directly.
        ///     Results are returned in order of likelyness.
        /// </summary>
        /// <param name="charBuffer">Should be >= count.</param>
        private static ParsedObject ParseObjectAndMetrics(byte[] buffer, int offset, int count, char[] charBuffer, bool is_surrounded_by_quotes) {
            if(count == 0)
                return new ParsedObject(is_surrounded_by_quotes, null) { IsEmpty = true };

            var b = buffer[offset];

            // special values:
            // <nothing>, null, true, false, NaN, Inf, -Inf, -<timestamp>, "..."
            if(count == 4) {
                var b2 = buffer[offset + 1];
                var b3 = buffer[offset + 2];
                var b4 = buffer[offset + 3];
                
                // check for 'null'
                if((b == 'n' || b == 'N') && (b2 == 'u' || b2 == 'U') && (b3 == 'l' || b3 == 'L') && (b4 == 'l' || b4 == 'L'))
                    return new ParsedObject(is_surrounded_by_quotes, null) { IsEmpty = false };
                // check for 'true'
                if((b == 't' || b == 'T') && (b2 == 'r' || b2 == 'R') && (b3 == 'u' || b3 == 'U') && (b4 == 'e' || b4 == 'E'))
                    return new ParsedObject(is_surrounded_by_quotes, new[] { new BasicMetric(true, typeof(bool)) });
                // check for '-Inf'
                if(b == '-' && (b2 == 'i' || b2 == 'I') && (b3 == 'n' || b3 == 'N') && (b4 == 'f' || b4 == 'F'))
                    return new ParsedObject(is_surrounded_by_quotes, new[] { 
                        new BasicMetric(double.NegativeInfinity, typeof(double)),
                        new BasicMetric(float.NegativeInfinity, typeof(float)),
                    });
            } else if(count == 5) {
                var b2 = buffer[offset + 1];
                var b3 = buffer[offset + 2];
                var b4 = buffer[offset + 3];
                var b5 = buffer[offset + 4];

                // check for 'false'
                if((b == 'f' || b == 'F') && (b2 == 'a' || b2 == 'A') && (b3 == 'l' || b3 == 'L') && (b4 == 's' || b4 == 'S') && (b5 == 'e' || b5 == 'E'))
                    return new ParsedObject(is_surrounded_by_quotes, new[] { new BasicMetric(false, typeof(bool)) });
            } else if(count == 3) {
                var b2 = buffer[offset + 1];
                var b3 = buffer[offset + 2];

                // check for 'NaN'
                if((b == 'N' || b == 'n') && (b2 == 'a' || b2 == 'A') && (b3 == 'N' || b3 == 'n'))
                    return new ParsedObject(is_surrounded_by_quotes, new[] { 
                        new BasicMetric(double.NaN, typeof(double)),
                        new BasicMetric(float.NaN, typeof(float)),
                    });
                // check for 'Inf'
                if((b == 'i' || b == 'I') && (b2 == 'n' || b2 == 'N') && (b3 == 'f' || b3 == 'F'))
                    return new ParsedObject(is_surrounded_by_quotes, new[] { 
                        new BasicMetric(double.PositiveInfinity, typeof(double)),
                        new BasicMetric(float.PositiveInfinity, typeof(float)),
                    });
            } else if(count == 1) {
                if(b == 't' || b == 'T')
                    return new ParsedObject(is_surrounded_by_quotes, new[] { 
                        new BasicMetric(true, typeof(bool)),
                        new BasicMetric((char)b, typeof(char)),
                    });
                if(b == 'f' || b == 'F')
                    return new ParsedObject(is_surrounded_by_quotes, new[] { 
                        new BasicMetric(false, typeof(bool)),
                        new BasicMetric((char)b, typeof(char)),
                    });
                if(b == '0')
                    return new ParsedObject(is_surrounded_by_quotes, new[] { 
                        new BasicMetric(false, typeof(bool)),
                        new BasicMetric((int)0, typeof(int)),
                        new BasicMetric((long)0, typeof(long)),
                        new BasicMetric((char)b, typeof(char)),
                        new BasicMetric((double)0, typeof(double)),
                        new BasicMetric((float)0, typeof(float)),
                        new BasicMetric((uint)0, typeof(uint)),
                        new BasicMetric((ulong)0, typeof(ulong)),
                        new BasicMetric((decimal)0, typeof(decimal)),
                        new BasicMetric((byte)0, typeof(byte)),
                        new BasicMetric((ushort)0, typeof(ushort)),
                        new BasicMetric((sbyte)0, typeof(sbyte)),
                        new BasicMetric((short)0, typeof(short)),
                    });
                if(b == '1')
                    return new ParsedObject(is_surrounded_by_quotes, new[] { 
                        new BasicMetric(true, typeof(bool)),
                        new BasicMetric((int)1, typeof(int)),
                        new BasicMetric((long)1, typeof(long)),
                        new BasicMetric((char)b, typeof(char)),
                        new BasicMetric((double)1, typeof(double)),
                        new BasicMetric((float)1, typeof(float)),
                        new BasicMetric((uint)1, typeof(uint)),
                        new BasicMetric((ulong)1, typeof(ulong)),
                        new BasicMetric((decimal)1, typeof(decimal)),
                        new BasicMetric((byte)1, typeof(byte)),
                        new BasicMetric((ushort)1, typeof(ushort)),
                        new BasicMetric((sbyte)1, typeof(sbyte)),
                        new BasicMetric((short)1, typeof(short)),
                    });
                if(b <= '9' && b >= '2') {
                    int val = b - '0';
                    return new ParsedObject(is_surrounded_by_quotes, new[] {
                        new BasicMetric((int)val, typeof(int)),
                        new BasicMetric((long)val, typeof(long)),
                        new BasicMetric((char)b, typeof(char)),
                        new BasicMetric((double)val, typeof(double)),
                        new BasicMetric((float)val, typeof(float)),
                        new BasicMetric((uint)val, typeof(uint)),
                        new BasicMetric((ulong)val, typeof(ulong)),
                        new BasicMetric((decimal)val, typeof(decimal)),
                        new BasicMetric((byte)val, typeof(byte)),
                        new BasicMetric((ushort)val, typeof(ushort)),
                        new BasicMetric((sbyte)val, typeof(sbyte)),
                        new BasicMetric((short)val, typeof(short)),
                    });
                }
                // else assume its just a char
                return new ParsedObject(is_surrounded_by_quotes, new[] { new BasicMetric((char)b, typeof(char)) });
            }

            // count the number of '-', '/', ':', '.'
            // and only [0-9 ,./-] are valid

            int digit_count        = 0;
            int dash_count         = 0;
            int slash_count        = 0;
            int spacer_count       = 0;
            int dot_index          = -1;
            int doubledot_count    = 0;
            int uppercase_T_count  = 0; // in case of 20100101T000000
            bool contains_exponent = false;

            for(int i = 0; i < count; i++) {
                b = buffer[offset + i];

                if(b <= '9' && b >= '0')
                    digit_count++;
                else if(b == '.' && dot_index == -1)
                    dot_index = offset + i;
                else if(b == '-')
                    dash_count++;
                else if(b == '/')
                    slash_count++;
                else if(b == ':')
                    doubledot_count++;
                else if(b == ' ' || b == '+')
                    spacer_count++;
                else if(b == 'T' && uppercase_T_count == 0)
                    uppercase_T_count++;
                else if(count >= 32 && ((b <= 'f' && b >= 'a') || (b <= 'F' && b >= 'A') || b == '{' || b == '}' || b == '(' || b == ')')) // GUID
                    continue;
                else if(count < 32 && (b == 'E' || b == 'e') && !contains_exponent)
                    contains_exponent = true;
                else
                    return new ParsedObject(is_surrounded_by_quotes, new[] { new BasicMetric(ConvertToString(buffer, offset, count, charBuffer), typeof(string)) });
            }

            if(count >= 32 && dot_index < 0 && slash_count == 0 && spacer_count == 0 && uppercase_T_count == 0 && (dash_count == 0 || dash_count == 4)) {
                var index = offset;
                if(TryParseGuid(buffer, ref index, count, out var guid))
                    return new ParsedObject(is_surrounded_by_quotes, new[] { new BasicMetric(guid, typeof(Guid)) });
                // note: if this isn't a GUID, it can still be a very precise decimal that we truncate the precision at the end of it
                // ex: 0.1234567890123456789012345678901234567890
            } else {
                bool triedParseDateTime = false;

                if(dash_count > 1 || slash_count > 0) {
                    var index = offset;
                    if(TryParseDateTime(buffer, ref index, out var dt))
                        return new ParsedObject(is_surrounded_by_quotes, new[] { new BasicMetric(dt, typeof(DateTime)) });
                    triedParseDateTime = true;
                }
                if(doubledot_count > 0) {
                    var index = offset;
                    if(TryParseTimeSpan(buffer, ref index, dot_index >= 0, out var ts))
                        return new ParsedObject(is_surrounded_by_quotes, new[] { new BasicMetric(ts, typeof(TimeSpan)) });
                }
                if(uppercase_T_count == 1 && !triedParseDateTime) {
                    var index = offset;
                    if(TryParseDateTime(buffer, ref index, out var dt))
                        return new ParsedObject(is_surrounded_by_quotes, new[] { new BasicMetric(dt, typeof(DateTime)) });
                }
            }

            if(doubledot_count == 0 &&
                slash_count == 0 &&
                uppercase_T_count == 0 &&
                spacer_count == 0 &&
                (dash_count <= 1 || (dash_count == 2 && contains_exponent)) &&
                TryParseNumber(buffer, offset, count, digit_count, dash_count > 0, dot_index, contains_exponent, charBuffer, is_surrounded_by_quotes, out var res))
                return res;

            // if we dont know what the data type is, then treat it as UTF-8 string
            return new ParsedObject(is_surrounded_by_quotes, new[] { new BasicMetric(ConvertToString(buffer, offset, count, charBuffer), typeof(string)) });
        }
        private static bool TryParseNumber(byte[] buffer, int offset, int count, int digit_count, bool has_negative_number, int dot_index, bool contains_exponent, char[] charBuffer, bool is_surrounded_by_quotes, out ParsedObject result) {
            if(contains_exponent) {
                var parse_result = double.TryParse(
                    ConvertToString(buffer, offset, count, charBuffer),
                    NumberStyles.AllowCurrencySymbol | NumberStyles.AllowLeadingSign | NumberStyles.AllowExponent | NumberStyles.AllowDecimalPoint,
                    FORMAT,
                    out var res);
                if(parse_result) {
                    // if you write with an exponent, it almost guaranteed is not meant to be a decimal even though it's possible the decimal is able to represent the value
                    // as such, we do not return a decimal as a potential result at it isn't what is meant to be returned

                    var include_float = res >= float.MinValue && res <= float.MaxValue;
                    var metrics = !include_float ?
                        new[] { new BasicMetric(res, typeof(double)) } :
                        new[] { 
                            new BasicMetric(res, typeof(double)), // always prioritize double on exponents notation, since its likely the intended data
                            new BasicMetric((float)res, typeof(float), (float)res == res ? MatchType.Lossless : MatchType.Lossy), // not the best check, but a lot faster than "((float)res).ToString(invariant) == res.ToString(invariant)"
                        };

                    result = new ParsedObject(is_surrounded_by_quotes, metrics);
                    return true;
                }
            } else if(dot_index < 0) {
                if(count <= 9) {
                    if(TryParseInt32(buffer, offset, count, out var res)) {
                        var metrics = new List<BasicMetric>(11) {
                            new BasicMetric((int)res, typeof(int)),
                            new BasicMetric((long)res, typeof(long)),
                            new BasicMetric((double)res, typeof(double)),
                            // cast is intentional and not redundant
                            new BasicMetric((float)res, typeof(float), (int)(float)res == res ? MatchType.Lossless : MatchType.Lossy),
                        };
                        if(res >= 0) {
                            metrics.Add(new BasicMetric((uint)res, typeof(uint)));
                            metrics.Add(new BasicMetric((ulong)res, typeof(ulong)));
                        }
                        metrics.Add(new BasicMetric((decimal)res, typeof(decimal)));
                        if(res >= 0) {
                            if(res <= byte.MaxValue)   metrics.Add(new BasicMetric((byte)res, typeof(byte)));
                            if(res <= ushort.MaxValue) metrics.Add(new BasicMetric((ushort)res, typeof(ushort)));
                        }
                        if(res >= sbyte.MinValue && res <= sbyte.MaxValue) metrics.Add(new BasicMetric((sbyte)res, typeof(sbyte)));
                        if(res >= short.MinValue && res <= short.MaxValue) metrics.Add(new BasicMetric((short)res, typeof(short)));

                        result = new ParsedObject(is_surrounded_by_quotes, metrics.ToArray());
                        return true;
                    }
                } else if(count <= 18) {
                    if(TryParseInt64(buffer, offset, count, out var res)) {
                        var metrics = new List<BasicMetric>(11);
                        if(res <= int.MaxValue && res >= int.MinValue) metrics.Add(new BasicMetric((int)res, typeof(int)));
                        metrics.Add(new BasicMetric((long)res, typeof(long)));
                        // cast is intentional and not redundant
                        metrics.Add(new BasicMetric((double)res, typeof(double), (long)(double)res == res ? MatchType.Lossless : MatchType.Lossy));
                        metrics.Add(new BasicMetric((float)res, typeof(float), (long)(float)res == res ? MatchType.Lossless : MatchType.Lossy));
                        
                        if(res >= 0) {
                            if(res <= uint.MaxValue)  metrics.Add(new BasicMetric((uint)res, typeof(uint)));
                            metrics.Add(new BasicMetric((ulong)res, typeof(ulong)));
                        }
                        metrics.Add(new BasicMetric((decimal)res, typeof(decimal)));
                        if(res >= 0) {
                            if(res <= byte.MaxValue)   metrics.Add(new BasicMetric((byte)res, typeof(byte)));
                            if(res <= ushort.MaxValue) metrics.Add(new BasicMetric((ushort)res, typeof(ushort)));
                        }
                        if(res >= sbyte.MinValue && res <= sbyte.MaxValue) metrics.Add(new BasicMetric((sbyte)res, typeof(sbyte)));
                        if(res >= short.MinValue && res <= short.MaxValue) metrics.Add(new BasicMetric((short)res, typeof(short)));

                        result = new ParsedObject(is_surrounded_by_quotes, metrics.ToArray());
                        return true;
                    }
                } else {
                    decimal res;
                    bool parse_result;

                    if(digit_count == count - (has_negative_number ? 1 : 0)) {
                        // use fast decimal parser if everything is just -99999999999999999999999999
                        parse_result = TryParseDecimal(buffer, offset, count, -1, out res);
                    } else {
                        // since this has no floating points, the highest number available in .net is a decimal
                        parse_result = decimal.TryParse(
                            ConvertToString(buffer, offset, count, charBuffer),
                            NumberStyles.AllowCurrencySymbol | NumberStyles.AllowLeadingSign, // | NumberStyles.AllowDecimalPoint
                            FORMAT,
                            out res);
                    }

                    if(parse_result) {
                        var metrics = new List<BasicMetric>(3);
                        if(res >= long.MinValue && res <= long.MaxValue)   metrics.Add(new BasicMetric((long)res, typeof(long)));
                        if(res >= ulong.MinValue && res <= ulong.MaxValue) metrics.Add(new BasicMetric((ulong)res, typeof(ulong)));
                        metrics.Add(new BasicMetric((decimal)res, typeof(decimal)));
                        
                        result = new ParsedObject(is_surrounded_by_quotes, metrics.ToArray());
                        return true;
                    }
                }
            } else {
                int magnitude = dot_index - offset - (has_negative_number ? 1 : 0); // -68.34567 = 2
                int precision = count - (dot_index - offset) - 1;                   // -68.34567 = 5

                //          precision
                // float       ~6-9
                // double      ~15-17
                // decimal     ~28-29

                bool use_double_parser = magnitude + precision <= 15;

                // if everything is just -9999999999999.9999999999999, then use the fast parser
                if(digit_count == count - (has_negative_number ? 1 : 0) - 1) { // -1 for dot
                    if(TryParseDecimal(buffer, offset, count, dot_index, out var res)) {
                        var metrics      = new List<BasicMetric>(3);
                        var float_match  = (decimal)(float)res == res ? MatchType.Lossless : MatchType.Lossy;
                        var double_match = (decimal)(double)res == res ? MatchType.Lossless : MatchType.Lossy;

                        if(float_match == MatchType.Lossless && double_match == MatchType.Lossless) {
                            metrics.Add(new BasicMetric((float)res, typeof(float), float_match));
                            metrics.Add(new BasicMetric((double)res, typeof(double), double_match));
                            metrics.Add(new BasicMetric((decimal)res, typeof(decimal)));
                        } else if(float_match == MatchType.Lossless && double_match == MatchType.Lossy) {
                            metrics.Add(new BasicMetric((float)res, typeof(float), float_match));
                            metrics.Add(new BasicMetric((decimal)res, typeof(decimal)));
                            metrics.Add(new BasicMetric((double)res, typeof(double), double_match));
                        } else if(float_match == MatchType.Lossy && double_match == MatchType.Lossless) {
                            metrics.Add(new BasicMetric((double)res, typeof(double), double_match));
                            metrics.Add(new BasicMetric((decimal)res, typeof(decimal)));
                            metrics.Add(new BasicMetric((float)res, typeof(float), float_match));
                        } else {
                            metrics.Add(new BasicMetric((decimal)res, typeof(decimal)));
                            metrics.Add(new BasicMetric((double)res, typeof(double), double_match));
                            metrics.Add(new BasicMetric((float)res, typeof(float), float_match));
                        }
                        
                        result = new ParsedObject(is_surrounded_by_quotes, metrics.ToArray());
                        return true;
                    }
                } else {
                    if(use_double_parser) {
                        var parse_result = double.TryParse(
                            ConvertToString(buffer, offset, count, charBuffer),
                            NumberStyles.AllowCurrencySymbol | NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint,
                            FORMAT,
                            out var res);
                        if(parse_result) {
                            var metrics = new List<BasicMetric>(3);

                            if(res >= float.MinValue && res <= float.MaxValue) {
                                if((float)res != res) {
                                    metrics.Add(new BasicMetric((double)res, typeof(double)));
                                    metrics.Add(new BasicMetric((decimal)res, typeof(decimal)));
                                    metrics.Add(new BasicMetric((float)res, typeof(float), MatchType.Lossy));
                                } else {
                                    metrics.Add(new BasicMetric((float)res, typeof(float)));
                                    metrics.Add(new BasicMetric((double)res, typeof(double)));
                                    metrics.Add(new BasicMetric((decimal)res, typeof(decimal)));
                                }
                            } else {
                                metrics.Add(new BasicMetric((double)res, typeof(double)));
                                metrics.Add(new BasicMetric((decimal)res, typeof(decimal)));
                            }
                        
                            result = new ParsedObject(is_surrounded_by_quotes, metrics.ToArray());
                            return true;
                        }
                    } else {
                        var parse_result = decimal.TryParse(
                            ConvertToString(buffer, offset, count, charBuffer),
                            NumberStyles.AllowCurrencySymbol | NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint,
                            FORMAT,
                            out var res);
                        if(parse_result) {
                            var metrics = new List<BasicMetric>(3);

                            if((double)res >= double.MinValue && (double)res <= double.MaxValue) {
                                if((decimal)(double)res != res) {
                                    metrics.Add(new BasicMetric((decimal)res, typeof(decimal)));
                                    metrics.Add(new BasicMetric((double)res, typeof(double), MatchType.Lossy));
                                    metrics.Add(new BasicMetric((float)res, typeof(float), MatchType.Lossy));
                                } else {
                                    metrics.Add(new BasicMetric((double)res, typeof(double)));
                                    metrics.Add(new BasicMetric((decimal)res, typeof(decimal)));
                                    metrics.Add(new BasicMetric((float)res, typeof(float), (decimal)(float)res == res ? MatchType.Lossless : MatchType.Lossy));
                                }
                            } else
                                metrics.Add(new BasicMetric((decimal)res, typeof(decimal)));
                        
                            result = new ParsedObject(is_surrounded_by_quotes, metrics.ToArray());
                            return true;
                        }
                    }
                }
            }
            result = new ParsedObject();
            return false;
        }
        #endregion
        #region private static TryParseUInt8()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static bool TryParseUInt8(byte[] buffer, int offset, int count, out byte result) {
            bool is_error = false;
            result = 0;
            int res = 0;
            switch(count) {
                case 3: res += ReadDigit(buffer[offset + count - 3], ref is_error) * 100; goto case 2;
                case 2: res += ReadDigit(buffer[offset + count - 2], ref is_error) * 10;  goto case 1;
                case 1: res += ReadDigit(buffer[offset + count - 1], ref is_error) * 1;   break;
                default:
                    return false;
            }
            if(res > byte.MaxValue)
                return false;
            result = unchecked((byte)res);
            return !is_error;
        }
        #endregion
        #region private static TryParseUInt16()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static bool TryParseUInt16(byte[] buffer, int offset, int count, out ushort result) {
            bool is_error = false;
            result = 0;
            int res = 0;
            switch(count) {
                case 5: res += ReadDigit(buffer[offset + count - 5], ref is_error) * 10000; goto case 4;
                case 4: res += ReadDigit(buffer[offset + count - 4], ref is_error) * 1000;  goto case 3;
                case 3: res += ReadDigit(buffer[offset + count - 3], ref is_error) * 100;   goto case 2;
                case 2: res += ReadDigit(buffer[offset + count - 2], ref is_error) * 10;    goto case 1;
                case 1: res += ReadDigit(buffer[offset + count - 1], ref is_error) * 1;     break;
                default:
                    return false;
            }
            if(res > ushort.MaxValue)
                return false;
            result = unchecked((ushort)res);
            return !is_error;
        }
        #endregion
        #region private static TryParseUInt32()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static bool TryParseUInt32(byte[] buffer, int offset, int count, out uint result) {
            bool is_error = false;
            result = 0;
            switch(count) {
                //case 10: result += unchecked((uint)ReadDigit(buffer[offset + count - 10], ref is_error)) * 1000000000; goto case 9;
                case 9:  result += unchecked((uint)ReadDigit(buffer[offset + count - 9],  ref is_error)) * 100000000;  goto case 8;
                case 8:  result += unchecked((uint)ReadDigit(buffer[offset + count - 8],  ref is_error)) * 10000000;   goto case 7;
                case 7:  result += unchecked((uint)ReadDigit(buffer[offset + count - 7],  ref is_error)) * 1000000;    goto case 6;
                case 6:  result += unchecked((uint)ReadDigit(buffer[offset + count - 6],  ref is_error)) * 100000;     goto case 5;
                case 5:  result += unchecked((uint)ReadDigit(buffer[offset + count - 5],  ref is_error)) * 10000;      goto case 4;
                case 4:  result += unchecked((uint)ReadDigit(buffer[offset + count - 4],  ref is_error)) * 1000;       goto case 3;
                case 3:  result += unchecked((uint)ReadDigit(buffer[offset + count - 3],  ref is_error)) * 100;        goto case 2;
                case 2:  result += unchecked((uint)ReadDigit(buffer[offset + count - 2],  ref is_error)) * 10;         goto case 1;
                case 1:  result += unchecked((uint)ReadDigit(buffer[offset + count - 1],  ref is_error)) * 1;          break;
                case 10:
                    return TryParseUInt32Rare(buffer, offset, out result);
                default:
                    return false;
            }
            return !is_error;
        }
        [MethodImpl(NoInlining)]
        private static bool TryParseUInt32Rare(byte[] buffer, int offset, out uint result) {
            bool is_error = false;
            result = 0;

            var first_digit = ReadDigit(buffer[offset], ref is_error);
            if(first_digit > 4 || is_error)
                return false;
            if(!TryParseUInt32(buffer, offset + 1, 9, out var res))
                return false;
            if(first_digit == 4) {
                if(res > 294967295)
                    return false;
                result = 4000000000 + res;
                return true;
            }

            result = (unchecked((uint)first_digit) * 1000000000) + res;
            return true;
        }
        #endregion
        #region private static TryParseUInt64()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static bool TryParseUInt64(byte[] buffer, int offset, int count, out ulong result) {
            bool is_error = false;
            result = 0;
            switch(count) {
                //case 20: result += unchecked((ulong)ReadDigit(buffer[offset + count - 20], ref is_error)) * 10000000000000000000; goto case 19;
                case 19: result += unchecked((ulong)ReadDigit(buffer[offset + count - 19], ref is_error)) * 1000000000000000000;  goto case 18;
                case 18: result += unchecked((ulong)ReadDigit(buffer[offset + count - 18], ref is_error)) * 100000000000000000;   goto case 17;
                case 17: result += unchecked((ulong)ReadDigit(buffer[offset + count - 17], ref is_error)) * 10000000000000000;    goto case 16;
                case 16: result += unchecked((ulong)ReadDigit(buffer[offset + count - 16], ref is_error)) * 1000000000000000;     goto case 15;
                case 15: result += unchecked((ulong)ReadDigit(buffer[offset + count - 15], ref is_error)) * 100000000000000;      goto case 14;
                case 14: result += unchecked((ulong)ReadDigit(buffer[offset + count - 14], ref is_error)) * 10000000000000;       goto case 13;
                case 13: result += unchecked((ulong)ReadDigit(buffer[offset + count - 13], ref is_error)) * 1000000000000;        goto case 12;
                case 12: result += unchecked((ulong)ReadDigit(buffer[offset + count - 12], ref is_error)) * 100000000000;         goto case 11;
                case 11: result += unchecked((ulong)ReadDigit(buffer[offset + count - 11], ref is_error)) * 10000000000;          goto case 10;
                case 10: result += unchecked((ulong)ReadDigit(buffer[offset + count - 10], ref is_error)) * 1000000000;           goto case 9;
                case 9:  result += unchecked((ulong)ReadDigit(buffer[offset + count - 9],  ref is_error)) * 100000000;            goto case 8;
                case 8:  result += unchecked((ulong)ReadDigit(buffer[offset + count - 8],  ref is_error)) * 10000000;             goto case 7;
                case 7:  result += unchecked((ulong)ReadDigit(buffer[offset + count - 7],  ref is_error)) * 1000000;              goto case 6;
                case 6:  result += unchecked((ulong)ReadDigit(buffer[offset + count - 6],  ref is_error)) * 100000;               goto case 5;
                case 5:  result += unchecked((ulong)ReadDigit(buffer[offset + count - 5],  ref is_error)) * 10000;                goto case 4;
                case 4:  result += unchecked((ulong)ReadDigit(buffer[offset + count - 4],  ref is_error)) * 1000;                 goto case 3;
                case 3:  result += unchecked((ulong)ReadDigit(buffer[offset + count - 3],  ref is_error)) * 100;                  goto case 2;
                case 2:  result += unchecked((ulong)ReadDigit(buffer[offset + count - 2],  ref is_error)) * 10;                   goto case 1;
                case 1:  result += unchecked((ulong)ReadDigit(buffer[offset + count - 1],  ref is_error)) * 1;                    break;
                case 20:
                    return TryParseUInt64Rare(buffer, offset, out result);
                default:
                    return false;
            }
            return !is_error;
        }
        [MethodImpl(NoInlining)]
        private static bool TryParseUInt64Rare(byte[] buffer, int offset, out ulong result) {
            bool is_error = false;
            result = 0;

            var first_digit = ReadDigit(buffer[offset], ref is_error);
            if(first_digit > 1 || is_error)
                return false;
            if(!TryParseUInt64(buffer, offset + 1, 19, out var res))
                return false;
            if(first_digit == 1) {
                if(res > 8446744073709551615)
                    return false;
                result = 10000000000000000000 + res;
                return true;
            }

            result = (unchecked((ulong)first_digit) * 10000000000000000000) + res;
            return true;
        }
        #endregion
        #region private static TryParseInt8()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static bool TryParseInt8(byte[] buffer, int offset, int count, out sbyte result) {
            bool is_error    = false;
            bool is_negative = false;
            if(buffer[offset] == '-') {
                is_negative = true;
                offset++;
                count--;
            }
            result = 0;
            int res = 0;
            switch(count) {
                case 3: res += ReadDigit(buffer[offset + count - 3], ref is_error) * 100;   goto case 2;
                case 2: res += ReadDigit(buffer[offset + count - 2], ref is_error) * 10;    goto case 1;
                case 1: res += ReadDigit(buffer[offset + count - 1], ref is_error) * 1;     break;
                default:
                    return false;
            }
            if(is_negative) {
                res = -res;
                if(res < sbyte.MinValue)
                    return false;
            } else if(res > sbyte.MaxValue)
                return false;

            result = unchecked((sbyte)res);
            return !is_error;
        }
        #endregion
        #region private static TryParseInt16()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static bool TryParseInt16(byte[] buffer, int offset, int count, out short result) {
            bool is_error    = false;
            bool is_negative = false;
            if(buffer[offset] == '-') {
                is_negative = true;
                offset++;
                count--;
            }
            result = 0;
            int res = 0;
            switch(count) {
                case 5: res += ReadDigit(buffer[offset + count - 5], ref is_error) * 10000; goto case 4;
                case 4: res += ReadDigit(buffer[offset + count - 4], ref is_error) * 1000;  goto case 3;
                case 3: res += ReadDigit(buffer[offset + count - 3], ref is_error) * 100;   goto case 2;
                case 2: res += ReadDigit(buffer[offset + count - 2], ref is_error) * 10;    goto case 1;
                case 1: res += ReadDigit(buffer[offset + count - 1], ref is_error) * 1;     break;
                default:
                    return false;
            }
            if(is_negative) {
                res = -res;
                if(res < short.MinValue)
                    return false;
            } else if(res > short.MaxValue)
                return false;

            result = unchecked((short)res);
            return !is_error;
        }
        #endregion
        #region private static TryParseInt32()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static bool TryParseInt32(byte[] buffer, int offset, int count, out int result) {
            bool is_error    = false;
            bool is_negative = false;
            if(buffer[offset] == '-') {
                is_negative = true;
                offset++;
                count--;
            }
            result = 0;
            switch(count) {
                //case 10: result += ReadDigit(buffer[offset + count - 10], ref is_error) * 1000000000; goto case 9;
                case 9:  result += ReadDigit(buffer[offset + count - 9],  ref is_error) * 100000000;  goto case 8;
                case 8:  result += ReadDigit(buffer[offset + count - 8],  ref is_error) * 10000000;   goto case 7;
                case 7:  result += ReadDigit(buffer[offset + count - 7],  ref is_error) * 1000000;    goto case 6;
                case 6:  result += ReadDigit(buffer[offset + count - 6],  ref is_error) * 100000;     goto case 5;
                case 5:  result += ReadDigit(buffer[offset + count - 5],  ref is_error) * 10000;      goto case 4;
                case 4:  result += ReadDigit(buffer[offset + count - 4],  ref is_error) * 1000;       goto case 3;
                case 3:  result += ReadDigit(buffer[offset + count - 3],  ref is_error) * 100;        goto case 2;
                case 2:  result += ReadDigit(buffer[offset + count - 2],  ref is_error) * 10;         goto case 1;
                case 1:  result += ReadDigit(buffer[offset + count - 1],  ref is_error) * 1;          break;
                case 10:
                    return TryParseInt32Rare(buffer, offset, is_negative, out result);
                default:
                    return false;
            }
            if(is_negative)
                result = -result;
            return !is_error;
        }
        [MethodImpl(NoInlining)]
        private static bool TryParseInt32Rare(byte[] buffer, int offset, bool is_negative, out int result) {
            bool is_error = false;
            result = 0;

            var first_digit = ReadDigit(buffer[offset], ref is_error);
            if(first_digit > 2 || is_error)
                return false;
            if(!TryParseUInt32(buffer, offset + 1, 9, out var res))
                return false;
            if(first_digit == 2) {
                if(!is_negative) {
                    if(res > 147483647)
                        return false;
                    result = 2000000000 + unchecked((int)res);
                } else {
                    if(res > 147483648)
                        return false;
                    result = -2000000000 - unchecked((int)res);
                }
                return true;
            }

            result = (first_digit * 1000000000) + unchecked((int)res);
            if(is_negative)
                result = -result;
            return true;
        }
        #endregion
        #region private static TryParseInt64()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static bool TryParseInt64(byte[] buffer, int offset, int count, out long result) {
            bool is_error    = false;
            bool is_negative = false;
            if(buffer[offset] == '-') {
                is_negative = true;
                offset++;
                count--;
            }
            result = 0;
            switch(count) {
                //case 19: result += ReadDigit(buffer[offset + count - 19], ref is_error) * 1000000000000000000; goto case 18;
                case 18: result += ReadDigit(buffer[offset + count - 18], ref is_error) * 100000000000000000;  goto case 17;
                case 17: result += ReadDigit(buffer[offset + count - 17], ref is_error) * 10000000000000000;   goto case 16;
                case 16: result += ReadDigit(buffer[offset + count - 16], ref is_error) * 1000000000000000;    goto case 15;
                case 15: result += ReadDigit(buffer[offset + count - 15], ref is_error) * 100000000000000;     goto case 14;
                case 14: result += ReadDigit(buffer[offset + count - 14], ref is_error) * 10000000000000;      goto case 13;
                case 13: result += ReadDigit(buffer[offset + count - 13], ref is_error) * 1000000000000;       goto case 12;
                case 12: result += ReadDigit(buffer[offset + count - 12], ref is_error) * 100000000000;        goto case 11;
                case 11: result += ReadDigit(buffer[offset + count - 11], ref is_error) * 10000000000;         goto case 10;
                case 10: result += ReadDigit(buffer[offset + count - 10], ref is_error) * 1000000000;          goto case 9;
                case 9:  result += ReadDigit(buffer[offset + count - 9],  ref is_error) * 100000000;           goto case 8;
                case 8:  result += ReadDigit(buffer[offset + count - 8],  ref is_error) * 10000000;            goto case 7;
                case 7:  result += ReadDigit(buffer[offset + count - 7],  ref is_error) * 1000000;             goto case 6;
                case 6:  result += ReadDigit(buffer[offset + count - 6],  ref is_error) * 100000;              goto case 5;
                case 5:  result += ReadDigit(buffer[offset + count - 5],  ref is_error) * 10000;               goto case 4;
                case 4:  result += ReadDigit(buffer[offset + count - 4],  ref is_error) * 1000;                goto case 3;
                case 3:  result += ReadDigit(buffer[offset + count - 3],  ref is_error) * 100;                 goto case 2;
                case 2:  result += ReadDigit(buffer[offset + count - 2],  ref is_error) * 10;                  goto case 1;
                case 1:  result += ReadDigit(buffer[offset + count - 1],  ref is_error) * 1;                   break;
                case 19:
                    return TryParseInt64Rare(buffer, offset, is_negative, out result);
                default:
                    return false;
            }
            if(is_negative)
                result = -result;
            return !is_error;
        }
        [MethodImpl(NoInlining)]
        private static bool TryParseInt64Rare(byte[] buffer, int offset, bool is_negative, out long result) {
            bool is_error = false;
            result = 0;

            var first_digit = ReadDigit(buffer[offset], ref is_error);
            if(is_error)
                return false;
            if(!TryParseUInt64(buffer, offset + 1, 18, out var res))
                return false;
            if(first_digit == 9) {
                if(!is_negative) {
                    if(res > 223372036854775807)
                        return false;
                    result = 9000000000000000000 + unchecked((long)res);
                } else {
                    if(res > 223372036854775808)
                        return false;
                    result = -9000000000000000000 - unchecked((long)res);
                }
                return true;
            }

            result = (first_digit * 1000000000000000000) + unchecked((long)res);
            if(is_negative)
                result = -result;
            return true;
        }
        #endregion
        #region private static TryParseFloat()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        /// <param name="charBuffer">Should be >= count.</param>
        [MethodImpl(AggressiveInlining)]
        private static bool TryParseFloat(byte[] buffer, int offset, int count, char[] charBuffer, out float result) {
            // if everything is just digits, then do the quick parse
            // ie: -123.1234
            if(TryParseDecimal(buffer, offset, count, out var res2)) {
                result = (float)res2;
                return true;
            }

            // otherwise then try a full-fledged parser
            var res = float.TryParse(
                ConvertToString(buffer, offset, count, charBuffer),
                NumberStyles.AllowCurrencySymbol | NumberStyles.AllowLeadingSign | NumberStyles.AllowExponent | NumberStyles.AllowDecimalPoint,
                FORMAT,
                out result);
            return res;
        }
        #endregion
        #region private static TryParseDouble()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        /// <param name="charBuffer">Should be >= count.</param>
        [MethodImpl(AggressiveInlining)]
        private static bool TryParseDouble(byte[] buffer, int offset, int count, char[] charBuffer, out double result) {
            // if everything is just digits, then do the quick parse
            // ie: -123.1234
            if(TryParseDecimal(buffer, offset, count, out var res2)) {
                result = (double)res2;
                return true;
            }

            // otherwise then try a full-fledged parser
            var res = double.TryParse(
                ConvertToString(buffer, offset, count, charBuffer),
                NumberStyles.AllowCurrencySymbol | NumberStyles.AllowLeadingSign | NumberStyles.AllowExponent | NumberStyles.AllowDecimalPoint,
                FORMAT,
                out result);
            return res;
        }
        #endregion
        #region private static TryParseDecimal()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        /// <param name="charBuffer">Should be >= count.</param>
        private static bool TryParseDecimal(byte[] buffer, int offset, int count, char[] charBuffer, out decimal result) {
            // if everything is just digits, then do the quick parse
            // ie: -123.1234
            if(TryParseDecimal(buffer, offset, count, out result))
                return true;

            // otherwise then try a full-fledged parser
            var res = decimal.TryParse(
                ConvertToString(buffer, offset, count, charBuffer),
                NumberStyles.AllowCurrencySymbol | NumberStyles.AllowLeadingSign | NumberStyles.AllowExponent | NumberStyles.AllowDecimalPoint,
                FORMAT,
                out result);
            return res;
        }
        /// <summary>
        ///     Only supports decimals in format: -234234234234.234234234, with no spaces before/after and no other characters.
        ///     
        ///     performance note: this code is a lot faster than decimal.TryParse() especially for precision of 19- numerics and 19- fractional
        ///     however, it becomes slower vs the default parser on large values (20+ numerics and 20+ fractional).
        /// </summary>
        private static bool TryParseDecimal(byte[] buffer, int offset, int count, out decimal result) {
            result = 0;

            if(count <= 0)
                return false;

            int dot_index = -1;
            for(int i = 0; i < count; i++) {
                if(buffer[offset + i] == '.') {
                    dot_index = offset + i;
                    break;
                }
            }

            return TryParseDecimal(buffer, offset, count, dot_index, out result);
        }
        /// <summary>
        ///     Only supports decimals in format: -234234234234.234234234, with no spaces before/after and no other characters.
        ///     
        ///     performance note: this code is a lot faster than decimal.TryParse() especially for precision of 19- numerics and 19- fractional
        ///     however, it becomes slower vs the default parser on large values (20+ numerics and 20+ fractional).
        /// </summary>
        private static bool TryParseDecimal(byte[] buffer, int offset, int count, int dot_index, out decimal result) {
            result = 0;

            if(count <= 0)
                return false;

            var is_negative = buffer[offset] == '-';
            if(is_negative) {
                if(count == 1)
                    return false;
                offset++;
                count--;
            }

            int digit_count = dot_index < 0 ?
                count :
                dot_index - offset;

            // can parse 29 digits pre-dot, and ignore most of the following decimals, but can't properly interpret 30 digits
            if(digit_count >= 30)
                return false;

            // parse numeric part
            if(digit_count > 0) {
                var processed = Math.Min(digit_count, 19);
                if(!TryParseUInt64(buffer, offset + digit_count - processed, processed, out ulong numeric_part))
                    return false;

                result += numeric_part;

                // if 19+ decimals
                if(digit_count > processed) {
                    //length -= processed;
                    processed = digit_count - 19;
                    if(!TryParseUInt64(buffer, offset, processed, out numeric_part))
                        return false;
                    // note: -decimal.MinValue == decimal.MaxValue
                    if(numeric_part >= 7922816251) { // avoid double checks in 99.9% of cases
                        if(numeric_part > 7922816251 || (numeric_part == 7922816251 && result > 4264337593543950335)) {
                            result = 0;
                            return false;
                        }
                    }
                    result += (decimal)numeric_part * 10000000000000000000m;
                }
            }

            // parse fractional part
            if(dot_index >= 0 && count > dot_index - offset + 1) {
                var decimal_count = count - (dot_index - offset) - 1;

                var processed = Math.Min(decimal_count, 19);
                if(!TryParseUInt64(buffer, dot_index + 1, processed, out ulong decimals_part))
                    return false;

                decimal divisor;
                switch(processed) {
                    case 1:  divisor = 10;                   break;
                    case 2:  divisor = 100;                  break;
                    case 3:  divisor = 1000;                 break;
                    case 4:  divisor = 10000;                break;
                    case 5:  divisor = 100000;               break;
                    case 6:  divisor = 1000000;              break;
                    case 7:  divisor = 10000000;             break;
                    case 8:  divisor = 100000000;            break;
                    case 9:  divisor = 1000000000;           break;
                    case 10: divisor = 10000000000;          break;
                    case 11: divisor = 100000000000;         break;
                    case 12: divisor = 1000000000000;        break;
                    case 13: divisor = 10000000000000;       break;
                    case 14: divisor = 100000000000000;      break;
                    case 15: divisor = 1000000000000000;     break;
                    case 16: divisor = 10000000000000000;    break;
                    case 17: divisor = 100000000000000000;   break;
                    case 18: divisor = 1000000000000000000;  break;
                    case 19: divisor = 10000000000000000000; break;
                    default:
                        return false;
                }
                result += (decimal)decimals_part / divisor;

                // if 20+ decimals
                if(decimal_count > processed) {
                    processed = Math.Min(decimal_count - processed, 9);
                    if(!TryParseUInt64(buffer, dot_index + 19 + 1, processed, out decimals_part))
                        return false;

                    switch(processed) {
                        case 1:  divisor = 100000000000000000000m;          break;
                        case 2:  divisor = 1000000000000000000000m;         break;
                        case 3:  divisor = 10000000000000000000000m;        break;
                        case 4:  divisor = 100000000000000000000000m;       break;
                        case 5:  divisor = 1000000000000000000000000m;      break;
                        case 6:  divisor = 10000000000000000000000000m;     break;
                        case 7:  divisor = 100000000000000000000000000m;    break;
                        case 8:  divisor = 1000000000000000000000000000m;   break;
                        case 9:  divisor = 10000000000000000000000000000m;  break;
                        //case 10: divisor = 100000000000000000000000000000m; break;
                        //case 11: divisor = 1000000000000000000000000000000m; break;
                        default:
                            return false;
                    }

                    result += (decimal)decimals_part / divisor;
                }
            }

            if(is_negative)
                result = -result;

            return true;
        }
        #endregion
        #region private static TryParseDateTime()
        /// <summary>
        ///     Parses a datetime in sortable format
        ///     2008-04-10 06:30:00.1234567
        ///     2008/04/10 06:30:00.1234567
        ///     20080410 06:30:00.1234567
        ///     20080410
        /// </summary>
        private static bool TryParseDateTime(byte[] buffer, ref int offset, out DateTime result) {
            if(!TryParseUInt32(buffer, offset, 4, out var year2)) {
                result = DateTime.MinValue;
                return false;
            }
            int year = unchecked((int)year2);
            offset  += 4;

            var b = buffer[offset];
            if(b == '-' || b == '/')
                offset++;

            if(!TryParseUInt32(buffer, offset, 2, out var month2)) {
                result = DateTime.MinValue;
                return false;
            }
            int month = unchecked((int)month2);
            offset   += 2;

            b = buffer[offset];
            if(b == '-' || b == '/')
                offset++;

            if(!TryParseUInt32(buffer, offset, 2, out var day2)) {
                result = DateTime.MinValue;
                return false;
            }
            int day = unchecked((int)day2);
            offset += 2;

            if(!TryParseTimeSpan(buffer, ref offset, false, out var time)) {
                result = default;
                return false;
            }

            result = new DateTime(new DateTime(year, month, day).Ticks + time.Ticks, DateTimeKind.Utc);
            return true;
        }
        #endregion
        #region private static TryParseTimeSpan()
        /// <summary>
        ///     Parses a timespan in constant format
        ///     00:00:00, 3.17:25:30.5000000
        /// </summary>
        private static bool TryParseTimeSpan(byte[] buffer, ref int offset, bool readDays, out TimeSpan result) {
            var b = buffer[offset];
            if(b == ' ' || b == 'T') // in case of 9999-12-20T00:00:00"
                b = buffer[++offset];

            bool is_negative = false;

            if(b == '-') {
                is_negative = true;
                b = buffer[++offset];
            }

            if(b > '9' || b < '0') {
                result = TimeSpan.Zero;
                return true;
            }

            int day = 0;
            if(readDays) {
                var digits = CountDigits(buffer, offset);
                if(buffer[offset + digits] == '.') {
                    if(!TryParseUInt32(buffer, offset, digits, out var day2)) {
                        result = TimeSpan.Zero;
                        return false;
                    }
                    day = unchecked((int)day2);
                }
            }

            if(!TryParseUInt32(buffer, offset, 2, out var hour2) || hour2 >= 24) {
                result = TimeSpan.Zero;
                return false;
            }
            int hour = unchecked((int)hour2);
            offset  += 2;

            b = buffer[offset];
            if(b == ':')
                offset++;

            if(!TryParseUInt32(buffer, offset, 2, out var minute2) || minute2 >= 60) {
                result = TimeSpan.Zero;
                return false;
            }
            int minute = unchecked((int)minute2);
            offset    += 2;

            b = buffer[offset];
            if(b == ':')
                offset++;

            if(!TryParseUInt32(buffer, offset, 2, out var second2) || second2 >= 60) {
                result = TimeSpan.Zero;
                return false;
            }
            long millisecond = 0;
            int second       = unchecked((int)second2);
            offset          += 2;

            b = buffer[offset];
            if(b == '.') {
                offset++;

                int digits = Math.Min(CountDigits(buffer, offset), 7);

                if(!TryParseUInt32(buffer, offset, digits, out var millisecond2)) {
                    result = TimeSpan.Zero;
                    return false;
                }
                millisecond = unchecked((int)millisecond2);
                offset     += digits;

                switch(digits) {
                    case 3: millisecond = (millisecond * 1)   * TimeSpan.TicksPerMillisecond; break;
                    case 2: millisecond = (millisecond * 10)  * TimeSpan.TicksPerMillisecond; break;
                    case 1: millisecond = (millisecond * 100) * TimeSpan.TicksPerMillisecond; break;

                    //case 7: millisecond *= 1; break;
                    case 6: millisecond *= 10;   break;
                    case 5: millisecond *= 100;  break;
                    case 4: millisecond *= 1000; break;
                }
            }

            long ticks =
                day    * TimeSpan.TicksPerDay +
                hour   * TimeSpan.TicksPerHour +
                minute * TimeSpan.TicksPerMinute +
                second * TimeSpan.TicksPerSecond +
                millisecond;

            if(is_negative)
                ticks = -ticks;

            result = new TimeSpan(ticks);
            return true;
        }
        [MethodImpl(AggressiveInlining)]
        private static int CountDigits(byte[] buffer, int offset) {
            int digits = 0;

            while(offset < buffer.Length) {
                var b = buffer[offset++];
                if(b <= '9' && b >= '0')
                    digits++;
                else
                    break;
            }
            return digits;
        }
        #endregion
        #region private static TryParseGuid()
        /// <summary>
        ///     Parses a Guid.
        ///     
        ///     Supported formats:
        ///     {00000000-0000-0000-0000-000000000000}
        ///     (00000000-0000-0000-0000-000000000000)
        ///     00000000-0000-0000-0000-000000000000
        ///     00000000000000000000000000000000
        ///     {00000000000000000000000000000000}
        ///     (00000000000000000000000000000000)
        /// </summary>
        private static bool TryParseGuid(byte[] buffer, ref int offset, int count, out Guid result) {
            if(count < 32) {
                result = Guid.Empty;
                return false;
            }

            byte brackets = (byte)'\0';

            int index = offset;
            var b = buffer[index];
            if(b == '(' || b == '{') {
                brackets = b == '(' ? (byte)')' : (byte)'}';
                index++;
                count--;

                if(count < 33) {
                    result = Guid.Empty;
                    return false;
                }
            }

            // part 1
            var part1 = ReadHex8(buffer, index, count);
            if(part1 < 0) {
                result = Guid.Empty;
                return false;
            }
            index += 8;
            count -= 8;

            bool has_dashes = false;
            b = buffer[index];
            if(b == '-') {
                has_dashes = true;
                index++;
                count--;

                if(count < 27 + (brackets != '\0' ? 1 : 0)) {
                    result = Guid.Empty;
                    return false;
                }
            }

            // part 2
            var part2 = ReadHex4(buffer, index, count);
            if(part2 < 0) {
                result = Guid.Empty;
                return false;
            }
            index += 4;
            count -= 4;

            if(has_dashes) {
                b = buffer[index];
                if(b == '-') {
                    index++;
                    count--;
                } else {
                    result = Guid.Empty;
                    return false;
                }
            }

            // part 3
            var part3 = ReadHex4(buffer, index, count);
            if(part3 < 0) {
                result = Guid.Empty;
                return false;
            }
            index += 4;
            count -= 4;

            if(has_dashes) {
                b = buffer[index];
                if(b == '-') {
                    index++;
                    count--;
                } else {
                    result = Guid.Empty;
                    return false;
                }
            }

            // part 4-5
            var part4 = ReadHex2(buffer, index + 0, count - 0);
            var part5 = ReadHex2(buffer, index + 2, count - 2);
            if(part4 < 0 || part5 < 0) {
                result = Guid.Empty;
                return false;
            }
            index += 4;
            count -= 4;

            if(has_dashes) {
                b = buffer[index];
                if(b == '-') {
                    index++;
                    count--;
                } else {
                    result = Guid.Empty;
                    return false;
                }
            }

            // part 6-11
            var part6  = ReadHex2(buffer, index + 0,  count - 0);
            var part7  = ReadHex2(buffer, index + 2,  count - 2);
            var part8  = ReadHex2(buffer, index + 4,  count - 4);
            var part9  = ReadHex2(buffer, index + 6,  count - 6);
            var part10 = ReadHex2(buffer, index + 8,  count - 8);
            var part11 = ReadHex2(buffer, index + 10, count - 10);
            if(part6 < 0 || part7 < 0 || part8 < 0 || part9 < 0 || part10 < 0 || part11 < 0) {
                result = Guid.Empty;
                return false;
            }
            index += 12;
            //count -= 12;

            if(brackets != '\0') {
                b = buffer[index];
                if(b == brackets) {
                    index++;
                    //count--;
                } else {
                    result = Guid.Empty;
                    return false;
                }
            }

            result = new Guid(
                unchecked((uint)part1),
                unchecked((ushort)part2),
                unchecked((ushort)part3),
                unchecked((byte)part4),
                unchecked((byte)part5),
                unchecked((byte)part6),
                unchecked((byte)part7),
                unchecked((byte)part8),
                unchecked((byte)part9),
                unchecked((byte)part10),
                unchecked((byte)part11));
            offset = index;
            return true;
        }
        /// <summary>
        ///     Reads 2 HEX values and returns a byte.
        ///     Returns -1 if invalid.
        /// </summary>
        private static int ReadHex2(byte[] buffer, int offset, int count) {
            if(count < 2)
                return -1;

            bool is_error = false;
            var res =
                (ReadHex(buffer[offset + 0], ref is_error) << 4) |
                (ReadHex(buffer[offset + 1], ref is_error) << 0);
            return is_error ? -1 : res;
        }
        /// <summary>
        ///     Reads 4 HEX values and returns a ushort.
        ///     Returns -1 if invalid.
        /// </summary>
        private static int ReadHex4(byte[] buffer, int offset, int count) {
            if(count < 4)
                return -1;

            bool is_error = false;
            var res =
                (ReadHex(buffer[offset + 0], ref is_error) << 12) |
                (ReadHex(buffer[offset + 1], ref is_error) << 8) |
                (ReadHex(buffer[offset + 2], ref is_error) << 4) |
                (ReadHex(buffer[offset + 3], ref is_error) << 0);
            return is_error ? -1 : res;
        }
        /// <summary>
        ///     Reads 8 HEX values and returns a int.
        ///     Returns -1 if invalid.
        /// </summary>
        private static long ReadHex8(byte[] buffer, int offset, int count) {
            if(count < 8)
                return -1;

            bool is_error = false;
            var res =
                ((long)ReadHex(buffer[offset + 0], ref is_error) << 28) |
                ((long)ReadHex(buffer[offset + 1], ref is_error) << 24) |
                ((long)ReadHex(buffer[offset + 2], ref is_error) << 20) |
                ((long)ReadHex(buffer[offset + 3], ref is_error) << 16) |
                ((long)ReadHex(buffer[offset + 4], ref is_error) << 12) |
                ((long)ReadHex(buffer[offset + 5], ref is_error) << 8) |
                ((long)ReadHex(buffer[offset + 6], ref is_error) << 4) |
                ((long)ReadHex(buffer[offset + 7], ref is_error) << 0);
            return is_error ? -1 : res;
        }
        #endregion
        #region private static TryParseBool()
        /// <summary>
        ///     Parses a boolean.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static bool TryParseBool(byte[] buffer, int offset, int count, out bool result) {
            if(count == 1) {
                var b = buffer[offset];
                if(b == '0' || b == 'f' || b == 'F') {
                    result = false;
                    return true;
                }
                if(b == '1' || b == 't' || b == 'T') { // some postgres versions uses t/f
                    result = true;
                    return true;
                }
            } else if(count == 4 || count == 5) {
                var b1 = buffer[offset + 0];
                var b2 = buffer[offset + 1];
                var b3 = buffer[offset + 2];
                var b4 = buffer[offset + 3];

                if(count == 5) {
                    var b5 = buffer[offset + 4];
                    if((b1 == 'f' || b1 == 'F') && (b2 == 'a' || b2 == 'A') && (b3 == 'l' || b3 == 'L') && (b4 == 's' || b4 == 'S') && (b5 == 'e' || b5 == 'E')) {
                        result = false;
                        return true;
                    }
                } else if((b1 == 't' || b1 == 'T') && (b2 == 'r' || b2 == 'R') && (b3 == 'u' || b3 == 'U') && (b4 == 'e' || b4 == 'E')) {
                    result = true;
                    return true;
                }   
            }
            result = false;
            return false;
        }
        #endregion
        #region private static TryParseChar()
        /// <summary>
        ///     Reads a byte and returns a char.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static bool TryParseChar(byte[] buffer, int offset, int count, out char result) {
            if(count == 1) {
                result = (char)buffer[offset];
                return true;
            }
            result = '\0';
            return false;
        }
        #endregion

        #region private static ReadDigit()
        [MethodImpl(AggressiveInlining)]
        private static int ReadDigit(byte value, ref bool is_error) {
            if(value > '9' || value < '0')
                is_error = true;
            return value - '0';
        }
        #endregion
        #region private static ReadHex()
        [MethodImpl(AggressiveInlining)]
        private static int ReadHex(byte c, ref bool is_error) {
            if(c <= '9' && c >= '0')
                return c - '0';
            if(c <= 'F' && c >= 'A')
                return c - 'A' + 10;
            if(c <= 'f' && c >= 'a')
                return c - 'a' + 10;
            is_error = true;
            return 0;
        }
        #endregion
        #region Dispose()
        public void Dispose() {
            if(m_closeStreamOnDispose)
                m_stream.Dispose();
        }
        #endregion


        /// <summary>
        ///     A value stored within a CSV file.
        ///     Note that there are multiple implicit converters to read this value.
        ///     Note that all implicit cast will return the default value on empty/improperly formatted data.
        ///     Use TryReadXXXXX() / TryGetValue() if you wish to throw FormatException()
        /// </summary>
        public struct CsvValue {
            internal int RowBufferIndex;  // this.Owner.m_rowBuffers[this.RowBufferIndex] where data starts
            internal int BufferIndex;     // the position data starts within this.Owner.m_rowBuffers[this.RowBufferIndex]. This includes the quote if double quoted
            internal int Length;          // total length
            internal bool IsQuoted;       // if true, means ' or " is around the value (ie: "text")
            internal bool IsDoubleQuoted; // if true, means ' or " will need to be double quoted parsed. False if there's quotes but no unquoting needed (ie: "text")
            internal CsvStreamReader Owner;

            #region private readonly struct CsvValueResult
            private readonly struct CsvValueResult {
                public readonly bool Success;
                public readonly object Value;
                public CsvValueResult(bool success, object value) {
                    this.Success = success;
                    this.Value   = value;
                }
            }
            #endregion
            private static readonly Dictionary<Type, Func<CsvValue, CsvValueResult>> m_tryReads;

            #region constructors
            static CsvValue() {
                m_tryReads = new Dictionary<Type, Func<CsvValue, CsvValueResult>> {
                    { typeof(string),    o => new CsvValueResult(o.TryReadString(out var res), res) },
                    { typeof(byte[]),    o => new CsvValueResult(o.TryReadByteArray(out var res), res) },
                    { typeof(Stream),    o => new CsvValueResult(o.TryReadStream(out var res), res) },
                    { typeof(bool),      o => new CsvValueResult(o.TryReadBool(out var res), res) },
                    { typeof(char),      o => new CsvValueResult(o.TryReadChar(out var res), res) },
                    { typeof(sbyte),     o => new CsvValueResult(o.TryReadInt8(out var res), res) },
                    { typeof(short),     o => new CsvValueResult(o.TryReadInt16(out var res), res) },
                    { typeof(int),       o => new CsvValueResult(o.TryReadInt32(out var res), res) },
                    { typeof(long),      o => new CsvValueResult(o.TryReadInt64(out var res), res) },
                    { typeof(byte),      o => new CsvValueResult(o.TryReadUInt8(out var res), res) },
                    { typeof(ushort),    o => new CsvValueResult(o.TryReadUInt16(out var res), res) },
                    { typeof(uint),      o => new CsvValueResult(o.TryReadUInt32(out var res), res) },
                    { typeof(ulong),     o => new CsvValueResult(o.TryReadUInt64(out var res), res) },
                    { typeof(float),     o => new CsvValueResult(o.TryReadFloat(out var res), res) },
                    { typeof(double),    o => new CsvValueResult(o.TryReadDouble(out var res), res) },
                    { typeof(decimal),   o => new CsvValueResult(o.TryReadDecimal(out var res), res) },
                    { typeof(Guid),      o => new CsvValueResult(o.TryReadGuid(out var res), res) },
                    { typeof(DateTime),  o => new CsvValueResult(o.TryReadDateTime(out var res), res) },
                    { typeof(TimeSpan),  o => new CsvValueResult(o.TryReadTimeSpan(out var res), res) },
                    { typeof(bool?),     o => new CsvValueResult(o.TryReadNullableBool(out var res), res) },
                    { typeof(char?),     o => new CsvValueResult(o.TryReadNullableChar(out var res), res) },
                    { typeof(sbyte?),    o => new CsvValueResult(o.TryReadNullableInt8(out var res), res) },
                    { typeof(short?),    o => new CsvValueResult(o.TryReadNullableInt16(out var res), res) },
                    { typeof(int?),      o => new CsvValueResult(o.TryReadNullableInt32(out var res), res) },
                    { typeof(long?),     o => new CsvValueResult(o.TryReadNullableInt64(out var res), res) },
                    { typeof(byte?),     o => new CsvValueResult(o.TryReadNullableUInt8(out var res), res) },
                    { typeof(ushort?),   o => new CsvValueResult(o.TryReadNullableUInt16(out var res), res) },
                    { typeof(uint?),     o => new CsvValueResult(o.TryReadNullableUInt32(out var res), res) },
                    { typeof(ulong?),    o => new CsvValueResult(o.TryReadNullableUInt64(out var res), res) },
                    { typeof(float?),    o => new CsvValueResult(o.TryReadNullableFloat(out var res), res) },
                    { typeof(double?),   o => new CsvValueResult(o.TryReadNullableDouble(out var res), res) },
                    { typeof(decimal?),  o => new CsvValueResult(o.TryReadNullableDecimal(out var res), res) },
                    { typeof(Guid?),     o => new CsvValueResult(o.TryReadNullableGuid(out var res), res) },
                    { typeof(DateTime?), o => new CsvValueResult(o.TryReadNullableDateTime(out var res), res) },
                    { typeof(TimeSpan?), o => new CsvValueResult(o.TryReadNullableTimeSpan(out var res), res) },
                };
            }
            #endregion

            /// <summary>
            ///     If true, indicates the CSV value contains nothing, ie: ',,'
            /// </summary>
            public bool IsEmpty => this.Length == 0;

            #region GetValue()
            /// <summary>
            ///     Returns the value under the best-guessed value type.
            ///     This is a lot slower than directly casting with the implicit converters since it attempts to determine the data type.
            /// </summary>
            public object GetValue() {
                if(this.IsEmpty)
                    return null;

                // if theres a ' or " at the start (and may have whitespaces before it)
                // or if the value is long, then assume its a string
                if(this.IsDoubleQuoted || this.Length > CSVVALUE_SIZE) {
                    TryReadValueString(in this, out var res);
                    return res;
                }

                // handles trim()
                var raw = ReadValue(in this);

                return ParseObject(raw.Data, raw.Index, raw.Length, this.Owner.m_charBuffer);
            }
            #endregion
            #region GetValues()
            public struct Values {
                public bool IsEmpty;  // contains no data (ie ',,')
                public bool IsQuoted; // indicates the data is surrounded by quotation marks
            
                public TypeMetric[] Metrics; // null if the value was explicitly 'null'

                public Values(bool is_quoted, IEnumerable<TypeMetric> metrics) {
                    this.IsEmpty  = false;
                    this.IsQuoted = is_quoted;
                    this.Metrics  = metrics?.ToArray();
                }
            }
            public struct TypeMetric {
                public byte Index; // nth type
                public Type Type;
                public object Value;
                public bool IsLossless;

                public TypeMetric(object value, byte index, Type type) : this(value, index, type, true) { }
                public TypeMetric(object value, byte index, Type type, bool isLossless) {
                    this.Index      = index;
                    this.Value      = value;
                    this.Type       = type;
                    this.IsLossless = isLossless;
                }
            }
            /// <summary>
            ///     Returns the possible values that this could be.
            ///     This is a lot slower than directly casting with the implicit converters since it attempts to determine the data types.
            ///     Results are returned in order of likelyness.
            /// </summary>
            public Values GetValues() {
                if(this.IsEmpty)
                    return new Values(this.IsQuoted, null) { IsEmpty = true };

                // if theres a ' or " at the start (and may have whitespaces before it)
                // or if the value is long, then assume its a string
                if(this.IsDoubleQuoted || this.Length > CSVVALUE_SIZE) {
                    TryReadValueString(in this, out var x);
                    return new Values(true, new[] { new TypeMetric(x, 0, typeof(string)) });
                }

                // handles trim()
                var raw = ReadValue(in this);

                var res = ParseObjectAndMetrics(raw.Data, raw.Index, raw.Length, this.Owner.m_charBuffer, this.IsQuoted);
                return new Values(this.IsQuoted, res.Metrics?.Select((m, i) => new TypeMetric(m.Value, (byte)i, m.Type, m.Match == MatchType.Lossless)));
            }
            #endregion
            #region TryGetValue()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            ///     Note that this is slightly slower than calling the TryRead() methods directly.
            /// </summary>
            public bool TryGetValue(Type type, out object value) {
                if(!m_tryReads.TryGetValue(type, out var _func))
                    throw new NotSupportedException($"Type {type} is not supported.");

                var res = _func(this);
                value = res.Value;
                return res.Success;
            }
            /// <summary>
            ///     Tries to interpret the value in the given type.
            ///     Note that this is slightly slower than calling the TryRead() methods directly.
            /// </summary>
            public bool TryGetValue<T>(out T value) {
                if(!m_tryReads.TryGetValue(typeof(T), out var _func))
                    throw new NotSupportedException($"Type {typeof(T)} is not supported.");

                var res = _func(this);
                value = (T)res.Value;
                return res.Success;
            }
            #endregion
            #region TryGetHexEncodedValue()
            /// <summary>
            ///     Reads hex-encoded byte[].
            /// </summary>
            public bool TryGetHexEncodedValue(out byte[] result) {
                return TryReadValueHex(in this, out result);
            }
            #endregion

            #region TryReadBool()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadBool(out bool value) {
                if(this.IsEmpty) {
                    value = default;
                    return false;
                }
                var raw = ReadValue(in this);
                return TryParseBool(raw.Data, raw.Index, raw.Length, out value);
            }
            #endregion
            #region TryReadChar()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadChar(out char value) {
                if(this.IsEmpty) {
                    value = default;
                    return false;
                }
                var raw = ReadValue(in this);
                return TryParseChar(raw.Data, raw.Index, raw.Length, out value);
            }
            #endregion
            #region TryReadInt8()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadInt8(out sbyte value) {
                if(this.IsEmpty) {
                    value = default;
                    return false;
                }
                var raw = ReadValue(in this);
                return TryParseInt8(raw.Data, raw.Index, raw.Length, out value);
            }
            #endregion
            #region TryReadInt16()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadInt16(out short value) {
                if(this.IsEmpty) {
                    value = default;
                    return false;
                }
                var raw = ReadValue(in this);
                return TryParseInt16(raw.Data, raw.Index, raw.Length, out value);
            }
            #endregion
            #region TryReadInt32()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadInt32(out int value) {
                if(this.IsEmpty) {
                    value = default;
                    return false;
                }
                var raw = ReadValue(in this);
                return TryParseInt32(raw.Data, raw.Index, raw.Length, out value);
            }
            #endregion
            #region TryReadInt64()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadInt64(out long value) {
                if(this.IsEmpty) {
                    value = default;
                    return false;
                }
                var raw = ReadValue(in this);
                return TryParseInt64(raw.Data, raw.Index, raw.Length, out value);
            }
            #endregion
            #region TryReadUInt8()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadUInt8(out byte value) {
                if(this.IsEmpty) {
                    value = default;
                    return false;
                }
                var raw = ReadValue(in this);
                return TryParseUInt8(raw.Data, raw.Index, raw.Length, out value);
            }
            #endregion
            #region TryReadUInt16()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadUInt16(out ushort value) {
                if(this.IsEmpty) {
                    value = default;
                    return false;
                }
                var raw = ReadValue(in this);
                return TryParseUInt16(raw.Data, raw.Index, raw.Length, out value);
            }
            #endregion
            #region TryReadUInt32()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadUInt32(out uint value) {
                if(this.IsEmpty) {
                    value = default;
                    return false;
                }
                var raw = ReadValue(in this);
                return TryParseUInt32(raw.Data, raw.Index, raw.Length, out value);
            }
            #endregion
            #region TryReadUInt64()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadUInt64(out ulong value) {
                if(this.IsEmpty) {
                    value = default;
                    return false;
                }
                var raw = ReadValue(in this);
                return TryParseUInt64(raw.Data, raw.Index, raw.Length, out value);
            }
            #endregion
            #region TryReadGuid()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadGuid(out Guid value) {
                if(this.IsEmpty) {
                    value = default;
                    return false;
                }
                var raw = ReadValue(in this);
                int index = raw.Index;
                return TryParseGuid(raw.Data, ref index, raw.Length, out value);
            }
            #endregion
            #region TryReadDateTime()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadDateTime(out DateTime value) {
                if(this.IsEmpty) {
                    value = default;
                    return false;
                }
                var raw = ReadValue(in this);
                int index = raw.Index;
                return TryParseDateTime(raw.Data, ref index, out value);
            }
            #endregion
            #region TryReadTimeSpan()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadTimeSpan(out TimeSpan value) {
                if(this.IsEmpty) {
                    value = default;
                    return false;
                }
                var raw = ReadValue(in this);
                int index = raw.Index;
                return TryParseTimeSpan(raw.Data, ref index, true, out value);
            }
            #endregion
            #region TryReadFloat()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadFloat(out float value) {
                if(this.IsEmpty) {
                    value = default;
                    return false;
                }
                var raw = ReadValue(in this);
                return TryParseFloat(raw.Data, raw.Index, raw.Length, this.Owner.m_charBuffer, out value);
            }
            #endregion
            #region TryReadDouble()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadDouble(out double value) {
                if(this.IsEmpty) {
                    value = default;
                    return false;
                }
                var raw = ReadValue(in this);
                return TryParseDouble(raw.Data, raw.Index, raw.Length, this.Owner.m_charBuffer, out value);
            }
            #endregion
            #region TryReadDecimal()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadDecimal(out decimal value) {
                if(this.IsEmpty) {
                    value = default;
                    return false;
                }
                var raw = ReadValue(in this);
                return TryParseDecimal(raw.Data, raw.Index, raw.Length, this.Owner.m_charBuffer, out value);
            }
            #endregion

            #region TryReadString()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadString(out string value) {
                return TryReadValueString(in this, out value);
            }
            #endregion
            #region TryReadByteArray()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadByteArray(out byte[] value) {
                if(!TryReadValueVariableLength(in this, out var raw)) {
                    value = null;
                    return false;
                }

                var data = raw.Data;
                if(raw.Length < data.Length)
                    Array.Resize(ref data, raw.Length);

                value = data;
                return true;
            }
            #endregion
            #region TryReadStream()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadStream(out Stream value) {
                if(!this.TryReadByteArray(out var raw)) {
                    value = null;
                    return false;
                }
                value = new MemoryStream(raw);
                return true;
            }
            #endregion

            #region TryReadNullableBool()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadNullableBool(out bool? value) {
                if(this.IsEmpty) {
                    value = default;
                    return true;
                }
                var raw = ReadValue(in this);
                var res = TryParseBool(raw.Data, raw.Index, raw.Length, out bool val);
                value = res ? val : (bool?)null;
                return res;
            }
            #endregion
            #region TryReadNullableChar()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadNullableChar(out char? value) {
                if(this.IsEmpty) {
                    value = default;
                    return true;
                }
                var raw = ReadValue(in this);
                var res = TryParseChar(raw.Data, raw.Index, raw.Length, out char val);
                value = res ? val : (char?)null;
                return res;
            }
            #endregion
            #region TryReadNullableInt8()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadNullableInt8(out sbyte? value) {
                if(this.IsEmpty) {
                    value = default;
                    return true;
                }
                var raw = ReadValue(in this);
                var res = TryParseInt8(raw.Data, raw.Index, raw.Length, out sbyte val);
                value = res ? val : (sbyte?)null;
                return res;
            }
            #endregion
            #region TryReadNullableInt16()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadNullableInt16(out short? value) {
                if(this.IsEmpty) {
                    value = default;
                    return true;
                }
                var raw = ReadValue(in this);
                var res = TryParseInt16(raw.Data, raw.Index, raw.Length, out short val);
                value = res ? val : (short?)null;
                return res;
            }
            #endregion
            #region TryReadNullableInt32()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadNullableInt32(out int? value) {
                if(this.IsEmpty) {
                    value = default;
                    return true;
                }
                var raw = ReadValue(in this);
                var res = TryParseInt32(raw.Data, raw.Index, raw.Length, out int val);
                value = res ? val : (int?)null;
                return res;
            }
            #endregion
            #region TryReadNullableInt64()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadNullableInt64(out long? value) {
                if(this.IsEmpty) {
                    value = default;
                    return true;
                }
                var raw = ReadValue(in this);
                var res = TryParseInt64(raw.Data, raw.Index, raw.Length, out long val);
                value = res ? val : (long?)null;
                return res;
            }
            #endregion
            #region TryReadNullableUInt8()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadNullableUInt8(out byte? value) {
                if(this.IsEmpty) {
                    value = default;
                    return true;
                }
                var raw = ReadValue(in this);
                var res = TryParseUInt8(raw.Data, raw.Index, raw.Length, out byte val);
                value = res ? val : (byte?)null;
                return res;
            }
            #endregion
            #region TryReadNullableUInt16()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadNullableUInt16(out ushort? value) {
                if(this.IsEmpty) {
                    value = default;
                    return true;
                }
                var raw = ReadValue(in this);
                var res = TryParseUInt16(raw.Data, raw.Index, raw.Length, out ushort val);
                value = res ? val : (ushort?)null;
                return res;
            }
            #endregion
            #region TryReadNullableUInt32()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadNullableUInt32(out uint? value) {
                if(this.IsEmpty) {
                    value = default;
                    return true;
                }
                var raw = ReadValue(in this);
                var res = TryParseUInt32(raw.Data, raw.Index, raw.Length, out uint val);
                value = res ? val : (uint?)null;
                return res;
            }
            #endregion
            #region TryReadNullableUInt64()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadNullableUInt64(out ulong? value) {
                if(this.IsEmpty) {
                    value = default;
                    return true;
                }
                var raw = ReadValue(in this);
                var res = TryParseUInt64(raw.Data, raw.Index, raw.Length, out ulong val);
                value = res ? val : (ulong?)null;
                return res;
            }
            #endregion
            #region TryReadNullableGuid()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadNullableGuid(out Guid? value) {
                if(this.IsEmpty) {
                    value = default;
                    return true;
                }
                var raw = ReadValue(in this);
                int index = raw.Index;
                var res = TryParseGuid(raw.Data, ref index, raw.Length, out Guid val);
                value = res ? val : (Guid?)null;
                return res;
            }
            #endregion
            #region TryReadNullableDateTime()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadNullableDateTime(out DateTime? value) {
                if(this.IsEmpty) {
                    value = default;
                    return true;
                }
                var raw = ReadValue(in this);
                int index = raw.Index;
                var res = TryParseDateTime(raw.Data, ref index, out DateTime val);
                value = res ? val : (DateTime?)null;
                return res;
            }
            #endregion
            #region TryReadNullableTimeSpan()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadNullableTimeSpan(out TimeSpan? value) {
                if(this.IsEmpty) {
                    value = default;
                    return true;
                }
                var raw = ReadValue(in this);
                int index = raw.Index;
                var res = TryParseTimeSpan(raw.Data, ref index, true, out TimeSpan val);
                value = res ? val : (TimeSpan?)null;
                return res;
            }
            #endregion
            #region TryReadNullableFloat()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadNullableFloat(out float? value) {
                if(this.IsEmpty) {
                    value = default;
                    return true;
                }
                var raw = ReadValue(in this);
                var res = TryParseFloat(raw.Data, raw.Index, raw.Length, this.Owner.m_charBuffer, out float val);
                value = res ? val : (float?)null;
                return res;
            }
            #endregion
            #region TryReadNullableDouble()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadNullableDouble(out double? value) {
                if(this.IsEmpty) {
                    value = default;
                    return true;
                }
                var raw = ReadValue(in this);
                var res = TryParseDouble(raw.Data, raw.Index, raw.Length, this.Owner.m_charBuffer, out double val);
                value = res ? val : (double?)null;
                return res;
            }
            #endregion
            #region TryReadNullableDecimal()
            /// <summary>
            ///     Tries to interpret the value in the given type.
            /// </summary>
            public bool TryReadNullableDecimal(out decimal? value) {
                if(this.IsEmpty) {
                    value = default;
                    return true;
                }
                var raw = ReadValue(in this);
                var res = TryParseDecimal(raw.Data, raw.Index, raw.Length, this.Owner.m_charBuffer, out decimal val);
                value = res ? val : (decimal?)null;
                return res;
            }
            #endregion

            // implicit casts -- returns default on empty/formatexception
            #region implicit bool?
            public static implicit operator bool?(CsvValue value) {
                value.TryReadNullableBool(out var res);
                return res;
            }
            #endregion
            #region implicit char?
            public static implicit operator char?(CsvValue value) {
                value.TryReadNullableChar(out var res);
                return res;
            }
            #endregion
            #region implicit sbyte?
            public static implicit operator sbyte?(CsvValue value) {
                value.TryReadNullableInt8(out var res);
                return res;
            }
            #endregion
            #region implicit short?
            public static implicit operator short?(CsvValue value) {
                value.TryReadNullableInt16(out var res);
                return res;
            }
            #endregion
            #region implicit int?
            public static implicit operator int?(CsvValue value) {
                value.TryReadNullableInt32(out var res);
                return res;
            }
            #endregion
            #region implicit long?
            public static implicit operator long?(CsvValue value) {
                value.TryReadNullableInt64(out var res);
                return res;
            }
            #endregion
            #region implicit byte?
            public static implicit operator byte?(CsvValue value) {
                value.TryReadNullableUInt8(out var res);
                return res;
            }
            #endregion
            #region implicit ushort?
            public static implicit operator ushort?(CsvValue value) {
                value.TryReadNullableUInt16(out var res);
                return res;
            }
            #endregion
            #region implicit uint?
            public static implicit operator uint?(CsvValue value) {
                value.TryReadNullableUInt32(out var res);
                return res;
            }
            #endregion
            #region implicit ulong?
            public static implicit operator ulong?(CsvValue value) {
                value.TryReadNullableUInt64(out var res);
                return res;
            }
            #endregion
            #region implicit Guid?
            public static implicit operator Guid?(CsvValue value) {
                value.TryReadNullableGuid(out var res);
                return res;
            }
            #endregion
            #region implicit float?
            public static implicit operator float?(CsvValue value) {
                value.TryReadNullableFloat(out var res);
                return res;
            }
            #endregion
            #region implicit double?
            public static implicit operator double?(CsvValue value) {
                value.TryReadNullableDouble(out var res);
                return res;
            }
            #endregion
            #region implicit decimal?
            public static implicit operator decimal?(CsvValue value) {
                value.TryReadNullableDecimal(out var res);
                return res;
            }
            #endregion
            #region implicit DateTime?
            public static implicit operator DateTime?(CsvValue value) {
                value.TryReadNullableDateTime(out var res);
                return res;
            }
            #endregion
            #region implicit TimeSpan?
            public static implicit operator TimeSpan?(CsvValue value) {
                value.TryReadNullableTimeSpan(out var res);
                return res;
            }
            #endregion

            #region implicit string
            public static implicit operator string(CsvValue value) {
                value.TryReadString(out var res);
                return res;
            }
            #endregion
            #region implicit byte[]
            /// <summary>
            ///     Reads the raw values.
            ///     This will un-doublequote the input.
            /// </summary>
            public static implicit operator byte[](CsvValue value) {
                value.TryReadByteArray(out var res);
                return res;
            }
            #endregion
            #region implicit Stream
            public static implicit operator Stream(CsvValue value) {
                value.TryReadStream(out var res);
                return res;
            }
            #endregion

            #region implicit bool
            public static implicit operator bool(CsvValue value) {
                value.TryReadBool(out var res);
                return res;
            }
            #endregion
            #region implicit char
            public static implicit operator char(CsvValue value) {
                value.TryReadChar(out var res);
                return res;
            }
            #endregion
            #region implicit sbyte
            public static implicit operator sbyte(CsvValue value) {
                value.TryReadInt8(out var res);
                return res;
            }
            #endregion
            #region implicit short
            public static implicit operator short(CsvValue value) {
                value.TryReadInt16(out var res);
                return res;
            }
            #endregion
            #region implicit int
            public static implicit operator int(CsvValue value) {
                value.TryReadInt32(out var res);
                return res;
            }
            #endregion
            #region implicit long
            public static implicit operator long(CsvValue value) {
                value.TryReadInt64(out var res);
                return res;
            }
            #endregion
            #region implicit byte
            public static implicit operator byte(CsvValue value) {
                value.TryReadUInt8(out var res);
                return res;
            }
            #endregion
            #region implicit ushort
            public static implicit operator ushort(CsvValue value) {
                value.TryReadUInt16(out var res);
                return res;
            }
            #endregion
            #region implicit uint
            public static implicit operator uint(CsvValue value) {
                value.TryReadUInt32(out var res);
                return res;
            }
            #endregion
            #region implicit ulong
            public static implicit operator ulong(CsvValue value) {
                value.TryReadUInt64(out var res);
                return res;
            }
            #endregion
            #region implicit Guid
            public static implicit operator Guid(CsvValue value) {
                value.TryReadGuid(out var res);
                return res;
            }
            #endregion
            #region implicit float
            public static implicit operator float(CsvValue value) {
                value.TryReadFloat(out var res);
                return res;
            }
            #endregion
            #region implicit double
            public static implicit operator double(CsvValue value) {
                value.TryReadDouble(out var res);
                return res;
            }
            #endregion
            #region implicit decimal
            public static implicit operator decimal(CsvValue value) {
                value.TryReadDecimal(out var res);
                return res;
            }
            #endregion
            #region implicit DateTime
            public static implicit operator DateTime(CsvValue value) {
                value.TryReadDateTime(out var res);
                return res;
            }
            #endregion
            #region implicit TimeSpan
            public static implicit operator TimeSpan(CsvValue value) {
                value.TryReadTimeSpan(out var res);
                return res;
            }
            #endregion


            #region internal TrimStart()
            internal void TrimStart() {
                var rowBuffers    = this.Owner.m_rowBuffers;
                var remaining     = this.Length;
                var index         = this.BufferIndex;

                while(remaining > 0) {
                    var rowBuffer = rowBuffers[this.RowBufferIndex];
                    var bytes     = Math.Min(remaining, BUFFER_SIZE);
                    int removed   = 0;

                    while(removed < bytes) {
                        var b = rowBuffer[index++];
                        if(b != ' ' && b != '\t') {
                            this.BufferIndex += removed;
                            this.Length -= removed;
                            return;
                        } else
                            removed++;
                    }
                    index            = 0;
                    this.BufferIndex = 0;
                    remaining       -= removed;
                    this.Length     -= removed;
                    this.RowBufferIndex++;
                }
            }
            #endregion
            #region internal TrimEnd()
            internal void TrimEnd() {
                var rowBuffers           = this.Owner.m_rowBuffers;
                var remaining            = this.Length;

                var firstBufferRemaining = Math.Min(this.BufferIndex + remaining, BUFFER_SIZE) - this.BufferIndex;
                var remainingBuffers     = (remaining - firstBufferRemaining) / BUFFER_SIZE + ((remaining - firstBufferRemaining) % BUFFER_SIZE != 0 ? 1 : 0);
                var middleBuffersSize    = Math.Max(0, remainingBuffers - 1) * BUFFER_SIZE;
                var index                = remaining - firstBufferRemaining - middleBuffersSize - 1;
                int rowBufferIndex       = this.RowBufferIndex + remainingBuffers - 1;

                while(remaining > 0) {
                    var rowBuffer = rowBuffers[rowBufferIndex];
                    var bytes     = Math.Min(remaining, index);
                    int removed   = 0;

                    while(removed < bytes) {
                        var b = rowBuffer[index--];
                        if(b != ' ' && b != '\t') {
                            this.Length -= removed;
                            return;
                        } else
                            removed++;
                    }
                    index        = BUFFER_SIZE;
                    remaining   -= removed;
                    this.Length -= removed;
                    rowBufferIndex--;
                }
            }
            #endregion

            #region ToString()
            public override string ToString() {
                if(this.BufferIndex == 0 && this.Length == 0 && this.RowBufferIndex == 0)
                    return "null";
                return $"{{{nameof(RowBufferIndex)}={this.RowBufferIndex}, {nameof(BufferIndex)}={this.BufferIndex}, {nameof(Length)}={this.Length}}}{(this.IsEmpty ? "  (.IsNull=true)" : null)}";
            }
            #endregion
        }
    }
}