using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using static System.Runtime.CompilerServices.MethodImplOptions;
using System.Diagnostics;


namespace TimeSeriesDB.IO
{
    /// <summary>
    ///     Efficient CSV file reader.
    ///     This is significantly faster than StreamReader/StringReader because no convertion takes place and all reads are hand-coded for speed.
    ///     This class however assumes the columns format is known ahead of time and that it was written by the writer.
    ///     Everything is decoded in UTF-8.
    /// </summary>
    public sealed class CsvStreamReader {
        private CultureInfo FORMAT = CultureInfo.InvariantCulture;

        // important note about UTF-8:
        // normally, we would have to use a unicode decoder to properly decode strings
        // however, due to the way data is encoded in UTF-8, even in multi-byte character encodings, you cannot have any multi-bytes
        // that would give a value '< 128' that wasn't meant to be a '< 128' in ASCII.
        // as such, all of the code compares directly bytes matching values '< 128' without worrying about proper UTF-8 decoding

        private const int BUFFER_SIZE     = 4096;
        private const int MAX_BUFFER_SIZE = BUFFER_SIZE * 2; // important to allow some overflow to speed up reads (avoids checking for overflows)

        private readonly Stream m_stream;
        private readonly byte[] m_buffer = new byte[MAX_BUFFER_SIZE];
        private int m_offset = 0;
        private int m_read = 0;

        private readonly Decoder m_decoder = Encoding.UTF8.GetDecoder();
        private readonly char[] m_charBuffer = new char[MAX_BUFFER_SIZE];

        private ulong m_skipColumnsParse = 0;
        private CsvValue[] m_row = new CsvValue[64];
        
        #region constructors
        public CsvStreamReader(Stream stream) {
            m_stream = stream ?? throw new ArgumentNullException(nameof(stream));
            //this.RefreshBuffer();
        }
        #endregion

        #region Read()
        /// <summary>
        ///     Advances the CsvReader to the next record.
        /// </summary>
        public bool Read() {
            if(m_offset >= BUFFER_SIZE || m_offset >= m_read)
                this.RefreshBuffer();

            this.FieldCount = 0;
            while(true) {
                // search only within buffer, do not extend past it
                while(m_offset < m_read) {
                    var b = m_buffer[m_offset];

                    if(b != ' ' && b != '\t') {
                        if(b == ',') {
                            m_row[this.FieldCount++] = CsvValue.Null; // new CsvValue(null, DataKind.Null)
                            m_offset++;
                            continue;
                        }
                        if(b == '\r') {
                            m_offset++;
                            continue;
                        }
                        if(b == '\n') {
                            m_row[this.FieldCount++] = CsvValue.Null; // new CsvValue(null, DataKind.Null)
                            m_offset++;
                            return true;
                        }
                        break;
                    }
                    m_offset++;
                }

                // end of stream
                if(m_offset == m_read)
                    return this.FieldCount > 0;

                if((m_skipColumnsParse & (1ul << this.FieldCount)) == 0)
                    m_row[this.FieldCount] = this.InternalGetValue();
                
                this.FieldCount++;

                // skip to next column/row
                while(m_offset < m_read) {
                    var b = m_buffer[m_offset++];
                    if(b == ',')
                        break;
                    if(b == '\n')
                        return true;
                    if(b == '"')
                        this.SkipString();
                }
            };
        }
        #endregion
        #region FieldCount
        /// <summary>
        ///     Gets the number of columns in the current row.
        /// </summary>
        public int FieldCount { get; private set; }
        #endregion
        #region this[]
        public object this[int columnIndex] => m_row[columnIndex].Value;
        #endregion
        #region SkipColumns
        /// <summary>
        ///     Marks the columns where parsing is not required.
        ///     Setting null will skip no column.
        /// </summary>
        public bool[] SkipColumns {
            get {
                var res = new bool[64];
                var current = m_skipColumnsParse;
                for(int i = 0; i < 64; i++) {
                    res[i] = (current & 1) == 1;
                    current >>= 1;
                }
                return res;
            }
            set {
                m_skipColumnsParse = 0;
                if(value == null || value.Length == 0)
                    return;
                for(int i = 0; i < value.Length; i++)
                    m_skipColumnsParse = (m_skipColumnsParse << 1) | (value[i] ? 1ul : 0ul);
            }
        }
        #endregion

        #region GetRows()
        /// <summary>
        ///     Shortcut to read all rows.
        ///     By default, will return the same row instances.
        /// </summary>
        public IEnumerable<object[]> GetRows(bool returnNewRowsOnly = false) {
            object[][] cache = null;

            while(this.Read()) {
                object[] row;

                if(returnNewRowsOnly)
                    row = new object[this.FieldCount];
                else {
                    if(cache == null || cache.Length < this.FieldCount) {
                        var old_size = cache?.Length ?? 0;
                        if(cache == null)
                            cache = new object[this.FieldCount][];
                        else if(cache.Length < this.FieldCount)
                            Array.Resize(ref cache, this.FieldCount);
                        for(int i = old_size; i < cache.Length; i++)
                            cache[i] = new object[i];
                    }
                    row = cache[this.FieldCount];
                }
                
                this.GetValues(row);
                yield return row;
            }
        }
        #endregion
        #region GetValue()
        public object GetValue(int columnIndex) => m_row[columnIndex].Value;
        #endregion
        #region GetValues()
        /// <summary>
        ///     Shortcut to read the values from the current row.
        ///     Returns the number of returned/read columns.
        /// </summary>
        public int GetValues(object[] values) {
            if(values == null)
                return 0;

            var count = Math.Min(this.FieldCount, values.Length);
            for(int i = 0; i < count; i++)
                values[i] = m_row[i].Value;

            return count;
        }
        /// <summary>
        ///     Shortcut to read the values from the current row.
        /// </summary>
        public object[] GetValues() {
            var count = this.FieldCount;
            var res = new object[count];
            
            for(int i = 0; i < count; i++)
                res[i] = m_row[i].Value;

            return res;
        }
        #endregion
        #region GetRawValue()
        public CsvValue GetRawValue(int columnIndex) => m_row[columnIndex];
        #endregion
        #region GetRawValues()
        /// <summary>
        ///     Shortcut to read the CsvValues from the current row.
        ///     Returns the number of returned/read columns.
        /// </summary>
        public int GetRawValues(CsvValue[] values) {
            if(values == null)
                return 0;

            var count = Math.Min(this.FieldCount, values.Length);
            
            //Array.Copy(m_row, values, count);
            for(int i = 0; i < count; i++)
                values[i] = m_row[i];

            return count;
        }
        /// <summary>
        ///     Shortcut to read the CsvValues from the current row.
        /// </summary>
        public CsvValue[] GetRawValues() {
            var count = this.FieldCount;
            var res = new CsvValue[count];
            
            //Array.Copy(m_row, res, count);
            for(int i = 0; i < count; i++)
                res[i] = m_row[i];

            return res;
        }
        #endregion

        #region private RefreshBuffer()
        /// <summary>
        ///     Refreshes the buffer and downshifts the remaining (unread) data.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private void RefreshBuffer() {
            int remaining = m_read - m_offset;

            // downshift remainder data
            if(remaining > 0)
                Buffer.BlockCopy(m_buffer, m_offset, m_buffer, 0, remaining);

            m_read = remaining + m_stream.Read(m_buffer, remaining, MAX_BUFFER_SIZE - remaining);
            m_offset = 0;
        }
        #endregion
        #region private DetermineDataLengthWithinBuffer()
        /// <summary>
        ///     Starting from current position within buffer, searches for the first ',', '\n' or '\r'.
        ///     The ending whitespaces of the data will be trimmed.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private int DetermineDataLengthWithinBuffer() {
            int start = m_offset;
            int lastNonWhitespace = start;

            while(start < m_read) {
                var b = m_buffer[start++];

                if(b == ',' || b == '\n' || b == '\r')
                    break;
                // char.IsWhiteSpace() very slow as it checks a lot of cases that dont apply to CSV
                if(b != ' ' && b != '\t')
                    lastNonWhitespace = start;
            }
            // end not found within buffer
            return lastNonWhitespace - m_offset; //return -1;
        }
        #endregion
        #region private TryDetermineDataLengthWithinBuffer()
        /// <summary>
        ///     Starting from current position within buffer, searches for the first ',', '\n' or '\r'.
        ///     The ending whitespaces of the data will be trimmed.
        ///     If the end of column or end of line is not found, returns -1.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private int TryDetermineDataLengthWithinBuffer() {
            int start = m_offset;
            int lastNonWhitespace = start;

            while(start < m_read) {
                var b = m_buffer[start++];

                if(b == ',' || b == '\n' || b == '\r')
                    return lastNonWhitespace - m_offset;
                // char.IsWhiteSpace() very slow as it checks a lot of cases that dont apply to CSV
                if(b != ' ' && b != '\t')
                    lastNonWhitespace = start;
            }
            // end not found within buffer
            return -1;
        }
        #endregion

        #region private InternalGetValue()
        private CsvValue InternalGetValue() {
            var b = m_buffer[m_offset];

            // if we're on a string/variable-length object
            if(b == '"') {
                var res = new CsvValue(this.InternalGetDelimitedString(), CsvValue.DataKind.String);
                // since this is variable-sized, make sure the buffer contains every non-variable sized items
                if(m_offset >= BUFFER_SIZE)
                    this.RefreshBuffer();
                return res;
            }

            //// then in every other case, we have to make sure we can read consecutively the entire data
            //if(m_offset >= BUFFER_SIZE)
            //    this.RefreshBuffer();

            // special values:
            // <nothing>, null, true, false, NaN, Inf, -Inf, -<timestamp>, "..."
            // since the caller is assumed to have called SkipToData(), there is no need to check for <nothing>

            m_offset++;

            // check for 'null', 'NaN'
            if(b == 'n' || b == 'N') {
                b = m_buffer[m_offset++];
                if(b == 'u' || b == 'U')
                    return CsvValue.Null; //new CsvValue(null, CsvValue.DataKind.Null);
                if(b == 'a' || b == 'A')
                    return new CsvValue(double.NaN, CsvValue.DataKind.Double);

                // encode as raw string ?
                // would be kind of evil since I don't fully parse the values to make sure they are 'null', yet can assume non-quoted strings are possible?
                //throw new FormatException();

                // this occurs when it is a non " delimited string that extends past the buffer
                m_offset--;
                var res = new CsvValue(this.InternalGetNonDelimitedString(), CsvValue.DataKind.String);
                // since this is variable-sized, make sure the buffer contains every non-variable sized items
                if(m_offset >= BUFFER_SIZE)
                    this.RefreshBuffer();
                return res;
            }
            // check for 'true'
            if(b == 't' || b == 'T')
                return new CsvValue(1, CsvValue.DataKind.Int32);
            // check for 'false'
            if(b == 'f' || b == 'F')
                return new CsvValue(0, CsvValue.DataKind.Int32);
            // check for 'Inf'
            if(b == 'i' || b == 'I')
                return new CsvValue(double.PositiveInfinity, CsvValue.DataKind.Double);
            // check for '-Inf'
            if(b == '-') {
                var temp = m_buffer[m_offset];
                if(temp == 'i' || temp == 'I') {
                    m_offset++;
                    return new CsvValue(double.NegativeInfinity, CsvValue.DataKind.Double);
                }
            }

            int len = this.TryDetermineDataLengthWithinBuffer();
            m_offset--;

            if(len < 0) {
                // this occurs when it is a non " delimited string that extends past the buffer
                var res = new CsvValue(this.InternalGetNonDelimitedString(), CsvValue.DataKind.String);
                // since this is variable-sized, make sure the buffer contains every non-variable sized items
                if(m_offset >= BUFFER_SIZE)
                    this.RefreshBuffer();
                return res;
            }

            // count the number of '-', '/', ':', '.'
            // and only [0-9 ,./-] are valid

            len++;
            
            int dash_count = 0;
            int slash_count = 0;
            int dot_index = -1;
            int doubledot_count = 0;
            int uppercase_T_count = 0; // in case of 20100101T000000
            bool contains_exponent = false;

            for(int i = 0; i < len; i++) {
                b = m_buffer[m_offset + i];

                if(b <= '9' && b >= '0')
                    continue;
                else if(b == '.')
                    dot_index = i;
                else if(b == '-')
                    dash_count++;
                else if(b == '/')
                    slash_count++;
                else if(b == ':')
                    doubledot_count++;
                else if(b == 'E' || b == 'e')
                    contains_exponent = true;
                else if(b == ' ' || b == '+')
                    continue;
                else if(b == 'T')
                    uppercase_T_count++;
                else {
                    //throw new FormatException();
                    var res = new CsvValue(this.InternalGetNonDelimitedString(len), CsvValue.DataKind.String);
                    // since this is variable-sized, make sure the buffer contains every non-variable sized items
                    if(m_offset >= BUFFER_SIZE)
                        this.RefreshBuffer();
                    return res;
                }
            }

            if(dash_count > 1 || slash_count > 0)
                return new CsvValue(this.InternalGetDateTime(), CsvValue.DataKind.DateTime);
            if(doubledot_count > 0)
                return new CsvValue(this.InternalGetTimeSpan(dot_index >= 0), CsvValue.DataKind.TimeSpan);
            if(uppercase_T_count == 1)
                return new CsvValue(this.InternalGetDateTime(), CsvValue.DataKind.DateTime);

            return this.InternalGetNumber(len, dot_index, contains_exponent);
        }
        #endregion
        #region private InternalGetNonDelimitedString()
        /// <summary>
        ///     Reads the string in this column.
        ///     The string will search until ',' or '\r' or '\n' is found.
        /// </summary>
        private string InternalGetNonDelimitedString() {
            // todo: add support for 'null' and ',,'

            StringBuilder sb = null; // create only if needed

            while(true) {
                var len = this.TryDetermineDataLengthWithinBuffer();

                if(len >= 0) {
                    int readChars = m_decoder.GetChars(m_buffer, m_offset, len, m_charBuffer, 0);
                    m_offset += len;
                    if(sb == null)
                        return new string(m_charBuffer, 0, readChars);
                    else {
                        sb.Append(m_charBuffer, 0, readChars);
                        return sb.ToString();
                    }
                } else {
                    // if continues past buffer
                    int readChars = m_decoder.GetChars(m_buffer, m_offset, MAX_BUFFER_SIZE - m_offset, m_charBuffer, 0);
                    if(sb == null)
                        sb = new StringBuilder(readChars);
                    sb.Append(m_charBuffer, 0, readChars);

                    this.RefreshBuffer();
                    if(m_read == 0)
                        return sb.ToString();
                }
            }
        }
        /// <summary>
        ///     Reads the string in this column.
        ///     The string will search until ',' or '\r' or '\n' is found.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private string InternalGetNonDelimitedString(int data_length) {
            int readChars = m_decoder.GetChars(m_buffer, m_offset, data_length, m_charBuffer, 0);
            m_offset += data_length;
            return new string(m_charBuffer, 0, readChars);
        }
        #endregion
        #region private InternalGetDelimitedString()
        /// <summary>
        ///     Reads the string in this column.
        ///     The string is required to have '"' around, and will throw if not (even on null string).
        /// </summary>
        private string InternalGetDelimitedString() {
            // todo: add support for 'null' and ',,'

            var b = m_buffer[m_offset];
            if(b != '"')
                throw new FormatException();

            // position inside the string
            m_offset++;
            StringBuilder sb = null; // create only if needed
            int start = m_offset;
            int size;

            while(true) {
                while(m_offset < m_read) {
                    b = m_buffer[m_offset++];

                    if(b == '"') {
                        // pre-fetch next byte
                        if(m_offset == m_read) {
                            // append remainder of block directly into result
                            size = m_offset - start - 1;
                            if(size > 0) {
                                int readChars = m_decoder.GetChars(m_buffer, start, size, m_charBuffer, 0);
                                if(sb == null)
                                    sb = new StringBuilder(readChars);
                                sb.Append(m_charBuffer, 0, readChars);
                            }
                            start = 0;

                            this.RefreshBuffer();
                            // if we reach end of stream and finished with ", that means we have a normal string end at the end of the stream
                            if(m_read == 0)
                                return sb?.ToString() ?? string.Empty;
                        }

                        // not two consecutive " (meaning: this was the end of the string)
                        if(m_buffer[m_offset] != '"') {
                            // append up to "
                            size = m_offset - start - 1;
                            if(size > 0) {
                                int readChars = m_decoder.GetChars(m_buffer, start, size, m_charBuffer, 0);
                                if(sb == null)
                                    return new string(m_charBuffer, 0, readChars);
                                else {
                                    sb.Append(m_charBuffer, 0, readChars);
                                    return sb.ToString();
                                }
                            } else
                                return sb?.ToString() ?? string.Empty;
                        }

                        // two consecutive ", keep searching for end of string
                        m_offset++;

                        // append everything up to first "
                        size = m_offset - start - 1;
                        if(size > 0) {
                            int readChars = m_decoder.GetChars(m_buffer, start, size, m_charBuffer, 0);
                            if(sb == null)
                                sb = new StringBuilder(readChars);
                            sb.Append(m_charBuffer, 0, readChars);
                        }
                        start = m_offset;
                    }
                }

                // append remainder of block directly into result
                size = m_offset - start;
                if(size > 0) {
                    int readChars = m_decoder.GetChars(m_buffer, start, size, m_charBuffer, 0);
                    if(sb == null)
                        sb = new StringBuilder(readChars);
                    sb.Append(m_charBuffer, 0, readChars);
                }
                start = 0;

                this.RefreshBuffer();
                if(m_read == 0)
                    throw new EndOfStreamException(); // FormatException(); ? since non-terminated string
            }
        }
        #endregion
        #region private CountDigits()
        [MethodImpl(AggressiveInlining)]
        private int CountDigits() {
            int digits = 0;
            int offset = m_offset;

            while(offset < m_read) {
                var b = m_buffer[offset++];
                if(b <= '9' && b >= '0')
                    digits++;
                else
                    break;
            }
            return digits;
        }
        #endregion
        #region private InternalGetDateTime()
        private DateTime InternalGetDateTime() {
            // sortable format
            // 2008-04-10 06:30:00.1234567

            int year = unchecked((int)Fast_AtoI_UInt32(m_buffer, m_offset, 4));
            m_offset += 4;

            var b = m_buffer[m_offset];
            if(b == '-' || b == '/')
                m_offset++;

            int month = unchecked((int)Fast_AtoI_UInt32(m_buffer, m_offset, 2));
            m_offset += 2;

            b = m_buffer[m_offset];
            if(b == '-' || b == '/')
                m_offset++;

            int day = unchecked((int)Fast_AtoI_UInt32(m_buffer, m_offset, 2));
            m_offset += 2;

            var time = this.InternalGetTimeSpan(false);
            return new DateTime(new DateTime(year, month, day).Ticks + time.Ticks, DateTimeKind.Utc);
        }
        #endregion
        #region private InternalGetTimeSpan()
        private TimeSpan InternalGetTimeSpan(bool readDays) {
            // constant format
            // 00:00:00, 3.17:25:30.5000000

            var b = m_buffer[m_offset];
            if(b == ' ' || b == 'T') // in case of 9999-12-20T00:00:00"
                b = m_buffer[++m_offset];

            bool is_negative = false;

            if(b == '-') {
                is_negative = true;
                b = m_buffer[++m_offset];
            }

            if(b > '9' || b < '0')
                return TimeSpan.Zero;

            int day = 0;
            if(readDays) {
                var digits = this.CountDigits();
                if(m_buffer[m_offset + digits] == '.')
                    day = unchecked((int)Fast_AtoI_UInt32(m_buffer, m_offset, digits));
            }

            int hour = unchecked((int)Fast_AtoI_UInt32(m_buffer, m_offset, 2));
            m_offset += 2;

            b = m_buffer[m_offset];
            if(b == ':')
                m_offset++;

            int minute = unchecked((int)Fast_AtoI_UInt32(m_buffer, m_offset, 2));
            m_offset += 2;

            b = m_buffer[m_offset];
            if(b == ':')
                m_offset++;

            long millisecond = 0;
            int second = unchecked((int)Fast_AtoI_UInt32(m_buffer, m_offset, 2));
            m_offset += 2;

            b = m_buffer[m_offset];
            if(b == '.') {
                m_offset++;

                int digits = Math.Min(this.CountDigits(), 7);

                millisecond = unchecked((int)Fast_AtoI_UInt32(m_buffer, m_offset, digits));
                m_offset += digits;

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

            return new TimeSpan(ticks);
        }
        #endregion
        #region private InternalGetNumber()
        private CsvValue InternalGetNumber(int len, int dot_index, bool contains_exponent) {
            if(contains_exponent) {
                // floating point data
                for(int i = 0; i < len; i++)
                    m_charBuffer[i] = unchecked((char)m_buffer[m_offset + i]);
                m_offset += len;

                var res = double.Parse(
                    new string(m_charBuffer, 0, len),
                    NumberStyles.AllowCurrencySymbol | NumberStyles.AllowLeadingSign | NumberStyles.AllowExponent, // | NumberStyles.AllowDecimalPoint
                    FORMAT);
                return new CsvValue(res, CsvValue.DataKind.Decimal);
            } else if(dot_index < 0) {
                if(len <= 9) {
                    var res = Fast_AtoI_Int32(m_buffer, m_offset, len);
                    m_offset += len;
                    return new CsvValue(res, CsvValue.DataKind.Int32);
                } else if(len <= 19) {
                    var res = Fast_AtoI_Int64(m_buffer, m_offset, len);
                    m_offset += len;
                    if(res <= int.MaxValue && res >= int.MinValue)
                        return new CsvValue(unchecked((int)res), CsvValue.DataKind.Int32);
                    else
                        return new CsvValue(res, CsvValue.DataKind.Int64);
                } else {
                    for(int i = 0; i < len; i++)
                        m_charBuffer[i] = unchecked((char)m_buffer[m_offset + i]);
                    m_offset += len;

                    // since this has no floating points, the highest number available in .net is a decimal
                    var res = decimal.Parse(
                        new string(m_charBuffer, 0, len),
                        NumberStyles.AllowCurrencySymbol | NumberStyles.AllowLeadingSign,
                        FORMAT);

                    if(res <= long.MaxValue && res >= long.MinValue)
                        return new CsvValue(unchecked((long)res), CsvValue.DataKind.Int64);
                    else if(res <= ulong.MaxValue && res >= ulong.MinValue)
                        return new CsvValue(unchecked((ulong)res), CsvValue.DataKind.UInt64);
                    else
                        return new CsvValue(res, CsvValue.DataKind.Decimal);
                }
            } else {
                // floating point data
                for(int i = 0; i < len; i++)
                    m_charBuffer[i] = unchecked((char)m_buffer[m_offset + i]);
                m_offset += len;

                //int magnitude = dot_index;
                //int precision = len - dot_index - 1;
                // call proper decimal/double parse depending on magnitude/precision

                // todo: better determine decimal/double parse because double supports a bigger range 
                // but you want to call decimal parse to avoid rounding loss
                
                var res = decimal.Parse(
                    new string(m_charBuffer, 0, len),
                    NumberStyles.AllowCurrencySymbol | NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint,
                    FORMAT);
                return new CsvValue(res, CsvValue.DataKind.Decimal);
            }
        }
        #endregion

        #region private SkipString()
        /// <summary>
        ///     Must be right after the initial '"'.
        ///     This will position itself after the ending '"'.
        ///     ie: assumes you're currently at the beginning of the string data.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private void SkipString() {
            while(true) {
                while(m_offset < m_read) {
                    var b = m_buffer[m_offset++];

                    if(b == '"') {
                        // pre-fetch next byte
                        if(m_offset == m_read) {
                            this.RefreshBuffer();
                            // if we reach end of stream and finished with ", that means we have a normal string end at the end of the stream
                            if(m_read == 0)
                                return;
                        }

                        // not two consecutive " (meaning: this was the end of the string)
                        if(m_buffer[m_offset] != '"')
                            return;

                        // two consecutive ", keep searching for end of string
                        m_offset++;
                    }
                }

                this.RefreshBuffer();
                if(m_read == 0)
                    return;
            }
        }
        #endregion

        // BitMethods imported functions
        #region private static Fast_AtoI_Int32()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static int Fast_AtoI_Int32(byte[] buffer, int offset, int count) {
            bool is_negative = false;
            if(buffer[offset] == '-') {
                is_negative = true;
                offset++;
                count--;
            }
            int res = 0;
            switch(count) {
                case 10: res  = (buffer[offset + count - 10] - '0') * 1000000000; goto case 9;
                case 9:  res += (buffer[offset + count -  9] - '0') * 100000000;  goto case 8;
                case 8:  res += (buffer[offset + count -  8] - '0') * 10000000;   goto case 7;
                case 7:  res += (buffer[offset + count -  7] - '0') * 1000000;    goto case 6;
                case 6:  res += (buffer[offset + count -  6] - '0') * 100000;     goto case 5;
                case 5:  res += (buffer[offset + count -  5] - '0') * 10000;      goto case 4;
                case 4:  res += (buffer[offset + count -  4] - '0') * 1000;       goto case 3;
                case 3:  res += (buffer[offset + count -  3] - '0') * 100;        goto case 2;
                case 2:  res += (buffer[offset + count -  2] - '0') * 10;         goto case 1;
                case 1:  res += (buffer[offset + count -  1] - '0') * 1;          break;
                default:
                    throw new FormatException();
            }
            return !is_negative ? res : -res;
        }
        #endregion
        #region private static Fast_AtoI_Int64()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static long Fast_AtoI_Int64(byte[] buffer, int offset, int count) {
            bool is_negative = false;
            if(buffer[offset] == '-') {
                is_negative = true;
                offset++;
                count--;
            }
            long res = 0;
            switch(count) {
                case 19: res  = (buffer[offset + count - 19] - '0') * 1000000000000000000;  goto case 18;
                case 18: res += (buffer[offset + count - 18] - '0') * 100000000000000000;   goto case 17;
                case 17: res += (buffer[offset + count - 17] - '0') * 10000000000000000;    goto case 16;
                case 16: res += (buffer[offset + count - 16] - '0') * 1000000000000000;     goto case 15;
                case 15: res += (buffer[offset + count - 15] - '0') * 100000000000000;      goto case 14;
                case 14: res += (buffer[offset + count - 14] - '0') * 10000000000000;       goto case 13;
                case 13: res += (buffer[offset + count - 13] - '0') * 1000000000000;        goto case 12;
                case 12: res += (buffer[offset + count - 12] - '0') * 100000000000;         goto case 11;
                case 11: res += (buffer[offset + count - 11] - '0') * 10000000000;          goto case 10;
                case 10: res += (buffer[offset + count - 10] - '0') * 1000000000;           goto case 9;
                case 9:  res += (buffer[offset + count -  9] - '0') * 100000000;            goto case 8;
                case 8:  res += (buffer[offset + count -  8] - '0') * 10000000;             goto case 7;
                case 7:  res += (buffer[offset + count -  7] - '0') * 1000000;              goto case 6;
                case 6:  res += (buffer[offset + count -  6] - '0') * 100000;               goto case 5;
                case 5:  res += (buffer[offset + count -  5] - '0') * 10000;                goto case 4;
                case 4:  res += (buffer[offset + count -  4] - '0') * 1000;                 goto case 3;
                case 3:  res += (buffer[offset + count -  3] - '0') * 100;                  goto case 2;
                case 2:  res += (buffer[offset + count -  2] - '0') * 10;                   goto case 1;
                case 1:  res += (buffer[offset + count -  1] - '0') * 1;                    break;
                default:
                    throw new FormatException();
            }
            return !is_negative ? res : -res;
        }
        #endregion
        #region private static Fast_AtoI_UInt32()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static uint Fast_AtoI_UInt32(byte[] buffer, int offset, int count) {
            uint res = 0;
            switch(count) {
                case 10: res  = unchecked((uint)(buffer[offset + count - 10] - '0')) * 1000000000; goto case 9;
                case 9:  res += unchecked((uint)(buffer[offset + count -  9] - '0')) * 100000000;  goto case 8;
                case 8:  res += unchecked((uint)(buffer[offset + count -  8] - '0')) * 10000000;   goto case 7;
                case 7:  res += unchecked((uint)(buffer[offset + count -  7] - '0')) * 1000000;    goto case 6;
                case 6:  res += unchecked((uint)(buffer[offset + count -  6] - '0')) * 100000;     goto case 5;
                case 5:  res += unchecked((uint)(buffer[offset + count -  5] - '0')) * 10000;      goto case 4;
                case 4:  res += unchecked((uint)(buffer[offset + count -  4] - '0')) * 1000;       goto case 3;
                case 3:  res += unchecked((uint)(buffer[offset + count -  3] - '0')) * 100;        goto case 2;
                case 2:  res += unchecked((uint)(buffer[offset + count -  2] - '0')) * 10;         goto case 1;
                case 1:  res += unchecked((uint)(buffer[offset + count -  1] - '0')) * 1;          break;
                default:
                    throw new FormatException();
            }
            return res;
        }
        #endregion
        #region private static HexDecode()
        /// <summary>
        ///     Reads the bytes in hexadecimal format, assuming no prepending of any kind (0x).
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static void HexDecode(string source, int sourceOffset, int sourceCount, byte[] target, ref int offset) {
            if(sourceCount <= 0)
                return;
            if(sourceCount % 2 != 0)
                throw new ArgumentException("Not a multiple of 2.", nameof(sourceCount));

            while(sourceCount > 0) {
                target[offset++] = unchecked((byte)((HexDecode(source[sourceOffset + 0]) << 4) & HexDecode(source[sourceOffset + 1])));
                sourceOffset += 2;
                sourceCount -= 2;
            }
        }
        [MethodImpl(AggressiveInlining)]
        private static int HexDecode(char c) {
            if(c <= '9' && c >= '0')
                return c - '0';
            if(c <= 'F' && c >= 'A')
                return c - 'A' + 10;
            if(c <= 'f' && c >= 'a')
                return c - 'a' + 10;

            throw new FormatException();
        }
        #endregion


        /// <summary>
        ///     A generic value wrapper.
        ///     Use the implicit operators to read the values.
        /// </summary>
        readonly public struct CsvValue { // : IConvertible
            public static readonly CsvValue Null = new CsvValue(null, DataKind.Null);

            private readonly object m_value;
            private readonly DataKind m_kind;

            #region constructors
            public CsvValue(object value, DataKind kind) {
                m_value = value;
                m_kind = kind;
            }
            #endregion

            public object Value => m_value;
            public DataKind Kind => m_kind;
            #region IsNull
            public bool IsNull => m_kind == DataKind.Null;
            #endregion

            #region implicit bool?
            public static implicit operator bool?(CsvValue value) {
                if(value.IsNull)
                    return default;
                return (bool)Convert.ChangeType(value.m_value, typeof(bool));
            }
            #endregion
            #region implicit sbyte?
            public static implicit operator sbyte?(CsvValue value) {
                if(value.IsNull)
                    return default;
                return (sbyte)Convert.ChangeType(value.m_value, typeof(sbyte));
            }
            #endregion
            #region implicit short?
            public static implicit operator short?(CsvValue value) {
                if(value.IsNull)
                    return default;
                return (short)Convert.ChangeType(value.m_value, typeof(short));
            }
            #endregion
            #region implicit int?
            public static implicit operator int?(CsvValue value) {
                if(value.IsNull)
                    return default;
                return (int)Convert.ChangeType(value.m_value, typeof(int));
            }
            #endregion
            #region implicit long?
            public static implicit operator long?(CsvValue value) {
                if(value.IsNull)
                    return default;
                return (long)Convert.ChangeType(value.m_value, typeof(long));
            }
            #endregion
            #region implicit byte?
            public static implicit operator byte?(CsvValue value){
                if(value.IsNull)
                    return default;
                return (byte)Convert.ChangeType(value.m_value, typeof(byte));
            }
            #endregion
            #region implicit ushort?
            public static implicit operator ushort?(CsvValue value) {
                if(value.IsNull)
                    return default;
                return (ushort)Convert.ChangeType(value.m_value, typeof(ushort));
            }
            #endregion
            #region implicit uint?
            public static implicit operator uint?(CsvValue value) {
                if(value.IsNull)
                    return default;
                return (uint)Convert.ChangeType(value.m_value, typeof(uint));
            }
            #endregion
            #region implicit ulong?
            public static implicit operator ulong?(CsvValue value) {
                if(value.IsNull)
                    return default;
                return (ulong)Convert.ChangeType(value.m_value, typeof(ulong));
            }
            #endregion
            #region implicit char?
            public static implicit operator char?(CsvValue value) {
                if(value.IsNull)
                    return default;
                return (char)Convert.ChangeType(value.m_value, typeof(char));
            }
            #endregion
            #region implicit float?
            public static implicit operator float?(CsvValue value) {
                if(value.IsNull)
                    return default;
                return (float)Convert.ChangeType(value.m_value, typeof(float));
            }
            #endregion
            #region implicit double?
            public static implicit operator double?(CsvValue value) {
                if(value.IsNull)
                    return default;
                return (double)Convert.ChangeType(value.m_value, typeof(double));
            }
            #endregion
            #region implicit decimal?
            public static implicit operator decimal?(CsvValue value) {
                if(value.IsNull)
                    return default;
                return (decimal)Convert.ChangeType(value.m_value, typeof(decimal));
            }
            #endregion
            #region implicit DateTime?
            public static implicit operator DateTime?(CsvValue value) {
                if(value.IsNull)
                    return default;
                return (DateTime)Convert.ChangeType(value.m_value, typeof(DateTime));
            }
            #endregion
            #region implicit TimeSpan?
            public static implicit operator TimeSpan?(CsvValue value) {
                if(value.IsNull)
                    return default;
                // timespan is not supported by Convert.ChangeType()
                if(value.m_kind == DataKind.TimeSpan)
                    return (TimeSpan)value.m_value;
                if(value.m_kind == DataKind.String)
                    return TimeSpan.Parse((string)value.m_value);
                throw new InvalidCastException();
            }
            #endregion
            #region implicit string
            public static implicit operator string(CsvValue value) {
                if(value.IsNull)
                    return default;
                return (string)Convert.ChangeType(value.m_value, typeof(string));
            }
            #endregion
            #region implicit byte[]
            public static implicit operator byte[](CsvValue value) {
                if(value.IsNull)
                    return default;
                if(value.m_kind != DataKind.String)
                    throw new InvalidCastException();

                var val = (string)value.m_value;
                var res = new byte[val.Length / 2];
                int offset = 0;
                HexDecode(val, 0, val.Length, res, ref offset);
                return res;
            }
            #endregion
            #region implicit Stream
            public static implicit operator Stream(CsvValue value) {
                if(value.IsNull)
                    return default;
                return new MemoryStream((byte[])value);
            }
            #endregion

            #region ToString()
            public override string ToString() {
                if(this.IsNull)
                    return string.Empty;
                return m_value?.ToString();
            }
            #endregion

            #region public enum DataKind
            public enum DataKind {
                Null,
                Int32,
                Int64,
                UInt64,
                Double,
                Decimal,
                DateTime,
                TimeSpan,
                String,
            }
            #endregion
        }
    }
}
