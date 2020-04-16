using System;
using System.Globalization;
using System.IO;
using System.Text;
using System.Runtime.CompilerServices;
using static System.Runtime.CompilerServices.MethodImplOptions;
using System.Diagnostics;


namespace TimeSeriesDB.IO
{
    /// <summary>
    ///     Efficient CSV file writer.
    ///     This is significantly faster than StreamWriter/StringWriter because no convertion takes place and all writes are hand-coded for speed.
    ///     Everything is encoded in UTF-8.
    /// </summary>
    public sealed class CsvStreamWriter : IDisposable {
        private static readonly CultureInfo FORMAT = CultureInfo.InvariantCulture;

        private const int MIN_COMMIT_BUFFER_SIZE = 4096;
        private const int CHAR_BUFFER_SIZE       = 4096;
        private const int MAX_BUFFER_SIZE        = 32768; // important to allow some overflow to speed up writes (avoids checking for overflows)

        private readonly Stream m_stream;
        private readonly byte[] m_buffer = new byte[MAX_BUFFER_SIZE];
        private int m_offset = 0;

        #region constructors
        public CsvStreamWriter(Stream stream) {
            m_stream = stream ?? throw new ArgumentNullException(nameof(stream));
        }
        #endregion

        #region Write()
        [MethodImpl(AggressiveInlining)]
        public void Write(sbyte value) {
            Fast_ItoA(m_buffer, ref m_offset, value);
            this.TryFlushBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(short value) {
            Fast_ItoA(m_buffer, ref m_offset, value);
            this.TryFlushBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(int value) {
            Fast_ItoA(m_buffer, ref m_offset, value);
            this.TryFlushBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(long value) {
            Fast_ItoA(m_buffer, ref m_offset, value);
            this.TryFlushBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(byte value) {
            Fast_ItoA(m_buffer, ref m_offset, value);
            this.TryFlushBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(ushort value) {
            Fast_ItoA(m_buffer, ref m_offset, value);
            this.TryFlushBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(uint value) {
            Fast_ItoA(m_buffer, ref m_offset, value);
            this.TryFlushBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(ulong value) {
            Fast_ItoA(m_buffer, ref m_offset, value);
            this.TryFlushBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(bool value) {
            if(value) {
                m_buffer[m_offset + 0] = (byte)'t';
                m_buffer[m_offset + 1] = (byte)'r';
                m_buffer[m_offset + 2] = (byte)'u';
                m_buffer[m_offset + 3] = (byte)'e';
                m_offset += 4;
            } else {
                m_buffer[m_offset + 0] = (byte)'f';
                m_buffer[m_offset + 1] = (byte)'a';
                m_buffer[m_offset + 2] = (byte)'l';
                m_buffer[m_offset + 3] = (byte)'s';
                m_buffer[m_offset + 4] = (byte)'e';
                m_offset += 5;
            }
            this.TryFlushBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(string value) {
            if(value != null)
                this.InternalWriteStringDoubleQuote(value);
            else
                this.WriteNull();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(DateTime value) {
            // sortable format
            // 2008-04-10 06:30:00.1234567

            // this could be sped up with fast_itoa() code to avoid some of the divisions

            int temp = value.Year;
            m_buffer[m_offset + 3] = (byte)('0' + (temp % 10));
            temp /= 10;
            m_buffer[m_offset + 2] = (byte)('0' + (temp % 10));
            temp /= 10;
            m_buffer[m_offset + 1] = (byte)('0' + (temp % 10));
            temp /= 10;
            m_buffer[m_offset + 0] = (byte)('0' + (temp % 10));
            m_buffer[m_offset + 4] = (byte)'-';
            temp = value.Month;
            m_buffer[m_offset + 5] = (byte)('0' + (temp / 10));
            m_buffer[m_offset + 6] = (byte)('0' + (temp % 10));
            m_buffer[m_offset + 7] = (byte)'-';
            temp = value.Day;
            m_buffer[m_offset + 8] = (byte)('0' + (temp / 10));
            m_buffer[m_offset + 9] = (byte)('0' + (temp % 10));
            m_buffer[m_offset + 10] = (byte)' ';
            m_offset += 11;

            this.InternalWriteTimeSpan(value.TimeOfDay, false);

            this.TryFlushBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(TimeSpan value) {
            // constant format
            // 00:00:00, 3.17:25:30.5000000

            this.InternalWriteTimeSpan(value, true);

            this.TryFlushBuffer();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(decimal value) {
            // convert to long for shorter encoding (ie: 0.0 -> 0)
            if(value >= long.MinValue && value <= long.MaxValue) {
                long converted = (long)value;
                if(converted == value) {
                    this.Write(converted);
                    return;
                }
            }
            this.InternalWriteString(value.ToString(FORMAT));
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(double value) {
            // convert to long for shorter encoding (ie: 0.0 -> 0)
            if(value >= long.MinValue && value <= long.MaxValue) {
                long converted = (long)value;
                if(converted == value) {
                    this.Write(converted);
                    return;
                }
            }

            // todo: implement grisu3
            this.InternalWriteString(value.ToString(FORMAT));
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(float value) {
            // convert to long for shorter encoding (ie: 0.0 -> 0)
            if(value >= long.MinValue && value <= long.MaxValue) {
                long converted = (long)value;
                if(converted == value) {
                    this.Write(converted);
                    return;
                }
            }

            // todo: implement grisu3
            this.InternalWriteString(value.ToString(FORMAT));
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(byte[] value) {
            if(value != null) {
                m_buffer[m_offset++] = (byte)'\"';
                HexEncode(m_buffer, ref m_offset, m_stream, value, 0, value.Length);
                m_buffer[m_offset++] = (byte)'\"';
                this.TryFlushBuffer();
            } else
                this.WriteNull();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(Stream value) {
            if(value != null) {
                m_buffer[m_offset++] = (byte)'\"';

                const int BUFFER_SIZE = 65535;

                int read;
                int size = value.CanSeek ? unchecked((int)Math.Min(value.Length - value.Position, BUFFER_SIZE)) : BUFFER_SIZE;
                var buffer = new byte[size];

                while((read = value.Read(buffer, 0, size)) > 0)
                    HexEncode(m_buffer, ref m_offset, m_stream, buffer, 0, read);

                m_buffer[m_offset++] = (byte)'\"';

                this.TryFlushBuffer();
            } else
                this.WriteNull();
        }
        [MethodImpl(AggressiveInlining)]
        public void Write(object value) {
            if(value == null) {
                // intentionally do nothing
                //this.WriteNull();
            } else if(value is int a)
                this.Write(a);
            else if(value is long b)
                this.Write(b);
            else if(value is double c)
                this.Write(c);
            else if(value is decimal d)
                this.Write(d);
            else if(value is float e)
                this.Write(e);
            else if(value is bool f)
                this.Write(f);
            else if(value is ulong g)
                this.Write(g);
            else if(value is string h)
                this.Write(h);
            else if(value is byte i)
                this.Write(i);
            else if(value is uint j)
                this.Write(j);
            else if(value is ushort k)
                this.Write(k);
            else if(value is short l)
                this.Write(l);
            else if(value is sbyte m)
                this.Write(m);
            else if(value is DateTime n)
                this.Write(n);
            else if(value is TimeSpan o)
                this.Write(o);
            else if(value is byte[] p)
                this.Write(p);
            else if(value is Stream q)
                this.Write(q);
            else
                throw new NotSupportedException();
        }
        #endregion
        #region WriteComma()
        [MethodImpl(AggressiveInlining)]
        public void WriteComma() {
            this.WritePassthrough(',');
        }
        #endregion
        #region WriteLine()
        [MethodImpl(AggressiveInlining)]
        public void WriteLine() {
            //dont use Environment.NewLine because we want consistent results regardless of environment
            this.WritePassthrough('\r');
            this.WritePassthrough('\n');
        }
        [MethodImpl(AggressiveInlining)]
        public void WriteLine(object[] values) {
            int count = values.Length;
            for(int i = 0; i < count; i++) {
                if(i != 0)
                    this.WriteComma();
                this.Write(values[i]);
            }
            this.WriteLine();
        }
        #endregion
        #region WriteNull()
        [MethodImpl(AggressiveInlining)]
        public void WriteNull() {
            m_buffer[m_offset + 0] = (byte)'n';
            m_buffer[m_offset + 1] = (byte)'u';
            m_buffer[m_offset + 2] = (byte)'l';
            m_buffer[m_offset + 3] = (byte)'l';
            m_offset += 4;
            this.TryFlushBuffer();
        }
        #endregion
        #region WritePassthrough()
        /// <summary>
        ///     Writes the value directly to the stream without any convertions.
        ///     This is useful for things like ',' or new lines.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public void WritePassthrough(byte value) {
            m_buffer[m_offset++] = value;
            this.TryFlushBuffer();
        }
        /// <summary>
        ///     Writes the value directly (in UTF-8) to the stream without any convertions.
        ///     This is useful for things like ',' or new lines.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public void WritePassthrough(char value) {
            // UTF8 encodes verbatim the first 128 characters
            if(value < 128)
                m_buffer[m_offset++] = unchecked((byte)value);
            else {
                byte[] raw = new byte[4];
                int len = Encoding.UTF8.GetBytes(value.ToString(), 0, 1, raw, 0);
                for(int i = 0; i < len; i++)
                    m_buffer[m_offset++] = raw[i];
            }
            this.TryFlushBuffer();
        }
        /// <summary>
        ///     Writes the value directly (in UTF-8) to the stream without any convertions.
        ///     This will not, for instance, double quote, or add quotes or any of that.
        ///     This is useful for things like ',' or new lines.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public void WritePassthrough(string value) {
            this.InternalWriteString(value);
        }
        /// <summary>
        ///     Writes the value directly to the stream without any convertions.
        ///     This is useful for things like ',' or new lines.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public void WritePassthrough(byte[] buffer, int offset, int count) {
            if(count > 0) {
                var write = Math.Min(count, MAX_BUFFER_SIZE - m_offset);
                count -= write;
                Buffer.BlockCopy(buffer, offset, m_buffer, m_offset, write);
                offset += write;
                m_offset += write;
                this.TryFlushBuffer();
            }

            if(count >= MIN_COMMIT_BUFFER_SIZE)
                m_stream.Write(buffer, offset, count);
            else if(count > 0) {
                Buffer.BlockCopy(buffer, offset, m_buffer, 0, count);
                m_offset = count;
            }
        }
        #endregion
        
        #region Flush()
        public void Flush() {
            if(m_offset > 0) {
                m_stream.Write(m_buffer, 0, m_offset);
                m_stream.Flush();
                m_offset = 0;
            }
        }
        #endregion
        #region Close()
        public void Close() {
            this.Flush();
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

        #region private TryFlushBuffer()
        [MethodImpl(AggressiveInlining)]
        private void TryFlushBuffer() {
            if(m_offset >= MIN_COMMIT_BUFFER_SIZE) {
                m_stream.Write(m_buffer, 0, m_offset);
                m_offset = 0;
            }
        }
        #endregion

        #region private InternalWriteString()
        /// <summary>
        ///     Writes the string directly in UTF-8, with no convertion whatsoever.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private void InternalWriteString(string value) {
            int charIndex = 0;
            int remaining = value.Length;
            var encoder = Encoding.UTF8;

            while(remaining > 0) {
                int encodedChars = Math.Min(remaining, CHAR_BUFFER_SIZE);

                var writtenBytes = encoder.GetBytes(value, charIndex, encodedChars, m_buffer, m_offset);

                m_offset += writtenBytes;
                remaining -= encodedChars;
                charIndex += encodedChars;

                if(m_offset >= MIN_COMMIT_BUFFER_SIZE) {
                    m_stream.Write(m_buffer, 0, m_offset);
                    m_offset = 0;
                }
            }
        }
        #endregion
        #region private InternalWriteStringDoubleQuote()
        /// <summary>
        ///     Writes the string directly in UTF-8, with " before and after the string, and 'escape' double-quotes by doubling them.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private void InternalWriteStringDoubleQuote(string value) {
            m_buffer[m_offset++] = (byte)'\"';
            this.TryFlushBuffer(); // yep, really, the code assumes you dont have a full buffer before doing those writes, so flush it to be safe

            int charIndex = 0;
            int remaining = value.Length;
            var encoder = Encoding.UTF8;

            while(remaining > 0) {
                bool escaped_char_found = false;
                int encodedChars = Math.Min(remaining, CHAR_BUFFER_SIZE);

                for(int i = 0; i < encodedChars; i++) {
                    if(value[charIndex + i] == '\"') {
                        escaped_char_found = true;
                        encodedChars = i;
                        break;
                    }
                }

                if(encodedChars > 0) {
                    var writtenBytes = encoder.GetBytes(value, charIndex, encodedChars, m_buffer, m_offset);

                    m_offset += writtenBytes;
                    remaining -= encodedChars;
                    charIndex += encodedChars;
                }

                if(escaped_char_found) {
                    m_buffer[m_offset + 0] = (byte)'\"';
                    m_buffer[m_offset + 1] = (byte)'\"';
                    m_offset += 2;
                    remaining--;
                    charIndex++;
                }

                if(m_offset >= MIN_COMMIT_BUFFER_SIZE) {
                    m_stream.Write(m_buffer, 0, m_offset);
                    m_offset = 0;
                }
            }

            m_buffer[m_offset++] = (byte)'\"';
            this.TryFlushBuffer();
        }
        #endregion
        #region private InternalWriteTimeSpan()
        [MethodImpl(AggressiveInlining)]
        private void InternalWriteTimeSpan(TimeSpan value, bool writeDays) {
            // constant format
            // 00:00:00, 3.17:25:30.5000000

            // this could be sped up with fast_itoa() code to avoid some of the divisions

            if(value < TimeSpan.Zero)
                m_buffer[m_offset++] = (byte)'-';

            int temp;
            if(writeDays) {
                temp = Math.Abs(value.Days);
                if(temp > 0) {
                    Fast_ItoA(m_buffer, ref m_offset, temp);
                    m_buffer[m_offset++] = (byte)'.';
                }
            }

            temp = Math.Abs(value.Hours);
            m_buffer[m_offset + 0] = (byte)('0' + (temp / 10));
            m_buffer[m_offset + 1] = (byte)('0' + (temp % 10));
            m_buffer[m_offset + 2] = (byte)':';
            temp = Math.Abs(value.Minutes);
            m_buffer[m_offset + 3] = (byte)('0' + (temp / 10));
            m_buffer[m_offset + 4] = (byte)('0' + (temp % 10));
            m_buffer[m_offset + 5] = (byte)':';
            temp = Math.Abs(value.Seconds);
            m_buffer[m_offset + 6] = (byte)('0' + (temp / 10));
            m_buffer[m_offset + 7] = (byte)('0' + (temp % 10));
            m_offset += 8;

            temp = unchecked((int)(Math.Abs(value.Ticks) % (TimeSpan.TicksPerMillisecond * 1000)));
            if(temp != 0) {
                m_buffer[m_offset + 0] = (byte)'.';
                m_buffer[m_offset + 7] = (byte)('0' + (temp % 10));    temp /= 10;
                m_buffer[m_offset + 6] = (byte)('0' + (temp % 10));    temp /= 10;
                m_buffer[m_offset + 5] = (byte)('0' + (temp % 10));    temp /= 10;
                m_buffer[m_offset + 4] = (byte)('0' + (temp % 10));    temp /= 10;
                m_buffer[m_offset + 3] = (byte)('0' + (temp % 10));    temp /= 10;
                m_buffer[m_offset + 2] = (byte)('0' + (temp % 10));    temp /= 10;
                m_buffer[m_offset + 1] = (byte)('0' + (temp % 10));

                m_offset += 8;
            }

            this.TryFlushBuffer();
        }
        #endregion

        // BitMethods imported functions
        #region private static CountDigits10()
        /// <summary>
        ///     Returns the number of characters needed to represent the number.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static int CountDigits10(byte value) {
            if(value < 10)  return 1;
            if(value < 100) return 2;
            return 3;
        }
        /// <summary>
        ///     Returns the number of characters needed to represent the number.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static int CountDigits10(ushort value) {
            if(value < 10)    return 1;
            if(value < 100)   return 2;
            if(value < 1000)  return 3;
            if(value < 10000) return 4;
            return 5;
        }
        /// <summary>
        ///     Returns the number of characters needed to represent the number.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static int CountDigits10(uint value) {
            if(value < 10)   return 1;
            if(value < 100)  return 2;
            if(value < 1000) return 3;

            if(value < 10000000) { // 4-7
                if(value < 100000)
                    return value < 10000 ? 4 : 5;
                else
                    return value < 1000000 ? 6 : 7;
            } else { // 8-10
                if(value < 1000000000)
                    return value < 100000000 ? 8 : 9;
                else
                    return 10;
            }
        }
        /// <summary>
        ///     Returns the number of characters needed to represent the number.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static int CountDigits10(ulong value) {
            if(value < 10)    return 1;
            if(value < 100)   return 2;
            if(value < 1000)  return 3;
            if(value < 10000) return 4;

            if(value < 1000000000000) { // 5-12
                if(value < 100000000) { // 5-8
                    if(value < 1000000)
                        return value < 100000 ? 5 : 6;
                    else
                        return value < 10000000 ? 7 : 8;
                } else { // 9-12
                    if(value < 10000000000)
                        return value < 1000000000 ? 9 : 10;
                    else
                        return value < 100000000000 ? 11 : 12;
                }
            } else { // 13-20
                if(value < 10000000000000000) { // 13-16
                    if(value < 100000000000000)
                        return value < 10000000000000 ? 13 : 14;
                    else
                        return value < 1000000000000000 ? 15 : 16;
                } else { // 17-20
                    if(value < 1000000000000000000)
                        return value < 100000000000000000 ? 17 : 18;
                    else
                        return value < 10000000000000000000 ? 19 : 20;
                }
            }
        }
        #endregion
        #region private static Fast_ItoA()
        // written this way to help the compiler see it as a const
        private static readonly byte[] ITOA_DECIMALS_BYTES = new byte[] {
            (byte)'0',(byte)'0',  (byte)'0',(byte)'1',  (byte)'0',(byte)'2',  (byte)'0',(byte)'3',  (byte)'0',(byte)'4',  (byte)'0',(byte)'5',  (byte)'0',(byte)'6',  (byte)'0',(byte)'7',  (byte)'0',(byte)'8',  (byte)'0',(byte)'9',
            (byte)'1',(byte)'0',  (byte)'1',(byte)'1',  (byte)'1',(byte)'2',  (byte)'1',(byte)'3',  (byte)'1',(byte)'4',  (byte)'1',(byte)'5',  (byte)'1',(byte)'6',  (byte)'1',(byte)'7',  (byte)'1',(byte)'8',  (byte)'1',(byte)'9',
            (byte)'2',(byte)'0',  (byte)'2',(byte)'1',  (byte)'2',(byte)'2',  (byte)'2',(byte)'3',  (byte)'2',(byte)'4',  (byte)'2',(byte)'5',  (byte)'2',(byte)'6',  (byte)'2',(byte)'7',  (byte)'2',(byte)'8',  (byte)'2',(byte)'9',
            (byte)'3',(byte)'0',  (byte)'3',(byte)'1',  (byte)'3',(byte)'2',  (byte)'3',(byte)'3',  (byte)'3',(byte)'4',  (byte)'3',(byte)'5',  (byte)'3',(byte)'6',  (byte)'3',(byte)'7',  (byte)'3',(byte)'8',  (byte)'3',(byte)'9',
            (byte)'4',(byte)'0',  (byte)'4',(byte)'1',  (byte)'4',(byte)'2',  (byte)'4',(byte)'3',  (byte)'4',(byte)'4',  (byte)'4',(byte)'5',  (byte)'4',(byte)'6',  (byte)'4',(byte)'7',  (byte)'4',(byte)'8',  (byte)'4',(byte)'9',
            (byte)'5',(byte)'0',  (byte)'5',(byte)'1',  (byte)'5',(byte)'2',  (byte)'5',(byte)'3',  (byte)'5',(byte)'4',  (byte)'5',(byte)'5',  (byte)'5',(byte)'6',  (byte)'5',(byte)'7',  (byte)'5',(byte)'8',  (byte)'5',(byte)'9',
            (byte)'6',(byte)'0',  (byte)'6',(byte)'1',  (byte)'6',(byte)'2',  (byte)'6',(byte)'3',  (byte)'6',(byte)'4',  (byte)'6',(byte)'5',  (byte)'6',(byte)'6',  (byte)'6',(byte)'7',  (byte)'6',(byte)'8',  (byte)'6',(byte)'9',
            (byte)'7',(byte)'0',  (byte)'7',(byte)'1',  (byte)'7',(byte)'2',  (byte)'7',(byte)'3',  (byte)'7',(byte)'4',  (byte)'7',(byte)'5',  (byte)'7',(byte)'6',  (byte)'7',(byte)'7',  (byte)'7',(byte)'8',  (byte)'7',(byte)'9',
            (byte)'8',(byte)'0',  (byte)'8',(byte)'1',  (byte)'8',(byte)'2',  (byte)'8',(byte)'3',  (byte)'8',(byte)'4',  (byte)'8',(byte)'5',  (byte)'8',(byte)'6',  (byte)'8',(byte)'7',  (byte)'8',(byte)'8',  (byte)'8',(byte)'9',
            (byte)'9',(byte)'0',  (byte)'9',(byte)'1',  (byte)'9',(byte)'2',  (byte)'9',(byte)'3',  (byte)'9',(byte)'4',  (byte)'9',(byte)'5',  (byte)'9',(byte)'6',  (byte)'9',(byte)'7',  (byte)'9',(byte)'8',  (byte)'9',(byte)'9',
        };
        /// <summary>
        ///     Fast integer-to-ascii.
        /// </summary>
        private static void Fast_ItoA(byte[] buffer, ref int offset, byte value) {
            int writeIndex = CountDigits10(value);
            offset += writeIndex;
            writeIndex = offset - 1;

            if(value >= 100) {
                int index = (value % 100) << 1;
                value /= 100;
                buffer[writeIndex - 1] = ITOA_DECIMALS_BYTES[index + 0];
                buffer[writeIndex - 0] = ITOA_DECIMALS_BYTES[index + 1];
                writeIndex -= 2;
            }

            if(value < 10)
                buffer[writeIndex] = unchecked((byte)('0' + value));
            else {
                int index = value << 1;
                buffer[writeIndex - 1] = ITOA_DECIMALS_BYTES[index + 0];
                buffer[writeIndex - 0] = ITOA_DECIMALS_BYTES[index + 1];
            }
        }
        /// <summary>
        ///     Fast integer-to-ascii.
        /// </summary>
        private static void Fast_ItoA(byte[] buffer, ref int offset, ushort value) {
            int writeIndex = CountDigits10(value);
            offset += writeIndex;
            writeIndex = offset - 1;

            while(value >= 100) {
                int index = (value % 100) << 1;
                value /= 100;
                buffer[writeIndex - 1] = ITOA_DECIMALS_BYTES[index + 0];
                buffer[writeIndex - 0] = ITOA_DECIMALS_BYTES[index + 1];
                writeIndex -= 2;
            }

            if(value < 10)
                buffer[writeIndex] = unchecked((byte)('0' + value));
            else {
                int index = value << 1;
                buffer[writeIndex - 1] = ITOA_DECIMALS_BYTES[index + 0];
                buffer[writeIndex - 0] = ITOA_DECIMALS_BYTES[index + 1];
            }
        }
        /// <summary>
        ///     Fast integer-to-ascii.
        /// </summary>
        private static void Fast_ItoA(byte[] buffer, ref int offset, uint value) {
            int writeIndex = CountDigits10(value);
            offset += writeIndex;
            writeIndex = offset - 1;

            while(value >= 100) {
                var index = (value % 100) << 1;
                value /= 100;
                buffer[writeIndex - 1] = ITOA_DECIMALS_BYTES[index + 0];
                buffer[writeIndex - 0] = ITOA_DECIMALS_BYTES[index + 1];
                writeIndex -= 2;
            }

            if(value < 10)
                buffer[writeIndex] = unchecked((byte)('0' + value));
            else {
                var index = value << 1;
                buffer[writeIndex - 1] = ITOA_DECIMALS_BYTES[index + 0];
                buffer[writeIndex - 0] = ITOA_DECIMALS_BYTES[index + 1];
            }
        }
        /// <summary>
        ///     Fast integer-to-ascii.
        /// </summary>
        private static void Fast_ItoA(byte[] buffer, ref int offset, ulong value) {
            int writeIndex = CountDigits10(value);
            offset += writeIndex;
            writeIndex = offset - 1;

            while(value >= 100) {
                var index = (value % 100) << 1;
                value /= 100;
                buffer[writeIndex - 1] = ITOA_DECIMALS_BYTES[index + 0];
                buffer[writeIndex - 0] = ITOA_DECIMALS_BYTES[index + 1];
                writeIndex -= 2;
            }

            if(value < 10)
                buffer[writeIndex] = unchecked((byte)('0' + value));
            else {
                var index = value << 1;
                buffer[writeIndex - 1] = ITOA_DECIMALS_BYTES[index + 0];
                buffer[writeIndex - 0] = ITOA_DECIMALS_BYTES[index + 1];
            }
        }
        /// <summary>
        ///     Fast integer-to-ascii.
        /// </summary>
        private static void Fast_ItoA(byte[] buffer, ref int offset, sbyte value) {
            if(value < 0) {
                value = unchecked((sbyte)-value);
                buffer[offset++] = (byte)'-';
            }
            int writeIndex = CountDigits10(unchecked((byte)value));
            offset += writeIndex;
            writeIndex = offset - 1;

            if(value >= 100) {
                int index = (value % 100) << 1;
                value /= 100;
                buffer[writeIndex - 1] = ITOA_DECIMALS_BYTES[index + 0];
                buffer[writeIndex - 0] = ITOA_DECIMALS_BYTES[index + 1];
                writeIndex -= 2;
            }

            if(value < 10)
                buffer[writeIndex] = unchecked((byte)('0' + value));
            else {
                int index = value << 1;
                buffer[writeIndex - 1] = ITOA_DECIMALS_BYTES[index + 0];
                buffer[writeIndex - 0] = ITOA_DECIMALS_BYTES[index + 1];
            }
        }
        /// <summary>
        ///     Fast integer-to-ascii.
        /// </summary>
        private static void Fast_ItoA(byte[] buffer, ref int offset, short value) {
            if(value < 0) {
                value = unchecked((short)-value);
                buffer[offset++] = (byte)'-';
            }
            int writeIndex = CountDigits10(unchecked((ushort)value));
            offset += writeIndex;
            writeIndex = offset - 1;

            while(value >= 100) {
                int index = (value % 100) << 1;
                value /= 100;
                buffer[writeIndex - 1] = ITOA_DECIMALS_BYTES[index + 0];
                buffer[writeIndex - 0] = ITOA_DECIMALS_BYTES[index + 1];
                writeIndex -= 2;
            }

            if(value < 10)
                buffer[writeIndex] = unchecked((byte)('0' + value));
            else {
                int index = value << 1;
                buffer[writeIndex - 1] = ITOA_DECIMALS_BYTES[index + 0];
                buffer[writeIndex - 0] = ITOA_DECIMALS_BYTES[index + 1];
            }
        }
        /// <summary>
        ///     Fast integer-to-ascii.
        /// </summary>
        private static void Fast_ItoA(byte[] buffer, ref int offset, int value) {
            if(value < 0) {
                value = -value;
                buffer[offset++] = (byte)'-';
            }
            int writeIndex = CountDigits10(unchecked((uint)value));
            offset += writeIndex;
            writeIndex = offset - 1;

            while(value >= 100) {
                int index = (value % 100) << 1;
                value /= 100;
                buffer[writeIndex - 1] = ITOA_DECIMALS_BYTES[index + 0];
                buffer[writeIndex - 0] = ITOA_DECIMALS_BYTES[index + 1];
                writeIndex -= 2;
            }

            if(value < 10)
                buffer[writeIndex] = unchecked((byte)('0' + value));
            else {
                int index = value << 1;
                buffer[writeIndex - 1] = ITOA_DECIMALS_BYTES[index + 0];
                buffer[writeIndex - 0] = ITOA_DECIMALS_BYTES[index + 1];
            }
        }
        /// <summary>
        ///     Fast integer-to-ascii.
        /// </summary>
        private static void Fast_ItoA(byte[] buffer, ref int offset, long value) {
            if(value < 0) {
                value = -value;
                buffer[offset++] = (byte)'-';
            }
            int writeIndex = CountDigits10(unchecked((ulong)value));
            offset += writeIndex;
            writeIndex = offset - 1;

            while(value >= 100) {
                var index = (value % 100) << 1;
                value /= 100;
                buffer[writeIndex - 1] = ITOA_DECIMALS_BYTES[index + 0];
                buffer[writeIndex - 0] = ITOA_DECIMALS_BYTES[index + 1];
                writeIndex -= 2;
            }

            if(value < 10)
                buffer[writeIndex] = unchecked((byte)('0' + value));
            else {
                var index = value << 1;
                buffer[writeIndex - 1] = ITOA_DECIMALS_BYTES[index + 0];
                buffer[writeIndex - 0] = ITOA_DECIMALS_BYTES[index + 1];
            }
        }
        #endregion
        #region private static HexEncode()
        /// <summary>
        ///     Writes the bytes in hexadecimal format, with no prepending of any kind (0x) and in uppercase.
        /// </summary>
        //[MethodImpl(AggressiveInlining)] // most likely a slowdown if called on many places, due to worse branch prediction if enabled
        private static void HexEncode(byte[] destBuffer, ref int destOffset, Stream destStream, byte[] sourceBuffer, int sourceOffset, int count) {
            int write_buffer_size = destBuffer.Length;

            while(count > 0) {
                int read = Math.Min(write_buffer_size - destOffset, count << 1) >> 1;

                count -= read;

                while(read-- > 0) {
                    int rawByte = sourceBuffer[sourceOffset++];

                    int low = rawByte & 0x0F;
                    int high = rawByte >> 4;

                    destBuffer[destOffset + 0] = high < 10 ? unchecked((byte)('0' + high)) : unchecked((byte)('A' + high - 10));
                    destBuffer[destOffset + 1] = low < 10  ? unchecked((byte)('0' + low))  : unchecked((byte)('A' + low - 10));
                    destOffset += 2;
                }

                // if we can't fit one hex-encoded item, then flush
                if(destOffset >= write_buffer_size - 1) {
                    destStream.Write(destBuffer, 0, destOffset);
                    destOffset = 0;
                }
            }
        }
        #endregion
    }
}
