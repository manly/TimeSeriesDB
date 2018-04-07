using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Linq;


namespace TimeSeriesDB
{
    /// <summary>
    ///     Immutable Name + tags + columns data.
    ///     Represents a series of data points (without the data points themselves).
    ///     The tag ordering is not important.
    ///     
    ///     [optional metric-name] [tag1]=[tag-value1] [tag2]=[tag-value2]...[tagN]=[tag-valueN]
    ///     [optional column1_name] [Type],[optional bit_size],[optional encoding_type],[optional compression_setting]    (ColumnDefinition)
    /// </summary>
    /// <example>
    ///     colevel room=42 building=2 floor=1 wing=East sensor=A
    ///     TimeStamp datetime,64,DeltaDelta,NoCompress
    ///     Value string,[var],[None],[zstd.level=10]
    ///     DummyColumn int
    /// </example>
    [DebuggerDisplay("{ToDebuggerDisplayString()}")]
    public sealed class DataSerieDefinition : IEquatable<DataSerieDefinition> {
        #region constructors
        private DataSerieDefinition() { }
        /// <param name="serialized">See class example.</param>
        public DataSerieDefinition(string serialized) {
            this.InternalParse(serialized, true);
        }
        public DataSerieDefinition(DataSerieDefinition serie) : this(serie.ToString()) { }
        public DataSerieDefinition(DataSerieDefinition serie, IEnumerable<ColumnDefinition> columns) : this() {
            this.InternalParse(serie.ToString(false), false);
            this.Columns = columns.ToArray();
        }
        #endregion

        /// <summary>
        ///     The name of the data serie.
        ///     Optional.
        ///     Cannot contain ' ', '=' or '\n'.
        /// </summary>
        public string Name { get; private set; }
        public Dictionary<string, string> Tags { get; private set; }
        public ColumnDefinition[] Columns { get; private set; }

        /// <summary>
        ///     The unique ID of this serie.
        ///     This is used by the Page for quick filtering.
        /// </summary>
        public uint UniqueID { get; set; }

        #region static Parse()
        /// <param name="serialized">See class example.</param>
        public static DataSerieDefinition Parse(string serialized) {
            return new DataSerieDefinition(serialized);
        }
        /// <param name="serialized">See class example.</param>
        private void InternalParse(string serialized, bool parseColumns) {
            if(string.IsNullOrEmpty(serialized))
                throw new ArgumentNullException(nameof(serialized), "The serie is missing the column definitions.");

            var lines = serialized.Split(new[] { '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);
            if(parseColumns && lines.Length == 1)
                throw new FormatException("The serie is missing the column definitions.");

            // read first line (serie name + tags)
            if(!string.IsNullOrEmpty(lines[0])){
                bool containsMetricName = false;
                var split = lines[0].Trim(' ').Split(new[] { ' ' }, StringSplitOptions.None);
                var count = split.Length;

                for(int i = 0; i < count; i++) {
                    var current = split[i];

                    if(string.IsNullOrEmpty(current))
                        throw new FormatException($"Found 1+ extraneous space in \"{serialized}\".");

                    int equalSignCount = 0;
                    for(int j = 0; j < current.Length; j++) {
                        if(current[j] == '=')
                            equalSignCount++;
                    }
                    if(equalSignCount != 1) {
                        if(i == 0 && equalSignCount == 0)
                            containsMetricName = true;
                        else
                            throw new FormatException($"Tag \"{current}\" must be in format '<tag-name>=<tag-value>'.");
                    }
                    if(current[0] == '=')
                        throw new FormatException($"Tag \"{current}\" must be in format '<tag-name>=<tag-value>'. The Tag.Name cannot be empty.");
                    if(current[current.Length - 1] == '=')
                        throw new FormatException($"Tag \"{current}\" must be in format '<tag-name>=<tag-value>'. The Tag.Value cannot be empty.");
                }

                this.Name = containsMetricName ? split[0] : null;
                this.Tags = new Dictionary<string, string>(count - (containsMetricName ? 1 : 0));

                for(int i = containsMetricName ? 1 : 0; i < count; i++) {
                    var key_value = split[i].Split('=');
                    this.Tags.Add(key_value[0], key_value[1]);
                }
            } else {
                this.Name = null;
                this.Tags = new Dictionary<string, string>(0);
            }

            // read columns
            if(parseColumns) {
                this.Columns = new ColumnDefinition[lines.Length - 1];
                for(int i = 1; i < lines.Length; i++)
                    this.Columns[i - 1] = ColumnDefinition.Parse(lines[i]);
            }
        }
        #endregion
        #region static ChangeCompression()
        /// <summary>
        ///     Changes the compression used by channels.
        ///     This will reset the UniqueID as that uniqueid is expected to somewhat match the hash of the serialized serie
        ///     Make sure not to do this
        /// </summary>
        public static DataSerieDefinition ChangeCompression(DataSerieDefinition value, CompressionSetting compression = null) {
            // must copy because if that serie is set on a page, the page header size can change meaning write positions are off
            var res = new DataSerieDefinition(value) {
                // reset the unique id because the hash of the serialize serie will not match anymore
                UniqueID = 0,
            };

            for(int i=0; i<res.Columns.Length; i++)
                res.Columns[i] = ColumnDefinition.ChangeCompression(res.Columns[i], compression);

            return res;
        }
        #endregion

        // IEquatable<>
        #region Equals()
        public bool Equals(DataSerieDefinition other) {
            if(other is null)
                return false;
            if(this.UniqueID != other.UniqueID ||
                this.Columns.Length != other.Columns.Length ||
                this.Tags.Count != other.Tags.Count ||
                string.CompareOrdinal(this.Name, other.Name) != 0)
                return false;

            // check columns
            //if(!this.Columns.SequenceEqual(other.Columns)) return false;
            int count = this.Columns.Length;
            for(int i = 0; i < count; i++) {
                if(this.Columns[i] != other.Columns[i]) // force usage of IEquatable<>
                    return false;
            }

            // check tags
            var tags2 = other.Tags.GetEnumerator();
            foreach(var tag in this.Tags) {
                if(!tags2.MoveNext())
                    return false;
                if(string.CompareOrdinal(tag.Key, tags2.Current.Key) != 0 ||
                    string.CompareOrdinal(tag.Value, tags2.Current.Value) != 0)
                    return false;
            }
            if(tags2.MoveNext())
                return false;
            return true;
        }
        public override bool Equals(object obj) {
            if(obj == null || !(obj is DataSerieDefinition))
                return false;
            return this.Equals((DataSerieDefinition)obj);
        }
        #endregion
        #region GetHashCode()
        public override int GetHashCode() {
            int hash = 
                (this.Name?.GetHashCode() ?? 0) ^
                this.UniqueID.GetHashCode() ^
                this.Tags.Count.GetHashCode() ^
                this.Columns.Length.GetHashCode();

            int count = this.Columns.Length;
            for(int i = 0; i < count; i++)
                hash ^= this.Columns[i].GetHashCode();

            foreach(var tag in this.Tags)
                hash ^= tag.Key.GetHashCode() ^ (tag.Value?.GetHashCode() ?? 0);

            return hash;
        }
        #endregion
        #region operator ==
        public static bool operator ==(DataSerieDefinition item1, DataSerieDefinition item2) {
            if(item1 is null)
                return item2 is null;
            return item1.Equals(item2);
        }
        #endregion
        #region operator !=
        public static bool operator !=(DataSerieDefinition item1, DataSerieDefinition item2) {
            return !(item1 == item2);
        }
        #endregion

        #region ToString()
        private string m_toStringFull = null;
        private string m_toStringNoColumns = null;

        public override string ToString() {
            return this.ToString(true);
        }
        public string ToString(bool includeColumns) {
            if(m_toStringFull == null) {
                int length = (this.Name != null ? this.Name.Length : 0);
                if(this.Tags != null) {
                    if(string.IsNullOrEmpty(this.Name) && this.Tags.Count > 0)
                        length--;
                    foreach(var item in this.Tags)
                        length += item.Key.Length + item.Value.Length + 2;
                }

                var columns = new string[this.Columns.Length];
                for(int i = 0; i < columns.Length; i++) {
                    columns[i] = this.Columns[i].ToString();
                    length += columns[i].Length + 1;
                }

                var sb = new StringBuilder(length);

                // generate first line
                // code is special to handle the case where no metric name is specified
                // ex: "colevel=12" (ie: name is skipped)
                if(!string.IsNullOrEmpty(this.Name))
                    sb.Append(this.Name);
                if(this.Tags != null) {
                    bool first = true;
                    foreach(var item in this.Tags) {
                        if(first) {
                            first = false;
                            if(!string.IsNullOrEmpty(this.Name))
                                sb.Append(' ');
                        } else
                            sb.Append(' ');

                        sb.Append(item.Key);
                        sb.Append('=');
                        sb.Append(item.Value);
                    }
                }

                m_toStringNoColumns = sb.ToString();

                // generate column definitions
                for(int i = 0; i < columns.Length; i++) {
                    sb.Append('\n');
                    sb.Append(columns[i]);
                }

                m_toStringFull = sb.ToString();
            }

            // since this class is immutable and we are likely to re-encode often this data in every page,
            // we cache the tostring
            return includeColumns ? m_toStringFull : m_toStringNoColumns;
        }
        #endregion
        #region private ToDebuggerDisplayString()
        private string ToDebuggerDisplayString() {
            var sb = new StringBuilder();
            sb.Append("[id=");
            sb.Append(this.UniqueID);
            sb.Append(", name=");
            sb.Append(this.Name);
            sb.Append("] tags=... ");
            bool first = true;
            foreach(var item in this.Columns) {
                if(!first)
                    sb.Append(", ");
                first = false;
                sb.Append('{');
                sb.Append(item.ToString());
                sb.Append('}');
            }
            return sb.ToString();
        }
        #endregion
    }
}
