using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.IO;
using System.Globalization;


namespace TimeSeriesDB
{
    [DebuggerDisplay("{ToDebuggerDisplayString()}")]
    public class CompressionSetting : IEquatable<CompressionSetting> {
        private static readonly Dictionary<string, CompressionAlgorithm> COMPRESSIONTYPE_PARSE_STRINGS = new [] { 
            new { Code="NoCompress,NoCompression,None", Value=CompressionAlgorithm.NoCompress, },
            new { Code="zlib,gzip",                     Value=CompressionAlgorithm.zlib, },
            new { Code="lz4",                           Value=CompressionAlgorithm.lz4, },
            new { Code="zstd",                          Value=CompressionAlgorithm.zstd, },
        }.SelectMany(o => o.Code.Split(',').Select(k => new { Code = k.ToLowerInvariant(), Value = o.Value }))
        .ToDictionary(o => o.Code, o => o.Value);

        private static readonly Dictionary<CompressionAlgorithm, string> DEFAULT_COMPRESSION_LEVELS = new [] { 
            new { Alg=CompressionAlgorithm.NoCompress, Level="" },
            new { Alg=CompressionAlgorithm.zlib,       Level=System.IO.Compression.CompressionLevel.Fastest.ToString()},
            new { Alg=CompressionAlgorithm.lz4,        Level=Lz4Net.Lz4Mode.Fast.ToString()},
            new { Alg=CompressionAlgorithm.zstd,       Level="3" },
        }.ToDictionary(o => o.Alg, o => o.Level);

        public static CompressionSetting None = new CompressionSetting(CompressionAlgorithm.NoCompress, null);


        public readonly CompressionAlgorithm Algorithm;
        public readonly string Level;

        public CompressionSetting() {
            this.Algorithm  = CompressionAlgorithm.NoCompress;
            this.Level = DEFAULT_COMPRESSION_LEVELS[CompressionAlgorithm.NoCompress];
        }
        /// <param name="type">Default: NoCompress</param>
        public CompressionSetting(CompressionAlgorithm type) {
            this.Algorithm  = type;
            this.Level = DEFAULT_COMPRESSION_LEVELS[type];
        }
        /// <param name="type">Default: NoCompress</param>
        public CompressionSetting(CompressionAlgorithm type, string level) {
            if(!string.IsNullOrEmpty(level) && level.Contains(','))
                throw new ArgumentException(nameof(level), $"{this.GetType().Name} cannot have {nameof(level)}.Contains(',').");

            this.Algorithm = type;
            this.Level = level;
        }

        /// <param name="value">ex: 'zstd.level=3'</param>
        public static CompressionSetting Parse(string value) {
            const string LEVEL_PARAM = ".level=";

            string typeString;
            string level;
            int levelIndex = value.IndexOf(LEVEL_PARAM, 0, value.Length, StringComparison.OrdinalIgnoreCase);

            if(levelIndex >= 0) {
                typeString = value.Substring(0, levelIndex);
                level = value.Substring(levelIndex + LEVEL_PARAM.Length);
            } else {
                typeString = value;
                level = null;
            }

            if(!COMPRESSIONTYPE_PARSE_STRINGS.TryGetValue(typeString.ToLowerInvariant(), out CompressionAlgorithm type))
                throw new FormatException($"\"{value}\" does not contain a valid {nameof(CompressionAlgorithm)}.");

            if(levelIndex < 0)
                level = DEFAULT_COMPRESSION_LEVELS[type];
            
            return new CompressionSetting(type, level);
        }


        public Stream CreateReadStream(Stream stream) {
            const bool LEAVE_OPEN = true;

            switch(this.Algorithm) {
                case CompressionAlgorithm.NoCompress:
                    return stream;
                case CompressionAlgorithm.zstd:
                    return new Zstandard.Net.ZstandardStream(stream, System.IO.Compression.CompressionMode.Decompress, LEAVE_OPEN);
                case CompressionAlgorithm.lz4:
                    return new Lz4Net.Lz4DecompressionStream(stream, LEAVE_OPEN);
                case CompressionAlgorithm.zlib:
                    return new System.IO.Compression.GZipStream(stream, System.IO.Compression.CompressionMode.Decompress, LEAVE_OPEN);
                default:
                    throw new NotImplementedException();
            }
        }

        public Stream CreateWriteStream(Stream stream) {
            const bool LEAVE_OPEN = true;

            switch(this.Algorithm) {
                case CompressionAlgorithm.NoCompress:
                    return stream;
                case CompressionAlgorithm.zstd:
                    var ilevel = int.Parse(this.Level, CultureInfo.InvariantCulture);
                    return new Zstandard.Net.ZstandardStream(stream, ilevel, LEAVE_OPEN);
                case CompressionAlgorithm.lz4:
                    var levelmode = (Lz4Net.Lz4Mode)Enum.Parse(typeof(Lz4Net.Lz4Mode), this.Level, true);
                    return new Lz4Net.Lz4CompressionStream(stream, levelmode, LEAVE_OPEN);
                case CompressionAlgorithm.zlib:
                    var level = (System.IO.Compression.CompressionLevel)Enum.Parse(typeof(System.IO.Compression.CompressionLevel), this.Level, true);
                    return new System.IO.Compression.GZipStream(stream, level, LEAVE_OPEN);
                default:
                    throw new NotImplementedException();
            }
        }

        public override string ToString() {
            var sb = new StringBuilder();
            sb.Append(this.Algorithm.ToString().ToLowerInvariant());
            if(!string.IsNullOrEmpty(this.Level)) {
                sb.Append(".level=");
                sb.Append(this.Level);
            }
            return sb.ToString();
        }

        // IEquatable<>
        #region Equals()
        public bool Equals(CompressionSetting other) {
            if(other is null)
                return false;
            return
                this.Algorithm == other.Algorithm &&
                this.Level == other.Level;
        }
        public override bool Equals(object obj) {
            if(obj == null || !(obj is CompressionSetting))
                return false;
            return this.Equals((CompressionSetting)obj);
        }
        #endregion
        #region GetHashCode()
        public override int GetHashCode() {
            return
                (this.Level?.GetHashCode() ?? 0) ^
                this.Algorithm.GetHashCode();
        }
        #endregion
        #region operator ==
        public static bool operator ==(CompressionSetting item1, CompressionSetting item2) {
            if(item1 is null)
                return item2 is null;
            return item1.Equals(item2);
        }
        #endregion
        #region operator !=
        public static bool operator !=(CompressionSetting item1, CompressionSetting item2) {
            return !(item1 == item2);
        }
        #endregion

        #region ToDebuggerDisplayString()
        private string ToDebuggerDisplayString() {
            return $"{{{this.ToString()}}}";
        }
        #endregion
    }
}
