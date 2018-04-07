using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Diagnostics;


namespace TimeSeriesDB
{
    using IO;
    using Internal;
    using DataStreams.Readers;
    using DataStreams.Readers.WithDecoders;
    using DataStreams.Writers;
    using DataStreams.Writers.WithEncoders;

    /// <summary>
    ///     An immutable column definition.
    ///     See remarks for quick list of valid datatypes / bit sizes / encodings.
    ///     
    ///     [optional column1_name] [Type],[optional bit_size],[optional encoding_type],[optional compression_setting]
    /// </summary>
    /// <remarks>
    ///     type         bit_size         encoding_type                                             compression
    ///     ----------------------------------------------------------------------------------------------------
    ///     datetime     64*              none, delta, deltadelta*, xor                             nocompress, zlib.level=fastest, lz4.level=fast, zstd.level=3*
    ///     timespan     64*              none, delta, deltadelta*, xor                             nocompress, zlib.level=fastest, lz4.level=fast, zstd.level=3*
    ///     decimal      128*             none*                                                     nocompress, zlib.level=fastest, lz4.level=fast, zstd.level=3*
    ///     double       64*              none, xor, dfcm*                                          nocompress, zlib.level=fastest, lz4.level=fast, zstd.level=3*
    ///     float        32*              none, xor, dfcm*                                          nocompress, zlib.level=fastest, lz4.level=fast, zstd.level=3*
    ///     UInt64       64* (var)        none, delta, deltadelta, xor*                             nocompress, zlib.level=fastest, lz4.level=fast, zstd.level=3*
    ///     UInt32       32* (var)        none, delta, deltadelta, xor*                             nocompress, zlib.level=fastest, lz4.level=fast, zstd.level=3*
    ///     UInt16       16*              none, delta, deltadelta, xor*                             nocompress, zlib.level=fastest, lz4.level=fast, zstd.level=3*
    ///     UInt8        1, 2, 4, 8*      (1, 2, 4*) = none*, 8* = none*, delta, deltadelta, xor    nocompress, zlib.level=fastest, lz4.level=fast, zstd.level=3*
    ///     Int64        64* (var)        none, delta, deltadelta, xor*                             nocompress, zlib.level=fastest, lz4.level=fast, zstd.level=3*
    ///     Int32        32* (var)        none, delta, deltadelta, xor*                             nocompress, zlib.level=fastest, lz4.level=fast, zstd.level=3*
    ///     Int16        16*              none, delta, deltadelta, xor*                             nocompress, zlib.level=fastest, lz4.level=fast, zstd.level=3*
    ///     Int8         1, 2, 4, 8*      (1, 2, 4*) = none*, 8* = none*, delta, deltadelta, xor    nocompress, zlib.level=fastest, lz4.level=fast, zstd.level=3*
    ///     bool         1*               none*                                                     nocompress, zlib.level=fastest, lz4.level=fast, zstd.level=3*
    ///     string       var*             none*                                                     nocompress, zlib.level=fastest, lz4.level=fast, zstd.level=3*
    ///     byte[]       var*             none*                                                     nocompress, zlib.level=fastest, lz4.level=fast, zstd.level=3*
    ///     stream       var*             none*                                                     nocompress, zlib.level=fastest, lz4.level=fast, zstd.level=3*
    ///     
    ///     * = default
    ///     
    ///     hidden "var" bitsize encoding option: search code for 'USE_TWO_CHANNEL'
    ///     enabling this option will separate the variable encoding lengths per item on their own separate channel
    ///     this leads to better compression having similar data grouped together (in the order of 5-10% better overall compression).
    ///     this option is not on by default as it results in fragmented data making recovery a lot harder in case of shutdowns
    /// </remarks>
    /// <example>
    ///     TimeStamp DateTime,64,DeltaDelta,nocompress
    ///     Value string,[var],[None],[zstd.level=3]
    ///     DummyColumn int
    /// </example>
    [DebuggerDisplay("{ToDebuggerDisplayString()}")]
    public sealed class ColumnDefinition : IEquatable<ColumnDefinition> {
        private static readonly ValidCombination[] VALID_COMBINATIONS = new[] {
            new ValidCombination(typeof(DateTime), DataType.DateTime,  "datetime", "64*;None,Delta,DeltaDelta*,XOR",             "nocompress,zlib.level=fastest,lz4.level=fast,zstd.level=3*"),
            new ValidCombination(typeof(TimeSpan), DataType.TimeSpan,  "timespan", "64*;None,Delta,DeltaDelta*,XOR",             "nocompress,zlib.level=fastest,lz4.level=fast,zstd.level=3*"),
            new ValidCombination(typeof(decimal),  DataType.Decimal,   "decimal",  "128*;None*",                                 "nocompress,zlib.level=fastest,lz4.level=fast,zstd.level=3*"),
            new ValidCombination(typeof(double),   DataType.Double,    "double",   "64*;None,XOR,DFCM*",                         "nocompress,zlib.level=fastest,lz4.level=fast,zstd.level=3*"),
            new ValidCombination(typeof(float),    DataType.Float,     "float",    "32*;None,XOR,DFCM*",                         "nocompress,zlib.level=fastest,lz4.level=fast,zstd.level=3*"),
            new ValidCombination(typeof(ulong),    DataType.UInt64,    "ulong",    "64*;None,Delta,DeltaDelta,XOR*",             "nocompress,zlib.level=fastest,lz4.level=fast,zstd.level=3*"),
            new ValidCombination(typeof(uint),     DataType.UInt32,    "uint",     "32*;None,Delta,DeltaDelta,XOR*",             "nocompress,zlib.level=fastest,lz4.level=fast,zstd.level=3*"),
            new ValidCombination(typeof(ushort),   DataType.UInt16,    "ushort",   "16*;None,Delta,DeltaDelta,XOR*",             "nocompress,zlib.level=fastest,lz4.level=fast,zstd.level=3*"),
            new ValidCombination(typeof(byte),     DataType.UInt8,     "byte",     "1,2,4*;None*|8*;None*,Delta,DeltaDelta,XOR", "nocompress,zlib.level=fastest,lz4.level=fast,zstd.level=3*"),
            new ValidCombination(typeof(long),     DataType.Int64,     "long",     "64*;None,Delta,DeltaDelta,XOR*",             "nocompress,zlib.level=fastest,lz4.level=fast,zstd.level=3*"),
            new ValidCombination(typeof(int),      DataType.Int32,     "int",      "32*;None,Delta,DeltaDelta,XOR*",             "nocompress,zlib.level=fastest,lz4.level=fast,zstd.level=3*"),
            new ValidCombination(typeof(short),    DataType.Int16,     "short",    "16*;None,Delta,DeltaDelta,XOR*",             "nocompress,zlib.level=fastest,lz4.level=fast,zstd.level=3*"),
            new ValidCombination(typeof(sbyte),    DataType.Int8,      "sbyte",    "1,2,4*;None*|8*;None*,Delta,DeltaDelta,XOR", "nocompress,zlib.level=fastest,lz4.level=fast,zstd.level=3*"),
            new ValidCombination(typeof(bool),     DataType.Boolean,   "bool",     "1*;None*",                                   "nocompress,zlib.level=fastest,lz4.level=fast,zstd.level=3*"),
            new ValidCombination(typeof(string),   DataType.String,    "string",   "var*;None*",                                 "nocompress,zlib.level=fastest,lz4.level=fast,zstd.level=3*"),
            new ValidCombination(typeof(byte[]),   DataType.ByteArray, "byte[]",   "var*;None*",                                 "nocompress,zlib.level=fastest,lz4.level=fast,zstd.level=3*"),
            new ValidCombination(typeof(Stream),   DataType.Stream,    "stream",   "var*;None*",                                 "nocompress,zlib.level=fastest,lz4.level=fast,zstd.level=3*"),
        };
        private static readonly Dictionary<Type, ValidCombination> m_typeCombinations = VALID_COMBINATIONS.ToDictionary(o => o.Type);
        private static readonly Dictionary<DataType, ValidCombination> m_datatypeCombinations = VALID_COMBINATIONS.ToDictionary(o => o.DataType);
        private static readonly Dictionary<string, ValidCombination> m_stringtypeCombinations = GenerateStringTypeCombinations();
        #region private static GenerateStringTypeCombinations()
        private static Dictionary<string, ValidCombination> GenerateStringTypeCombinations() {
            return VALID_COMBINATIONS.Select(o => new { Item = o, Name = o.TypeName.ToLowerInvariant() })
                .Concat(VALID_COMBINATIONS.Select(o => new { Item = o, Name = o.DataType.ToString().ToLowerInvariant() }))
                .Concat(VALID_COMBINATIONS.Select(o => new { Item = o, Name = o.Type.Name.ToLowerInvariant() }))
                .GroupBy(o => o.Name)
                .ToDictionary(o => o.Key, o => o.First().Item);
        }
        #endregion

        /// <summary>
        ///     The column name.
        ///     Optional.
        ///     Cannot contain ' ' or '\n'.
        /// </summary>
        public string Name { get; private set; }
        public Type Type => m_combination.Type;
        /// <summary>
        ///     Purely for informational purposes. Acts the same as this.Type.
        /// </summary>
        public DataType DataType => m_combination.DataType;
        public string TypeName => m_combination.TypeName;
        public BitSize BitSize { get; private set; }
        public EncodingType Encoding { get; private set; }
        public CompressionSetting Compression { get; private set; } // readonly to be consistent with the rest, but changeable with ChangeCompression()

        private ValidCombination m_combination;

        #region constructors
        private ColumnDefinition() { 
        }
        public ColumnDefinition(string name, Type type) {
            this.Init(name, FindCombination(type));
        }
        public ColumnDefinition(string name, Type type, EncodingType encoding) {
            this.Init(name, FindCombination(type), encoding);
        }
        public ColumnDefinition(string name, Type type, BitSize bitSize) {
            this.Init(name, FindCombination(type), bitSize);
        }
        public ColumnDefinition(string name, Type type, BitSize bitSize, EncodingType encoding) {
            this.Init(name, FindCombination(type), bitSize, encoding);
        }
        public ColumnDefinition(string name, Type type, BitSize bitSize, EncodingType encoding, CompressionSetting compression) {
            this.Init(name, FindCombination(type), bitSize, encoding, compression);
        }
        public ColumnDefinition(string name, DataType type) {
            this.Init(name, FindCombination(type));
        }
        public ColumnDefinition(string name, DataType type, EncodingType encoding) {
            this.Init(name, FindCombination(type), encoding);
        }
        public ColumnDefinition(string name, DataType type, BitSize bitSize) {
            this.Init(name, FindCombination(type), bitSize);
        }
        public ColumnDefinition(string name, DataType type, BitSize bitSize, EncodingType encoding) {
            this.Init(name, FindCombination(type), bitSize, encoding);
        }
        public ColumnDefinition(string name, DataType type, BitSize bitSize, EncodingType encoding, CompressionSetting compression) {
            this.Init(name, FindCombination(type), bitSize, encoding, compression);
        }
        #endregion

        #region private Init()
        private void Init(string name, ValidCombination combination) {
            this.Name        = name;
            m_combination    = combination;
            this.BitSize     = combination.GetDefaultBitSize();
            this.Encoding    = combination.GetDefaultEncoding();
            this.Compression = combination.DefaultCompressionSetting;
        }
        private void Init(string name, ValidCombination combination, EncodingType encoding) {
            var temp = combination.Find(encoding);
            if(temp == null) {
                throw new NotSupportedException(string.Format("The {0} ({1}) is not supported for type {2}. Valid {0} = {3}.",
                    nameof(this.Encoding),
                    encoding,
                    combination.Type,
                    combination.GenerateValidEncodingsList()));
            }

            this.Name        = name;
            this.Encoding    = encoding;
            m_combination    = combination;
            this.BitSize     = temp.DefaultBitSize;
            this.Compression = combination.DefaultCompressionSetting;
        }
        private void Init(string name, ValidCombination combination, BitSize bitSize) {
            var temp = combination.Find(bitSize);
            if(temp == null) {
                throw new NotSupportedException(string.Format("The {0} ({1}) is not supported for type {2}. Valid {0} = {3}.",
                    nameof(this.BitSize),
                    bitSize,
                    combination.Type,
                    combination.GenerateValidBitSizesList()));
            }

            this.Name        = name;
            this.BitSize     = bitSize;
            m_combination    = combination;
            this.Encoding    = temp.DefaultEncoding;
            this.Compression = combination.DefaultCompressionSetting;
        }
        private void Init(string name, ValidCombination combination, BitSize bitSize, EncodingType encoding) {
            var temp = combination.Find(bitSize);
            if(temp == null) {
                throw new NotSupportedException(string.Format("The {0} ({1}) is not supported for type {2}. Valid {0} = {3}.",
                    nameof(this.BitSize),
                    bitSize,
                    combination.Type,
                    combination.GenerateValidBitSizesList()));
            }
            if(!temp.Encodings.Contains(encoding)) {
                throw new NotSupportedException(string.Format("The {0} ({1}) is not supported for type {2}. Valid {0} = {3}.",
                    nameof(this.Encoding),
                    encoding,
                    combination.Type,
                    temp.GenerateValidEncodingsList()));
            }

            this.Name        = name;
            this.BitSize     = bitSize;
            m_combination    = combination;
            this.Encoding    = encoding;
            this.Compression = combination.DefaultCompressionSetting;
        }
        private void Init(string name, ValidCombination combination, BitSize bitSize, EncodingType encoding, CompressionSetting compression) {
            var temp = combination.Find(bitSize);
            if(temp == null) {
                throw new NotSupportedException(string.Format("The {0} ({1}) is not supported for type {2}. Valid {0} = {3}.",
                    nameof(this.BitSize),
                    bitSize,
                    combination.Type,
                    combination.GenerateValidBitSizesList()));
            }
            if(!temp.Encodings.Contains(encoding)) {
                throw new NotSupportedException(string.Format("The {0} ({1}) is not supported for type {2}. Valid {0} = {3}.",
                    nameof(this.Encoding),
                    encoding,
                    combination.Type,
                    temp.GenerateValidEncodingsList()));
            }
            if(!combination.CompressionSettings.Any(o => o.Algorithm == compression.Algorithm)) {
                throw new NotSupportedException(string.Format("The {0} ({1}) is not supported for type {2}. Valid {0} = {3}.",
                    nameof(this.Compression),
                    compression,
                    combination.Type,
                    combination.GenerateValidCompressionsList()));
            }

            this.Name        = name;
            this.BitSize     = bitSize;
            m_combination    = combination;
            this.Encoding    = encoding;
            this.Compression = compression;
        }
        #endregion
        #region private static FindCombination()
        private static ValidCombination FindCombination(Type type) {
            if(!m_typeCombinations.TryGetValue(type, out ValidCombination combination))
                throw new NotSupportedException($"The type {type} is not supported.");
            return combination;
        }
        private static ValidCombination FindCombination(DataType type) {
            if(!m_datatypeCombinations.TryGetValue(type, out ValidCombination combination))
                throw new NotImplementedException($"The type {type} is not implemented.");
            return combination;
        }
        #endregion

        #region ReadStreamWrapper()
        public Stream ReadStreamWrapper(Stream stream) {
            return this.Compression.CreateReadStream(stream);
        }
        #endregion
        #region WriteStreamWrapper()
        public Stream WriteStreamWrapper(Stream storageStream) {
            return this.Compression.CreateWriteStream(storageStream);
        }
        #endregion
        #region CreateReader()
        public IDataStreamReader CreateReader(MultiChannelStream channels, int channel_id, ulong itemCount = ulong.MaxValue) {
            var res = this.InternalCreateReader();

            var channelsList = new Stream[res.ChannelCount];
            for(int i = 0; i < channelsList.Length; i++)
                channelsList[i] = this.ReadStreamWrapper(channels.GetOrCreateChannel(channel_id++));

            res.Init(channelsList);
            res.ItemCount = itemCount;

            return res;
        }
        private IDataStreamReader InternalCreateReader() {
            switch(this.DataType) {
                case DataType.DateTime:
                    switch(this.Encoding) {
                        case EncodingType.None:       return new DataStreamReader_DateTime();
                        case EncodingType.Delta:      return new Delta_DataStreamReader_DateTime();
                        case EncodingType.DeltaDelta: return new DeltaDelta_DataStreamReader_DateTime();
                        case EncodingType.XOR:        return new Xor_DataStreamReader_DateTime();
                    }
                    break;
                case DataType.TimeSpan:
                    switch(this.Encoding) {
                        case EncodingType.None:       return new DataStreamReader_TimeSpan();
                        case EncodingType.Delta:      return new Delta_DataStreamReader_TimeSpan();
                        case EncodingType.DeltaDelta: return new DeltaDelta_DataStreamReader_TimeSpan();
                        case EncodingType.XOR:        return new Xor_DataStreamReader_TimeSpan();
                    }
                    break;
                case DataType.Decimal:
                    switch(this.Encoding) {
                        case EncodingType.None:       return new DataStreamReader_Decimal();
                    }
                    break;
                case DataType.Double:
                    switch(this.Encoding) {
                        case EncodingType.None:       return new DataStreamReader_Double();
                        case EncodingType.XOR:        return new Xor_DataStreamReader_Double();
                        case EncodingType.DFCM:       return new Dfcm_DataStreamReader_Double();
                    }
                    break;
                case DataType.Float:
                    switch(this.Encoding) {
                        case EncodingType.None:       return new DataStreamReader_Float();
                        case EncodingType.XOR:        return new Xor_DataStreamReader_Float();
                        case EncodingType.DFCM:       return new Dfcm_DataStreamReader_Float();
                    }
                    break;
                case DataType.UInt64:
                    switch(this.Encoding) {
                        case EncodingType.None:
                            //return new DataStreamReader_UInt64();
                            return new DataStreamReader_UInt64_LSB();
                        case EncodingType.Delta:
                            //return new Delta_DataStreamReader_UInt64();
                            return new Delta_DataStreamReader_UInt64_LSB();
                        case EncodingType.DeltaDelta:
                            //return new DeltaDelta_DataStreamReader_UInt64();
                            return new DeltaDelta_DataStreamReader_UInt64_LSB();
                        case EncodingType.XOR:
                            //return new Xor_DataStreamReader_UInt64();
                            return new Xor_DataStreamReader_UInt64_LSB();
                    }
                    break;
                case DataType.UInt32:
                    switch(this.Encoding) {
                        case EncodingType.None:       return new DataStreamReader_UInt32();
                        case EncodingType.Delta:      return new Delta_DataStreamReader_UInt32();
                        case EncodingType.DeltaDelta: return new DeltaDelta_DataStreamReader_UInt32();
                        case EncodingType.XOR:        return new Xor_DataStreamReader_UInt32();
                    }
                    break;
                case DataType.UInt16:
                    switch(this.Encoding) {
                        case EncodingType.None:       return new DataStreamReader_UInt16();
                        case EncodingType.Delta:      return new Delta_DataStreamReader_UInt16();
                        case EncodingType.DeltaDelta: return new DeltaDelta_DataStreamReader_UInt16();
                        case EncodingType.XOR:        return new Xor_DataStreamReader_UInt16();
                    }
                    break;
                case DataType.UInt8:
                    switch(this.BitSize.Bits.Value) {
                        case 8:
                            switch(this.Encoding) {
                                case EncodingType.None:       return new DataStreamReader_UInt8();
                                case EncodingType.Delta:      return new Delta_DataStreamReader_UInt8();
                                case EncodingType.DeltaDelta: return new DeltaDelta_DataStreamReader_UInt8();
                                case EncodingType.XOR:        return new Xor_DataStreamReader_UInt8();
                            }
                            break;
                        case 4:
                            switch(this.Encoding) {
                                case EncodingType.None: return new DataStreamReader_UInt4();
                            }
                            break;
                        case 2:
                            switch(this.Encoding) {
                                case EncodingType.None: return new DataStreamReader_UInt2();
                            }
                            break;
                        case 1:
                            switch(this.Encoding) {
                                case EncodingType.None: return new DataStreamReader_UInt1();
                            }
                            break;
                    }
                    break;
                case DataType.Int64:
                    switch(this.Encoding) {
                        case EncodingType.None:
                            //return new DataStreamReader_Int64();
                            return new DataStreamReader_Int64_LSB();
                        case EncodingType.Delta:
                            //return new Delta_DataStreamReader_Int64();
                            return new Delta_DataStreamReader_Int64_LSB();
                        case EncodingType.DeltaDelta:
                            //return new DeltaDelta_DataStreamReader_Int64();
                            return new DeltaDelta_DataStreamReader_Int64_LSB();
                        case EncodingType.XOR:
                            //return new Xor_DataStreamReader_Int64();
                            return new Xor_DataStreamReader_Int64_LSB();
                    }
                    break;
                case DataType.Int32:
                    switch(this.Encoding) {
                        case EncodingType.None:       return new DataStreamReader_Int32();
                        case EncodingType.Delta:      return new Delta_DataStreamReader_Int32();
                        case EncodingType.DeltaDelta: return new DeltaDelta_DataStreamReader_Int32();
                        case EncodingType.XOR:        return new Xor_DataStreamReader_Int32();
                    }
                    break;
                case DataType.Int16:
                    switch(this.Encoding) {
                        case EncodingType.None:       return new DataStreamReader_Int16();
                        case EncodingType.Delta:      return new Delta_DataStreamReader_Int16();
                        case EncodingType.DeltaDelta: return new DeltaDelta_DataStreamReader_Int16();
                        case EncodingType.XOR:        return new Xor_DataStreamReader_Int16();
                    }
                    break;
                case DataType.Int8:
                    switch(this.BitSize.Bits.Value) {
                        case 8:
                            switch(this.Encoding) {
                                case EncodingType.None:       return new DataStreamReader_Int8();
                                case EncodingType.Delta:      return new Delta_DataStreamReader_Int8();
                                case EncodingType.DeltaDelta: return new DeltaDelta_DataStreamReader_Int8();
                                case EncodingType.XOR:        return new Xor_DataStreamReader_Int8();
                            }
                            break;
                        case 4:
                            switch(this.Encoding) {
                                case EncodingType.None: return new DataStreamReader_Int4();
                            }
                            break;
                        case 2:
                            switch(this.Encoding) {
                                case EncodingType.None: return new DataStreamReader_Int2();
                            }
                            break;
                        case 1:
                            switch(this.Encoding) {
                                case EncodingType.None: return new DataStreamReader_Int1();
                            }
                            break;
                    }
                    break;
                case DataType.Boolean:
                    switch(this.Encoding) {
                        case EncodingType.None: return new DataStreamReader_Bool();
                    }
                    break;

                case DataType.String:
                    switch(this.Encoding) {
                        case EncodingType.None: return new DataStreamReader_String();
                    }
                    break;
                case DataType.ByteArray:
                    switch(this.Encoding) {
                        case EncodingType.None: return new DataStreamReader_ByteArray();
                    }
                    break;
                    
                case DataType.Stream:
                    switch(this.Encoding) {
                        case EncodingType.None: return new DataStreamReader_Stream();
                    }
                    break;
            }
            throw new NotImplementedException();
        }
        #endregion
        #region CreateWriter()
        public IDataStreamWriter CreateWriter(MultiChannelStream channels, int channel_id) {
            var res = this.InternalCreateWriter();

            var channelsList = new Stream[res.ChannelCount];
            for(int i = 0; i < channelsList.Length; i++)
                channelsList[i] = this.WriteStreamWrapper(channels.GetOrCreateChannel(channel_id++));

            res.Init(channelsList);

            return res;
        }
        private IDataStreamWriter InternalCreateWriter() {
            switch(this.DataType) {
                case DataType.DateTime:
                    switch(this.Encoding) {
                        case EncodingType.None:       return new DataStreamWriter_DateTime();
                        case EncodingType.Delta:      return new Delta_DataStreamWriter_DateTime();
                        case EncodingType.DeltaDelta: return new DeltaDelta_DataStreamWriter_DateTime();
                        case EncodingType.XOR:        return new Xor_DataStreamWriter_DateTime();
                    }
                    break;
                case DataType.TimeSpan:
                    switch(this.Encoding) {
                        case EncodingType.None:       return new DataStreamWriter_TimeSpan();
                        case EncodingType.Delta:      return new Delta_DataStreamWriter_TimeSpan();
                        case EncodingType.DeltaDelta: return new DeltaDelta_DataStreamWriter_TimeSpan();
                        case EncodingType.XOR:        return new Xor_DataStreamWriter_TimeSpan();
                    }
                    break;
                case DataType.Decimal:
                    switch(this.Encoding) {
                        case EncodingType.None:       return new DataStreamWriter_Decimal();
                    }
                    break;
                case DataType.Double:
                    switch(this.Encoding) {
                        case EncodingType.None:       return new DataStreamWriter_Double();
                        case EncodingType.XOR:        return new Xor_DataStreamWriter_Double();
                        case EncodingType.DFCM:       return new Dfcm_DataStreamWriter_Double();
                    }
                    break;
                case DataType.Float:
                    switch(this.Encoding) {
                        case EncodingType.None:       return new DataStreamWriter_Float();
                        case EncodingType.XOR:        return new Xor_DataStreamWriter_Float();
                        case EncodingType.DFCM:       return new Dfcm_DataStreamWriter_Float();
                    }
                    break;
                case DataType.UInt64:
                    switch(this.Encoding) {
                        case EncodingType.None:
                            //return new DataStreamWriter_UInt64();
                            return new DataStreamWriter_UInt64_LSB();
                        case EncodingType.Delta:
                            //return new Delta_DataStreamWriter_UInt64();
                            return new Delta_DataStreamWriter_UInt64_LSB();
                        case EncodingType.DeltaDelta:
                            //return new DeltaDelta_DataStreamWriter_UInt64();
                            return new DeltaDelta_DataStreamWriter_UInt64_LSB();
                        case EncodingType.XOR:
                            //return new Xor_DataStreamWriter_UInt64();
                            return new Xor_DataStreamWriter_UInt64_LSB();
                    }
                    break;
                case DataType.UInt32:
                    switch(this.Encoding) {
                        case EncodingType.None:       return new DataStreamWriter_UInt32();
                        case EncodingType.Delta:      return new Delta_DataStreamWriter_UInt32();
                        case EncodingType.DeltaDelta: return new DeltaDelta_DataStreamWriter_UInt32();
                        case EncodingType.XOR:        return new Xor_DataStreamWriter_UInt32();
                    }
                    break;
                case DataType.UInt16:
                    switch(this.Encoding) {
                        case EncodingType.None:       return new DataStreamWriter_UInt16();
                        case EncodingType.Delta:      return new Delta_DataStreamWriter_UInt16();
                        case EncodingType.DeltaDelta: return new DeltaDelta_DataStreamWriter_UInt16();
                        case EncodingType.XOR:        return new Xor_DataStreamWriter_UInt16();
                    }
                    break;
                case DataType.UInt8:
                    switch(this.BitSize.Bits.Value) {
                        case 8:
                            switch(this.Encoding) {
                                case EncodingType.None:       return new DataStreamWriter_UInt8();
                                case EncodingType.Delta:      return new Delta_DataStreamWriter_UInt8();
                                case EncodingType.DeltaDelta: return new DeltaDelta_DataStreamWriter_UInt8();
                                case EncodingType.XOR:        return new Xor_DataStreamWriter_UInt8();
                            }
                            break;
                        case 4:
                            switch(this.Encoding) {
                                case EncodingType.None: return new DataStreamWriter_UInt4();
                            }
                            break;
                        case 2:
                            switch(this.Encoding) {
                                case EncodingType.None: return new DataStreamWriter_UInt2();
                            }
                            break;
                        case 1:
                            switch(this.Encoding) {
                                case EncodingType.None: return new DataStreamWriter_UInt1();
                            }
                            break;
                    }
                    break;
                case DataType.Int64:
                    switch(this.Encoding) {
                        case EncodingType.None:
                            //return new DataStreamWriter_Int64();
                            return new DataStreamWriter_Int64_LSB();
                        case EncodingType.Delta:
                            //return new Delta_DataStreamWriter_Int64();
                            return new Delta_DataStreamWriter_Int64_LSB();
                        case EncodingType.DeltaDelta:
                            //return new DeltaDelta_DataStreamWriter_Int64();
                            return new DeltaDelta_DataStreamWriter_Int64_LSB();
                        case EncodingType.XOR:
                            //return new Xor_DataStreamWriter_Int64();
                            return new Xor_DataStreamWriter_Int64_LSB();
                    }
                    break;
                case DataType.Int32:
                    switch(this.Encoding) {
                        case EncodingType.None:       return new DataStreamWriter_Int32();
                        case EncodingType.Delta:      return new Delta_DataStreamWriter_Int32();
                        case EncodingType.DeltaDelta: return new DeltaDelta_DataStreamWriter_Int32();
                        case EncodingType.XOR:        return new Xor_DataStreamWriter_Int32();
                    }
                    break;
                case DataType.Int16:
                    switch(this.Encoding) {
                        case EncodingType.None:       return new DataStreamWriter_Int16();
                        case EncodingType.Delta:      return new Delta_DataStreamWriter_Int16();
                        case EncodingType.DeltaDelta: return new DeltaDelta_DataStreamWriter_Int16();
                        case EncodingType.XOR:        return new Xor_DataStreamWriter_Int16();
                    }
                    break;
                case DataType.Int8:
                    switch(this.BitSize.Bits.Value) {
                        case 8:
                            switch(this.Encoding) {
                                case EncodingType.None:       return new DataStreamWriter_Int8();
                                case EncodingType.Delta:      return new Delta_DataStreamWriter_Int8();
                                case EncodingType.DeltaDelta: return new DeltaDelta_DataStreamWriter_Int8();
                                case EncodingType.XOR:        return new Xor_DataStreamWriter_Int8();
                            }
                            break;
                        case 4:
                            switch(this.Encoding) {
                                case EncodingType.None: return new DataStreamWriter_Int4();
                            }
                            break;
                        case 2:
                            switch(this.Encoding) {
                                case EncodingType.None: return new DataStreamWriter_Int2();
                            }
                            break;
                        case 1:
                            switch(this.Encoding) {
                                case EncodingType.None: return new DataStreamWriter_Int1();
                            }
                            break;
                    }
                    break;
                case DataType.Boolean:
                    switch(this.Encoding) {
                        case EncodingType.None: return new DataStreamWriter_Bool();
                    }
                    break;

                case DataType.String:
                    switch(this.Encoding) {
                        case EncodingType.None: return new DataStreamWriter_String();
                    }
                    break;
                case DataType.ByteArray:
                    switch(this.Encoding) {
                        case EncodingType.None: return new DataStreamWriter_ByteArray();
                    }
                    break;
                    
                case DataType.Stream:
                    switch(this.Encoding) {
                        case EncodingType.None: return new DataStreamWriter_Stream();
                    }
                    break;
            }
            throw new NotImplementedException();
        }
        #endregion

        // IEquatable<>
        #region Equals()
        public bool Equals(ColumnDefinition other) {
            if(other is null)
                return false;
            return
                this.Type == other.Type &&
                this.BitSize == other.BitSize &&
                this.Encoding == other.Encoding &&
                //this.DataType == other.DataType &&
                string.CompareOrdinal(this.Name, other.Name) == 0 &&
                string.CompareOrdinal(this.TypeName, other.TypeName) == 0 &&
                this.Compression == other.Compression;
        }
        public override bool Equals(object obj) {
            if(obj == null || !(obj is ColumnDefinition))
                return false;
            return this.Equals((ColumnDefinition)obj);
        }
        #endregion
        #region GetHashCode()
        public override int GetHashCode() {
            return 
                (this.Name?.GetHashCode() ?? 0) ^ 
                this.Type.GetHashCode() ^ 
                this.BitSize.GetHashCode() ^
                this.Encoding.GetHashCode() ^
                //this.DataType.GetHashCode() ^
                (this.TypeName?.GetHashCode() ?? 0) ^
                this.Compression.GetHashCode();
        }
        #endregion
        #region operator ==
        public static bool operator ==(ColumnDefinition item1, ColumnDefinition item2) {
            if(item1 is null)
                return item2 is null;
            return item1.Equals(item2);
        }
        #endregion
        #region operator !=
        public static bool operator !=(ColumnDefinition item1, ColumnDefinition item2) {
            return !(item1 == item2);
        }
        #endregion

        #region static Parse()
        /// <param name="serialized">See class example.</param>
        public static ColumnDefinition Parse(string serialized) {
            var res = new ColumnDefinition();
            res.InternalParse(serialized);
            return res;
        }
        /// <param name="serialized">See class example.</param>
        private void InternalParse(string serialized) {
            if(string.IsNullOrEmpty(serialized))
                throw new ArgumentNullException(nameof(serialized), "The column definition is missing.");

            var split = serialized.Trim(' ').Split(new[] { ' ' }, StringSplitOptions.None);

            if(split.Length > 2)
                throw new FormatException($"The ColumnDefinition ({serialized}) contains extraneous spaces.");

            this.Name = split.Length == 2 ? split[0] : null;

            split = split[split.Length - 1].Split(new[] { ',' }, StringSplitOptions.None);
            if(split.Length < 4) {
                // upsize
                var temp = new string[4];
                Array.Copy(split, 0, temp, 0, split.Length);
                split = temp;
            }
            if(string.IsNullOrEmpty(split[0]))
                throw new FormatException($"The ColumnDefinition ({serialized}) requires the type to be specified. Expected: '[optional column1_name] [Type],[optional bit_size],[optional encoding_type],[optional compression_setting]'.");

            if(!m_stringtypeCombinations.TryGetValue(split[0].ToLowerInvariant(), out ValidCombination combination))
                throw new FormatException($"The ColumnDefinition ({serialized}) contains an unsupported type ({split[0]}).");

            m_combination = combination;

            if(string.IsNullOrEmpty(split[1]))
                this.BitSize = combination.GetDefaultBitSize();
            else {
                BitSize bitSize = split[1];

                if(combination.Find(bitSize) == null) {
                    throw new NotSupportedException(string.Format("The {0} ({1}) is not supported for type {2}. Valid {0} = {3}.",
                        nameof(this.BitSize),
                        split[1],
                        combination.Type,
                        combination.GenerateValidBitSizesList()));
                }

                this.BitSize = bitSize;
            }

            if(string.IsNullOrEmpty(split[2]))
                this.Encoding = combination.Find(this.BitSize).DefaultEncoding;
            else {
                if(!Enum.TryParse(split[2], true, out EncodingType encoding))
                    throw new FormatException($"Invalid EncodingType ({split[2]}).");

                var temp = combination.Find(this.BitSize);

                if(!temp.Encodings.Contains(encoding)) {
                    throw new NotSupportedException(string.Format("The {0} ({1}) is not supported for type {2}. Valid {0} = {3}.",
                        nameof(this.Encoding),
                        split[2],
                        combination.Type,
                        temp.GenerateValidEncodingsList()));
                }

                this.Encoding = encoding;
            }

            if(string.IsNullOrEmpty(split[3]))
                this.Compression = combination.DefaultCompressionSetting;
            else {
                var compression = CompressionSetting.Parse(split[3]);
                
                if(!combination.CompressionSettings.Any(o => o.Algorithm == compression.Algorithm)) {
                    throw new NotSupportedException(string.Format("The {0} ({1}) is not supported for type {2}. Valid {0} = {3}.",
                        nameof(this.Compression),
                        split[3],
                        combination.Type,
                        combination.GenerateValidCompressionsList()));
                }

                this.Compression = compression;
            }
        }
        #endregion
        #region static ChangeCompression()
        public static ColumnDefinition ChangeCompression(ColumnDefinition column, CompressionSetting compression = null) {
            return new ColumnDefinition(
                column.Name,
                column.DataType,
                column.BitSize,
                column.Encoding,
                compression ?? new CompressionSetting(CompressionAlgorithm.NoCompress));
        }
        #endregion
        #region static ChangeDataType()
        public static ColumnDefinition ChangeDataType(ColumnDefinition column, DataType datatype) {
            return new ColumnDefinition(
                column.Name,
                datatype,
                column.BitSize,
                column.Encoding,
                column.Compression);
        }
        #endregion
        #region static GetCombinations()
        /// <summary>
        ///     Returns all the possible ColumnDefinitions for a given data type.
        /// </summary>
        public static IEnumerable<ColumnDefinition> GetCombinations(DataType dataType) {
            var combination = FindCombination(dataType);

            foreach(var item in combination.BitSizesAndEncodings) {
                var combinations = BitMethods.GenerateCombinations(new[] { item.BitSizes.Length, item.Encodings.Length, combination.CompressionSettings.Length });
                foreach(var c in combinations) {
                    var res = new ColumnDefinition(null, dataType, item.BitSizes[c[0]], item.Encodings[c[1]], combination.CompressionSettings[c[2]]);
                    yield return res;
                }
            }
        }
        #endregion
        #region ToString()
        public override string ToString() {
            int length = (!string.IsNullOrEmpty(this.Name) ? this.Name.Length + 1 : 0) +
                this.TypeName.Length + 
                this.BitSize.ToString().Length + 
                this.Encoding.ToString().Length + 
                this.Compression.ToString().Length + 
                3;

            var sb = new StringBuilder(length);

            if(!string.IsNullOrEmpty(this.Name)) {
                sb.Append(this.Name);
                sb.Append(' ');
            }

            sb.Append(this.TypeName);
            sb.Append(',');
            sb.Append(this.BitSize.ToString());
            sb.Append(',');
            sb.Append(this.Encoding.ToString());
            sb.Append(',');
            sb.Append(this.Compression.ToString());

            return sb.ToString();
        }
        #endregion
        #region private ToDebuggerDisplayString()
        private string ToDebuggerDisplayString() {
            return $"{{{this.ToString()}}}";
        }
        #endregion

        #region private class ValidCombination
        private class ValidCombination {
            public readonly Type Type;
            public readonly DataType DataType;
            public readonly string TypeName;
            public readonly BitSizeAndEncoding[] BitSizesAndEncodings;
            public readonly CompressionSetting[] CompressionSettings;
            public readonly CompressionSetting DefaultCompressionSetting;
            /// <param name="data">ex: "1,2,4*;None*|8,var*;None,DFCM*"</param>
            public ValidCombination(Type type, DataType dataType, string typeName, string data, string compressionSettings) {
                this.Type = type;
                this.DataType = dataType;
                this.TypeName = typeName;

                this.BitSizesAndEncodings = data
                    .Split('|')
                    .Select(o => new BitSizeAndEncoding(o))
                    .OrderBy(o => o.BitSizes[0])
                    .ToArray();

                CompressionSetting defaultCompressionSetting = null;
                this.CompressionSettings = compressionSettings
                    .Split(',')
                    .Select(o => {
                        var x = ParseCompressionSetting(o);
                        if(x.Item2)
                            defaultCompressionSetting = x.Item1;
                        return x.Item1;
                    })
                    .OrderBy(o => o.Algorithm)
                    .ToArray();
                this.DefaultCompressionSetting = defaultCompressionSetting ?? this.CompressionSettings.Last();
            }
            public BitSize GetDefaultBitSize() {
                return this.BitSizesAndEncodings.Last().DefaultBitSize;
            }
            public EncodingType GetDefaultEncoding() {
                return this.BitSizesAndEncodings.Last().DefaultEncoding;
            }
            public string GenerateValidBitSizesList() {
                return string.Join(",", this.BitSizesAndEncodings.Select(o => o.GenerateValidBitSizesList()));
            }
            public string GenerateValidEncodingsList() {
                return string.Join(",", this.BitSizesAndEncodings.Select(o => o.GenerateValidEncodingsList()));
            }
            public string GenerateValidCompressionsList() {
                return string.Join(",", this.CompressionSettings.Select(o => o.Algorithm));
            }
            public BitSizeAndEncoding Find(BitSize bitSize) {
                for(int i = 0; i < this.BitSizesAndEncodings.Length; i++) {
                    var item_bit_sizes = this.BitSizesAndEncodings[i].BitSizes;
                    for(int j = 0; j < item_bit_sizes.Length; j++) {
                        if(item_bit_sizes[j] == bitSize)
                            return this.BitSizesAndEncodings[i];
                    }
                }
                return null;
            }
            public BitSizeAndEncoding Find(EncodingType encoding) {
                for(int i = 0; i < this.BitSizesAndEncodings.Length; i++) {
                    var encodings = this.BitSizesAndEncodings[i].Encodings;
                    for(int j = 0; j < encodings.Length; j++) {
                        if(encodings[j] == encoding)
                            return this.BitSizesAndEncodings[i];
                    }
                }
                return null;
            }
            private static Tuple<CompressionSetting, bool> ParseCompressionSetting(string value) {
                bool is_default = false;
                if(value.EndsWith("*", StringComparison.Ordinal)) {
                    value = value.Substring(0, value.Length - 1);
                    is_default = true;
                }
                return Tuple.Create(CompressionSetting.Parse(value), is_default);
            }
        }
        #endregion
        #region private class BitSizeAndEncoding
        private class BitSizeAndEncoding {
            public readonly BitSize[] BitSizes;
            public readonly EncodingType[] Encodings;

            public readonly BitSize DefaultBitSize;
            public readonly EncodingType DefaultEncoding;

            /// <param name="data">ex: "1,2,4*;None*"</param>
            public BitSizeAndEncoding(string data) {
                BitSize? defaultBitSize = null;
                EncodingType? defaultEncoding = null;

                var split = data.Split(';');
                this.BitSizes = split[0]
                    .Split(',')
                    .Select(o => {
                        var x = ParseBitSize(o);
                        if(x.Item2)
                            defaultBitSize = x.Item1;
                        return x.Item1;
                    })
                    .OrderBy(o => o)
                    .ToArray();
                this.Encodings = split[1]
                    .Split(',')
                    .Select(o => {
                        var x = ParseEncoding(o);
                        if(x.Item2)
                            defaultEncoding = x.Item1;
                        return x.Item1;
                    })
                    .ToArray();
                this.DefaultBitSize = defaultBitSize ?? this.BitSizes.Last();
                this.DefaultEncoding = defaultEncoding ?? this.Encodings.Last();
            }
            private static Tuple<BitSize, bool> ParseBitSize(string value) {
                bool is_default = false;
                if(value.EndsWith("*", StringComparison.Ordinal)) {
                    value = value.Substring(0, value.Length - 1);
                    is_default = true;
                }
                return Tuple.Create((BitSize)value, is_default);
            }
            private static Tuple<EncodingType, bool> ParseEncoding(string value) {
                bool is_default = false;
                if(value.EndsWith("*", StringComparison.Ordinal)) {
                    value = value.Substring(0, value.Length - 1);
                    is_default = true;
                }
                return Tuple.Create((EncodingType)Enum.Parse(typeof(EncodingType), value, true), is_default);
            }
            public string GenerateValidBitSizesList() {
                return string.Join(",", this.BitSizes.Select(o => (string)o));
            }
            public string GenerateValidEncodingsList() {
                return string.Join(",", this.Encodings);
            }
        }
        #endregion
    }
}
