using System;


namespace TimeSeriesDB
{
    /// <summary>
    ///     The supported data types.
    ///     This is somewhat more for documentation purpose than actual usage, since we support both passing the System.Type or this enum.
    /// </summary>
    public enum DataType {
        /// <summary>
        ///     DateTime
        ///     valid item_sizes:
        ///       * 64 bits = date+time     ] -> EncodingType.None
        ///                                      EncodingType.Delta
        ///                                      EncodingType.DeltaDelta (highly recommended for sorted data)
        ///                                      EncodingType.XOR
        /// </summary>
        DateTime,
        /// <summary>
        ///     TimeSpan
        ///     valid item_sizes:
        ///       * 64 bits            ] -> EncodingType.None
        ///                                 EncodingType.Delta
        ///                                 EncodingType.DeltaDelta
        ///                                 EncodingType.XOR
        /// </summary>
        TimeSpan,
        /// <summary>
        ///     decimal
        ///     valid item_sizes:
        ///       * 128 bits = decimal (although technically only 128 - 23 bits are encoded, so could be encoded more efficiently)
        ///                                 EncodingType.None
        /// </summary>
        Decimal,
        /// <summary>
        ///     double
        ///     valid item_sizes:
        ///       * 64 bits = double        ] -> EncodingType.None
        ///                                      EncodingType.XOR
        ///                                      EncodingType.DFCM (highly recommended for patterned or cyclic data)
        /// </summary>
        Double,
        /// <summary>
        ///     float
        ///     valid item_sizes:
        ///       * 32 bits = float         ] -> EncodingType.None
        ///                                      EncodingType.XOR
        ///                                      EncodingType.DFCM (highly recommended for patterned or cyclic data)
        /// </summary>
        Float,
        /// <summary>
        ///     ulong
        ///     valid item_sizes:
        ///       * 64 bits            ] -> EncodingType.None
        ///                                 EncodingType.Delta
        ///                                 EncodingType.DeltaDelta
        ///                                 EncodingType.XOR
        /// </summary>
        UInt64,
        /// <summary>
        ///     uint
        ///     valid item_sizes:
        ///       * 32 bits            ] -> EncodingType.None
        ///                                 EncodingType.Delta
        ///                                 EncodingType.DeltaDelta
        ///                                 EncodingType.XOR
        /// </summary>
        UInt32,
        /// <summary>
        ///     ushort
        ///     valid item_sizes:
        ///       * 16 bits            ] -> EncodingType.None
        ///                                 EncodingType.Delta
        ///                                 EncodingType.DeltaDelta
        ///                                 EncodingType.XOR
        /// </summary>
        UInt16,
        /// <summary>
        ///     byte
        ///     valid item_sizes:
        ///       * 1 bit              ]
        ///       * 2 bits             ] -> EncodingType.None
        ///       * 4 bits             ]
        ///       * 8 bits             ] -> EncodingType.None
        ///                                 EncodingType.Delta
        ///                                 EncodingType.DeltaDelta
        ///                                 EncodingType.XOR
        /// </summary>
        UInt8,
        /// <summary>
        ///     long
        ///     valid item_sizes:
        ///       * 64 bits            ] -> EncodingType.None
        ///                                 EncodingType.Delta
        ///                                 EncodingType.DeltaDelta
        ///                                 EncodingType.XOR
        /// </summary>
        Int64,
        /// <summary>
        ///     int
        ///     valid item_sizes:
        ///       * 32 bits            ] -> EncodingType.None
        ///                                 EncodingType.Delta
        ///                                 EncodingType.DeltaDelta
        ///                                 EncodingType.XOR
        /// </summary>
        Int32,
        /// <summary>
        ///     short
        ///     valid item_sizes:
        ///       * 16 bits            ] -> EncodingType.None
        ///                                 EncodingType.Delta
        ///                                 EncodingType.DeltaDelta
        ///                                 EncodingType.XOR
        /// </summary>
        Int16,
        /// <summary>
        ///     sbyte
        ///     valid item_sizes:
        ///       * 1 bit              ]
        ///       * 2 bits             ] -> EncodingType.None
        ///       * 4 bits             ]
        ///       * 8 bits             ] -> EncodingType.None
        ///                                 EncodingType.Delta
        ///                                 EncodingType.DeltaDelta
        ///                                 EncodingType.XOR
        /// </summary>
        Int8,
        /// <summary>
        ///     Boolean
        ///     valid item_sizes:
        ///       * 1 bit -> EncodingType.None
        /// </summary>
        Boolean,
        /// <summary>
        ///     String (UTF-8)
        ///     valid item_sizes:
        ///       * var bits -> EncodingType.None
        /// </summary>
        String,
        /// <summary>
        ///     byte[]
        ///     valid item_sizes:
        ///       * var bits -> EncodingType.None
        /// </summary>
        ByteArray,
        /// <summary>
        ///     stream
        ///     valid item_sizes:
        ///       * var bits -> EncodingType.None
        /// </summary>
        Stream,
    }
}
