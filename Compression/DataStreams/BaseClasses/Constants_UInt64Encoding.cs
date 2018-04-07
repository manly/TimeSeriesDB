using System.Runtime.CompilerServices;


#region FORMAT EXPLANATION
// Similar to akumuli, but with a better usage of flag byte
//
// byte    flag
// Int64   value1
// Int64   value2
// 
// value1/value2 n bytes are encoded in flag
//
// flag format
// 0x88 = 1000 1000 (binary)
//        ---- ----
//        ^         first encoded value
//             ^    second encoded value
// 0x88 = 1000 1000 (binary)
//        ^
//        |
//        first bit:      0 = trailing bytes (* must be this if remainder bits = 8)
//                        1 = leading bytes
//        remainder bits: [1-8] n bytes length
//
// flag format special values
// as noted above, 0xF cannot be generated. This is used to indicate 2 special cases
//
// 0x?F = ???? 1111
//             ^    indicates there is no secondary encoded value
// 0xF0 = 1111 0000
//        ^         RLE encoding. This indicates that the following 4 bits will instead be used to store the # of repetitive zeroes
//             ^    [1-16] Number of repeating zeroes
#endregion

namespace TimeSeriesDB.DataStreams.BaseClasses {
    /// <summary>
    ///     Stores efficiently a stream of ulong values.
    ///     Will store either the top-most or bottom-most bytes, depending on the side having the most zeroes.
    ///     If you know you are storing small values, use the UInt64_LSB encoders instead for a far better compression ratio/speed.
    ///     Does automatic RLE on zero values.
    /// </summary>
    public static class Constants_UInt64Encoding {
        public const int SIGNAL_SINGLE_ITEM               = 0x0F;
        public const int SIGNAL_REPEATING_ZEROES          = 0xF0;
        public const int MAX_CONSECUTIVE_ZEROES_RLE       = 16;
        public const byte MAX_CONSECUTIVE_ZEROES_ENCODING = 0x0F;
        public const int LEADING_BIT_MASK                 = 0x08;
        public const int NBYTES_BIT_MASK                  = 0x07;
        public const int MAX_ENCODE_FRAME_SIZE            = 17; // 1 + 2*8

        #region static IsNonRLE()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsNonRLE(byte flags) {
            return (flags & SIGNAL_REPEATING_ZEROES) != SIGNAL_REPEATING_ZEROES;
        }
        #endregion
    }
}
