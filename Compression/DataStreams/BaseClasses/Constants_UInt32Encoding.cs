using System.Runtime.CompilerServices;
using System.Diagnostics;


#region FORMAT EXPLANATION
// Similar to akumuli, but with a better usage of flag byte
//
// byte    flag
// Int32   value1
// Int32   value2
// 
// value1/value2 n bytes are encoded in flag
//
// flag format
// 0x11 = 0001 0001 (binary)
//        ---- ----
//        ^         first encoded value
//             ^    second encoded value
// 0x11 = 0001 0001 (binary)
//        ^  ^
//        |  |
//        |last bit:  0 = trailing bytes
//        |           1 = leading bytes
//        top 3 bits: [0-4]: n bytes length
//                    [5-7]: special case, RLE encoding (for zeroes)
//                           combines with the [lower 5 bits] for a value range of [ (7 - 5 + 1) * 32 ] = 96 combinations
//                           1-2 zeroes can be encoded already in one byte (0x0F or 0x00) 
//                           as a result, there is no point to add support for that range in RLE coding, 
//                           so RLE range is always 96 combinations [0-95] + 3 = [3-98]
// ex: 3-consecutive zeroes RLE:  1010 0000
// ex: 4-consecutive zeroes RLE:  1010 0010
// ex: 98-consecutive zeroes RLE: 1111 1111
//
// other special case
//
// 0x?F = ???? 1111
//             ^    indicates there is no secondary encoded value
#endregion

namespace TimeSeriesDB.DataStreams.BaseClasses
{
    /// <summary>
    ///     Stores efficiently a stream of uint values.
    ///     Will store either the top-most or bottom-most bytes, depending on the side having the most zeroes.
    ///     Does automatic RLE on zero values.
    /// </summary>
    public static class Constants_UInt32Encoding {
        public const int SIGNAL_SINGLE_ITEM               = 0x0F;
        public const int MAX_CONSECUTIVE_ZEROES_RLE       = 98;
        public const byte TWO_CONSECUTIVE_ZEROES_ENCODING = 0x00;
        public const int MAX_ENCODE_FRAME_SIZE            = 9; // 1 + 2*4

        public const int LEADING_BIT_MASK = 0x01; // 0001b
        public const int NBYTES_BIT_MASK  = 0x0E; // 1110b
        public const int NBYTES_SHIFT     = 1;

        private const int MAX_NON_RLE_VALUE = 4; // "<= x" means not RLE

        #region static EncodeConsecutiveZeroes()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte EncodeConsecutiveZeroes(int zeroes_rle) {
            if(zeroes_rle == 1)
                return 0x00 | SIGNAL_SINGLE_ITEM;
            if(zeroes_rle == 2)
                return TWO_CONSECUTIVE_ZEROES_ENCODING;

            Debug.Assert(zeroes_rle > 2 && zeroes_rle <= MAX_CONSECUTIVE_ZEROES_RLE);

            return unchecked((byte)(zeroes_rle - 3 + 0xA0));
        }
        #endregion
        #region static DecodeConsecutiveZeroes()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte DecodeConsecutiveZeroes(byte zeroes_rle) {
            if(zeroes_rle == TWO_CONSECUTIVE_ZEROES_ENCODING)
                return 2;

            Debug.Assert(zeroes_rle >= 0xA0); // if assert doesnt pass, return 0 ?

            return unchecked((byte)(zeroes_rle - 0xA0 + 3));
        }
        #endregion
        #region static IsNonRLE()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsNonRLE(byte flag1) {
            return (flag1 & NBYTES_BIT_MASK) <= (MAX_NON_RLE_VALUE << NBYTES_SHIFT);
        }
        #endregion
    }
}
