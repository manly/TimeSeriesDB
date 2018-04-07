using System.Runtime.CompilerServices;
using System.Diagnostics;


#region FORMAT EXPLANATION
// Similar to akumuli, but with a better usage of flag byte
//
// this format is made for ordered/monotonic data that uses the least-important bits first
// this format is not suitable for any other case. This means any negative value will force 
// a full encoding everytime because of the initial 1 to mark that the value is negative.
//
// byte    flag
// Int64   value1
// Int64   value2
// 
// value1/value2 lower n bytes are encoded in flag
//
// flag format
// 0x88 = 1000 1000 (binary)
//        ---- ----
//        ^         first encoded value
//             ^    second encoded value
// 0x88 = 1000 1000 (binary)
//        ^
//        |
//        [0-8]:      0-8 bytes
//        [9-15]:     special case, RLE encoding (for zeroes)
//                    combines with the lower 4 bits for a value range of [ (15 - 9 + 1) * 16 ] = 112 combinations
//                    1-2 zeroes can be encoded already in one byte (0x0F or 0x00) 
//                    as a result, there is no point to add support for that range in RLE coding, 
//                    so RLE range is always 112 combinations [0-111] + 3 = [3-114]
//
// other special case
//
// 0x?F = ???? 1111
//             ^    indicates there is no secondary encoded value
#endregion

namespace TimeSeriesDB.DataStreams.BaseClasses
{
    /// <summary>
    ///     Stores efficiently a stream of ulong values whose content is stored in LSB (least significant bytes).
    ///     Will cut the most significant bytes that are zeroes.
    ///     Does automatic RLE on zero values.
    ///     Use SignedToUnsigned() for efficient negative values encoding.
    /// </summary>
    public static class Constants_UInt64Encoding_LSB {
        public const int SIGNAL_SINGLE_ITEM               = 0x0F;
        public const int MAX_CONSECUTIVE_ZEROES_RLE       = 114;
        public const byte TWO_CONSECUTIVE_ZEROES_ENCODING = 0x00;
        public const int MAX_ENCODE_FRAME_SIZE            = 17; // 1 + 2*8

        private const byte MAX_NON_RLE_VALUE = 8;
        
        #region static EncodeConsecutiveZeroes()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte EncodeConsecutiveZeroes(int zeroes_rle) {
            if(zeroes_rle == 2)
                return TWO_CONSECUTIVE_ZEROES_ENCODING;

            Debug.Assert(zeroes_rle > 2 && zeroes_rle <= MAX_CONSECUTIVE_ZEROES_RLE);

            return unchecked((byte)(zeroes_rle - 3 + 0x90));
        }
        #endregion
        #region static DecodeConsecutiveZeroes()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte DecodeConsecutiveZeroes(byte zeroes_rle) {
            if(zeroes_rle == TWO_CONSECUTIVE_ZEROES_ENCODING)
                return 2;

            Debug.Assert(zeroes_rle >= 0x90); // if assert doesnt pass, return 0 ?

            return unchecked((byte)(zeroes_rle - 0x90 + 3));
        }
        #endregion
        #region static IsNonRLE()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsNonRLE(int flag1) {
            return flag1 <= MAX_NON_RLE_VALUE;
        }
        #endregion
    }
}
