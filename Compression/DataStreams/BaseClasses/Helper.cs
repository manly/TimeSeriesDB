using System;
using System.IO;
using System.Runtime.CompilerServices;


namespace TimeSeriesDB.DataStreams.BaseClasses {
    using Internal;

    public static class Helper {
        #region static StreamReset()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void StreamReset(Stream source) {
            //if(source is DataStreams.IResettableStream reset)
            //    reset.Reset();

            if(source.CanSeek)
                source.Position = 0;
        }
        #endregion
        #region static StreamZero()
        private const int CLEAR_BUFFER_SIZE = 131072;
        private static readonly byte[] CLEAR_BUFFER = new byte[CLEAR_BUFFER_SIZE];
        /// <summary>
        ///     Writes zeroes.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void StreamZero(Stream stream, long count) {
            // const int defaultCopyBufferSize = 81920;
            // var buffer = new byte[defaultCopyBufferSize];
            
            while(count > 0) {
                int clear = unchecked((int)Math.Min(count, CLEAR_BUFFER_SIZE));
                stream.Write(CLEAR_BUFFER, 0, clear);
                count -= clear;
            }
        }
        #endregion

        #region static DeltaAdd()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong DeltaAdd(ulong value1, ulong value2) {
            // The representation of integers in practically every modern hardware CPU is two's complement. 
            // One of the reasons is that math using two's complement binary numbers has a very nice property: 
            // you get the same binary representation for addition and subtraction whether you're dealing with signed or unsigned values.
            // This means that you can just cast the adjustment value to ulong to do your addition. 
            // According to the C# language rules, the cast will simply reinterpret the signed value's binary representation as unsigned. 
            // And adding the unsigned value will have exactly the same effect as if you'd added a signed value with that same binary representation.

            var newValue2 = BitMethods.UnsignedToSigned(value2);
            return unchecked(value1 + (ulong)newValue2);

            //if(newValue2 >= 0)
            //    return value1 + (ulong)newValue2;
            //else
            //    return unchecked(value1 - (ulong)(-newValue2));
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint DeltaAdd(uint value1, uint value2) {
            // avoid upcasting to long/ulong

            var newValue2 = BitMethods.UnsignedToSigned(value2);
            return unchecked(value1 + (uint)newValue2);
            //return unchecked((uint)(value1 + newValue2));
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort DeltaAdd(ushort value1, ushort value2) {
            var newValue2 = BitMethods.UnsignedToSigned(value2);
            return unchecked((ushort)(value1 + newValue2));
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte DeltaAdd(byte value1, byte value2) {
            //var newValue2 = BitMethods.UnsignedToSigned(value2);
            //return unchecked((byte)(value1 + newValue2));

            // faster to execute and always return 1 byte anyway
            // we can't use overflows on bytes because substracting on bytes upcasts them into int32, making this impossible
            return unchecked((byte)((value1 + value2) % 256));
        }
        #endregion
        #region static DeltaRemove()
        /// <summary>
        ///     This method can be vectorized.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong DeltaRemove(ulong value1, ulong value2) {
            return BitMethods.SignedToUnsigned(unchecked((long)(value1 - value2)));
        }
        /// <summary>
        ///     This method can be vectorized.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint DeltaRemove(uint value1, uint value2) {
            return BitMethods.SignedToUnsigned(unchecked((int)(value1 - value2)));
        }
        /// <summary>
        ///     This method can be vectorized.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort DeltaRemove(ushort value1, ushort value2) {
            return BitMethods.SignedToUnsigned(unchecked((short)(value1 - value2)));
        }
        /// <summary>
        ///     This method can be vectorized.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static byte DeltaRemove(byte value1, byte value2) {
            //return BitMethods.SignedToUnsigned(unchecked((sbyte)(value1 - value2)));

            // faster to execute and always return 1 byte anyway
            // we can't use overflows on bytes because substracting on bytes upcasts them into int32, making this impossible
            int diff = value2 - value1;
            return unchecked(diff >= 0 ? (byte)diff : (byte)(256 + diff));
        }
        #endregion
    }
}
