#define USING_SYSTEM_RUNTIME_COMPILERSERVICES_UNSAFE_NUGET_PACKAGE
#define USE_NETINTRINSICS_NUGET_PACKAGE // if using the NetIntrinsics NuGet package that injects CPU intrinsics on methods to run faster
#define NON_PORTABLE_CODE

using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using static System.Runtime.CompilerServices.MethodImplOptions;
using System.Diagnostics;
using System.Security;
using System.Runtime.InteropServices;
using System.Text;
using System.Globalization;
using System.Numerics;


namespace TimeSeriesDB.Internal
{
    using IO;

    public static class BitMethods {
        public static readonly int MAX_VAR_UINT64_ENCODED_LENGTH = CalculateVarInt64Length(ulong.MaxValue);

        #region static CountZeroBytes()
        /// <summary>
        ///     Returns the leading/trailing bytes zero count.
        ///     
        ///     This method was 2x slower to execute when returning the struct rather than make it an out parameter, even using ref/stackallocs/readonly structs.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static void CountZeroBytes(ulong value, out Zeroes zeroes) {
            if(value == 0) {
                zeroes = new Zeroes(8, 8);
                return;
            }

#if USE_NETINTRINSICS_NUGET_PACKAGE
            zeroes = new Zeroes(
                unchecked((byte)((63 - System.Intrinsic.BitScanReverse(value)) >> 3)),
                unchecked((byte)(System.Intrinsic.BitScanForward(value) >> 3)));
#else
            if(value <= 0x0000_0000_FFFF_FFFFul) {
                if(value <= 0x0000_0000_0000_FFFFul) {
                    if(value <= 0x0000_0000_0000_00FFul)
                        zeroes = new Zeroes(7, 0);
                    else
                        zeroes = new Zeroes(6, (value & 0x0000_0000_0000_00FFul) != 0 ? (byte)0 : (byte)1);
                } else {
                    if(value <= 0x0000_0000_00FF_FFFFul) {
                        if((value & 0x0000_0000_0000_00FFul) != 0)
                            zeroes = new Zeroes(5, 0);
                        else
                            zeroes = new Zeroes(5, (value & 0x0000_0000_0000_FFFFul) != 0 ? (byte)1 : (byte)2);
                    } else {
                        if((value & 0x0000_0000_0000_FFFFul) != 0)
                            zeroes = new Zeroes(4, (value & 0x0000_0000_0000_00FFul) != 0 ? (byte)0 : (byte)1);
                        else
                            zeroes = new Zeroes(4, (value & 0x0000_0000_00FF_FFFFul) != 0 ? (byte)2 : (byte)3);
                    }
                }
            } else {
                if(value > 0x0000_FFFF_FFFF_FFFFul) {
                    if(value > 0x00FF_FFFF_FFFF_FFFFul) {
                        if((value & 0x0000_0000_FFFF_FFFFul) != 0) {
                            if((value & 0x0000_0000_0000_FFFFul) != 0)
                                zeroes = new Zeroes(0, (value & 0x0000_0000_0000_00FFul) != 0 ? (byte)0 : (byte)1);
                            else
                                zeroes = new Zeroes(0, (value & 0x0000_0000_00FF_FFFFul) != 0 ? (byte)2 : (byte)3);
                        } else {
                            if((value & 0x0000_FFFF_FFFF_FFFFul) == 0)
                                zeroes = new Zeroes(0, (value & 0x00FF_FFFF_FFFF_FFFFul) == 0 ? (byte)7 : (byte)6);
                            else
                                zeroes = new Zeroes(0, (value & 0x0000_00FF_FFFF_FFFFul) != 0 ? (byte)4 : (byte)5);
                        }
                    } else {
                        if((value & 0x0000_0000_FFFF_FFFFul) != 0) {
                            if((value & 0x0000_0000_0000_FFFFul) != 0)
                                zeroes = new Zeroes(1, (value & 0x0000_0000_0000_00FFul) != 0 ? (byte)0 : (byte)1);
                            else
                                zeroes = new Zeroes(1, (value & 0x0000_0000_00FF_FFFFul) != 0 ? (byte)2 : (byte)3);
                        } else {
                            if((value & 0x0000_FFFF_FFFF_FFFFul) == 0)
                                zeroes = new Zeroes(1, 6);
                            else
                                zeroes = new Zeroes(1, (value & 0x0000_00FF_FFFF_FFFFul) != 0 ? (byte)4 : (byte)5);
                        }
                    }
                } else {
                    if(value <= 0x0000_00FF_FFFF_FFFFul) {
                        if((value & 0x0000_0000_0000_FFFFul) != 0)
                            zeroes = new Zeroes(3, (value & 0x0000_0000_0000_00FFul) != 0 ? (byte)0 : (byte)1);
                        else {
                            if((value & 0x0000_0000_00FF_FFFFul) != 0)
                                zeroes = new Zeroes(3, 2);
                            else 
                                zeroes = new Zeroes(3, (value & 0x0000_0000_FFFF_FFFFul) != 0 ? (byte)3 : (byte)4);
                        }
                    } else {
                        if((value & 0x0000_0000_FFFF_FFFFul) != 0) {
                            if((value & 0x0000_0000_0000_FFFFul) != 0)
                                zeroes = new Zeroes(2, (value & 0x0000_0000_0000_00FFul) != 0 ? (byte)0 : (byte)1);
                            else
                                zeroes = new Zeroes(2, (value & 0x0000_0000_00FF_FFFFul) != 0 ? (byte)2 : (byte)3);
                        } else
                            zeroes = new Zeroes(2, (value & 0x0000_00FF_FFFF_FFFFul) != 0 ? (byte)4 : (byte)5);
                    }
                }
            }
#endif
        }
        /// <summary>
        ///     Returns the leading/trailing zero bytes count.
        ///     
        ///     This method was 2x slower to execute when returning the struct rather than make it an out parameter, even using ref/stackallocs/readonlys.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static void CountZeroBytes(uint value, out Zeroes zeroes) {
            if(value == 0) {
                zeroes = new Zeroes(4, 4);
                return;
            }

#if USE_NETINTRINSICS_NUGET_PACKAGE
            zeroes = new Zeroes(
                unchecked((byte)((31 - System.Intrinsic.BitScanReverse(value)) >> 3)),
                unchecked((byte)(System.Intrinsic.BitScanForward(value) >> 3)));
#else
            if(value <= 0x0000_FFFFu) {
                if(value <= 0x0000_00FFu)
                    zeroes = new Zeroes(3, 0);
                else
                    zeroes = new Zeroes(2, (value & 0x0000_00FFul) != 0 ? (byte)0 : (byte)1);
            } else {
                if(value <= 0x00FF_FFFFu) {
                    if((value & 0x0000_00FFu) != 0)
                        zeroes = new Zeroes(1, 0);
                    else
                        zeroes = new Zeroes(1, (value & 0x0000_FFFFu) != 0 ? (byte)1 : (byte)2);
                } else {
                    if((value & 0x0000_FFFFu) != 0)
                        zeroes = new Zeroes(0, (value & 0x0000_00FFu) != 0 ? (byte)0 : (byte)1);
                    else
                        zeroes = new Zeroes(0, (value & 0x00FF_FFFFu) != 0 ? (byte)2 : (byte)3);
                }
            }
#endif
        }
        readonly public ref struct Zeroes {
            /// <summary>
            ///     Counts the number of bytes containing zeroes in the most (higher/left side) significant parts of the value.
            ///     ex: 0x0000_FF00_0000_0000 = 2.
            /// </summary>
            public readonly byte LeadingZeroes;
            /// <summary>
            ///     Counts the number of bytes containing zeroes in the least (lower/right side) significant parts of the value.
            ///     ex: 0x0000_FF00_0000_0000 = 5.
            /// </summary>
            public readonly byte TrailingZeroes;

            public Zeroes(byte leading, byte trailing) {
                this.LeadingZeroes = leading;
                this.TrailingZeroes = trailing;
            }

            public override string ToString() {
                return string.Format("{0}-{1}", this.LeadingZeroes, this.TrailingZeroes);
            }
        }
        #endregion
        #region static CountLeadingZeroBytes()
        /// <summary>
        ///     Counts the number of bytes containing zeroes in the most (higher/left side) significant parts of the value.
        ///     Use SignedToUnsigned() if you need to process a signed integer.
        ///     ex: 0x0000_FF00_0000_0000 = 2.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int CountLeadingZeroBytes(ulong value) {
            if(value == 0)
                return 8;

#if USE_NETINTRINSICS_NUGET_PACKAGE
            return (63 - System.Intrinsic.BitScanReverse(value)) >> 3;
#else            
            if(value <= 0x0000_0000_FFFF_FFFFul) {
                if(value <= 0x0000_0000_0000_FFFFul)
                    return value <= 0x0000_0000_0000_00FFul ? 7 : 6;
                else
                    return value <= 0x0000_0000_00FF_FFFFul ? 5 : 4;
            } else {
                if(value > 0x0000_FFFF_FFFF_FFFFul)
                    return value > 0x00FF_FFFF_FFFF_FFFFul ? 0 : 1;
                else
                    return value <= 0x0000_00FF_FFFF_FFFFul ? 3 : 2;
            }
#endif
        }
        /// <summary>
        ///     Counts the number of bytes containing zeroes in the most (higher/left side) significant parts of the value.
        ///     Use SignedToUnsigned() if you need to process a signed integer.
        ///     ex: 0x0000_FF00 = 2.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int CountLeadingZeroBytes(uint value) {
            if(value == 0)
                return 4;

#if USE_NETINTRINSICS_NUGET_PACKAGE
            return (31 - System.Intrinsic.BitScanReverse(value)) >> 3;
#else
            if(value <= 0x0000_FFFFu)
                return value <= 0x0000_00FFu ? 3 : 2;
            else
                return value <= 0x00FF_FFFFu ? 1 : 0;

            //const int BITS = sizeof(int) * 8;
            //value |= value >> 1;
            //value |= value >> 2;
            //value |= value >> 4;
            //value |= value >> 8;
            //value |= value >> 16;
            //value -= value >> 1 & 0x5555_5555u;
            //value = (value >> 2 & 0x3333_3333u) + (value & 0x3333_3333u);
            //value = (value >> 4) + (value & 0x0F0F_0F0Fu);
            //value += value >> 8;
            //value += value >> 16;
            //return unchecked((int)(BITS - (value & 0x0000_003Fu)));
#endif
        }
        #endregion
        #region static CountTrailingZeroBytes()
        /// <summary>
        ///     Counts the number of bytes containing zeroes in the least (lower/right side) significant parts of the value.
        ///     Use SignedToUnsigned() if you need to process a signed integer.
        ///     ex: 0x0000_FF00_0000_0000 = 5.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int CountTrailingZeroBytes(ulong value) {
            if(value == 0)
                return 8;

#if USE_NETINTRINSICS_NUGET_PACKAGE
            return System.Intrinsic.BitScanForward(value) >> 3;
#else
            if((value & 0x0000_0000_FFFF_FFFFul) != 0) {
                if((value & 0x0000_0000_0000_FFFFul) != 0)
                    return (value & 0x0000_0000_0000_00FFul) != 0 ? 0 : 1;
                else
                    return (value & 0x0000_0000_00FF_FFFFul) != 0 ? 2 : 3;
            } else {
                if((value & 0x0000_FFFF_FFFF_FFFFul) == 0)
                    return (value & 0x00FF_FFFF_FFFF_FFFFul) == 0 ? 7 : 6;
                else
                    return (value & 0x0000_00FF_FFFF_FFFFul) != 0 ? 4 : 5;
            }
#endif
        }
        /// <summary>
        ///     Counts the number of bytes containing zeroes in the least (lower/right side) significant parts of the value.
        ///     Use SignedToUnsigned() if you need to process a signed integer.
        ///     ex: 0x0000_FF00 = 1.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int CountTrailingZeroBytes(uint value) {
            if(value == 0)
                return 4;

#if USE_NETINTRINSICS_NUGET_PACKAGE
            return System.Intrinsic.BitScanForward(value) >> 3;
#else
            if((value & 0x0000_FFFFu) != 0)
                return (value & 0x0000_00FFu) != 0 ? 0 : 1;
            else
                return (value & 0x00FF_FFFFu) != 0 ? 2 : 3;
#endif
        }
        #endregion
        #region static SignedToUnsigned()
        /// <summary>
        ///     Encodes negative values as a LSB flag for better compressibility.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static ulong SignedToUnsigned(long value) {
            if(value >= 0)
                return unchecked((ulong)value) << 1;
            else
                return (unchecked((ulong)(~value)) << 1) | 1;
        }
        /// <summary>
        ///     Encodes negative values as a LSB flag for better compressibility.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static uint SignedToUnsigned(int value) {
            if(value >= 0)
                return unchecked((uint)value) << 1;
            else
                return (unchecked((uint)(~value)) << 1) | 1;
        }
        /// <summary>
        ///     Encodes negative values as a LSB flag for better compressibility.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static ushort SignedToUnsigned(short value) {
            if(value >= 0)
                // no bitshift operator on ushort
                return unchecked((ushort)((uint)value << 1));
            else
                return unchecked((ushort)(((uint)(~value) << 1) | 1));
        }
        /// <summary>
        ///     Encodes negative values as a LSB flag for better compressibility.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static byte SignedToUnsigned(sbyte value) {
            if(value >= 0)
                // no bitshift operator on sbyte
                return unchecked((byte)((uint)value << 1));
            else
                return unchecked((byte)(((uint)(~value) << 1) | 1));
        }
        #endregion
        #region static UnsignedToSigned()
        /// <summary>
        ///     Encodes negative values as a LSB flag for better compressibility.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static long UnsignedToSigned(ulong value) {
            if((value & 1) == 0)
                return unchecked((long)(value >> 1));
            else
                return unchecked(~(long)(value >> 1));
        }
        /// <summary>
        ///     Encodes negative values as a LSB flag for better compressibility.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int UnsignedToSigned(uint value) {
            if((value & 1) == 0)
                return unchecked((int)(value >> 1));
            else
                return unchecked(~(int)(value >> 1));
        }
        /// <summary>
        ///     Encodes negative values as a LSB flag for better compressibility.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static short UnsignedToSigned(ushort value) {
            if((value & 1) == 0)
                return unchecked((short)(value >> 1));
            else {
                // no bitshift operator on ushort
                return unchecked((short)((~((uint)value >> 1)) & 0xFFFF));
            }
        }
        /// <summary>
        ///     Encodes negative values as a LSB flag for better compressibility.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static sbyte UnsignedToSigned(byte value) {
            if((value & 1) == 0)
                return unchecked((sbyte)(value >> 1));
            else {
                // no bitshift operator on byte
                return unchecked((sbyte)((~((uint)value >> 1)) & 0xFF));
            }
        }
        #endregion
        #region static ReadVarUInt64()
        /// <summary>
        ///     Read variable length LE-encoded (little endian) uint64.
        ///     Use UnsignedToSigned() if you need to process a signed integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static ulong ReadVarUInt64(byte[] buffer, ref int index) {
            // written specifically for optimum performance while keeping good compression

            ulong res;
            byte c = buffer[index++];

            if((c & 0x80) == 0x00) // 0
                return c;
            if((c & 0xFC) < 0xF8) { // 1-4
                if((c & 0xF0) < 0xE0) { // 1-2
                    if((c & 0xC0) == 0x80) // 1
                        res = ((ulong)c & 0x3F) |
                            ((ulong)buffer[index++] << 6);
                    else { // 2
                        res = ((ulong)c & 0x1F) |
                            ((ulong)buffer[index + 0] << 5) |
                            ((ulong)buffer[index + 1] << 13);
                        index += 2;
                    }
                } else { // 3-4
                    if((c & 0xF0) == 0xE0) { // 3
                        res = ((ulong)c & 0x0F) |
                            ((ulong)buffer[index + 0] << 4) |
                            ((ulong)buffer[index + 1] << 12) |
                            ((ulong)buffer[index + 2] << 20);
                        index += 3;
                    } else { // 4
                        res = ((ulong)c & 0x07) |
                            ((ulong)buffer[index + 0] << 3) |
                            ((ulong)buffer[index + 1] << 11) |
                            ((ulong)buffer[index + 2] << 19) |
                            ((ulong)buffer[index + 3] << 27);
                        index += 4;
                    }
                }
            } else { // 5-8
                if(c >= 0xFE) { // 7-8
                    if(c == 0xFF) { // 8
                        res = //((ulong)c & 0x00) |
                            ((ulong)buffer[index + 0] << 0) |
                            ((ulong)buffer[index + 1] << 8) |
                            ((ulong)buffer[index + 2] << 16) |
                            ((ulong)buffer[index + 3] << 24) |
                            ((ulong)buffer[index + 4] << 32) |
                            ((ulong)buffer[index + 5] << 40) |
                            ((ulong)buffer[index + 6] << 48) |
                            ((ulong)buffer[index + 7] << 56);
                        index += 8;
                    } else { // 7
                        res = //((ulong)c & 0x00) |
                            ((ulong)buffer[index + 0] << 0) |
                            ((ulong)buffer[index + 1] << 8) |
                            ((ulong)buffer[index + 2] << 16) |
                            ((ulong)buffer[index + 3] << 24) |
                            ((ulong)buffer[index + 4] << 32) |
                            ((ulong)buffer[index + 5] << 40) |
                            ((ulong)buffer[index + 6] << 48);
                        index += 7;
                    }
                } else { // 5-6
                    if((c & 0xFC) == 0xF8) { // 5
                        res = ((ulong)c & 0x03) |
                            ((ulong)buffer[index + 0] << 2) |
                            ((ulong)buffer[index + 1] << 10) |
                            ((ulong)buffer[index + 2] << 18) |
                            ((ulong)buffer[index + 3] << 26) |
                            ((ulong)buffer[index + 4] << 34);
                        index += 5;
                    } else { // 6
                        res = ((ulong)c & 0x01) |
                            ((ulong)buffer[index + 0] << 1) |
                            ((ulong)buffer[index + 1] << 9) |
                            ((ulong)buffer[index + 2] << 17) |
                            ((ulong)buffer[index + 3] << 25) |
                            ((ulong)buffer[index + 4] << 33) |
                            ((ulong)buffer[index + 5] << 41);
                        index += 6;
                    }
                }
            }
            return res;
        }
        /// <summary>
        ///     Read variable length LE-encoded (little endian) uint64.
        ///     Use UnsignedToSigned() if you need to process a signed integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static ulong ReadVarUInt64(byte[] buffer, ref int index, ref int read, Stream stream) {
            // written specifically for optimum performance while keeping good compression

            if(index == read) {
                read = stream.Read(buffer, 0, buffer.Length);
                index = 0;
            }

            byte c = buffer[index++];

            if((c & 0x80) == 0x00) // 0
                return c;

            ulong res;
            int remaining = buffer.Length - index;

            if((c & 0xFC) < 0xF8) { // 1-4
                if((c & 0xF0) < 0xE0) { // 1-2
                    if((c & 0xC0) == 0x80) { // 1
                        if(remaining == 0) {
                            read = stream.Read(buffer, 0, buffer.Length);
                            index = 0;
                        }
                        res = ((ulong)c & 0x3F) |
                            ((ulong)buffer[index++] << 6);
                    } else { // 2
                        if(remaining <= 1) {
                            if(remaining >= 1) buffer[0] = buffer[index++];
                            read = stream.Read(buffer, remaining, buffer.Length - remaining) + remaining;
                            index = 0;
                        }
                        res = ((ulong)c & 0x1F) |
                            ((ulong)buffer[index + 0] << 5) |
                            ((ulong)buffer[index + 1] << 13);
                        index += 2;
                    }
                } else { // 3-4
                    if((c & 0xF0) == 0xE0) { // 3
                        if(remaining <= 2) {
                            if(remaining >= 1) buffer[0] = buffer[index++];
                            if(remaining >= 2) buffer[1] = buffer[index++];
                            read = stream.Read(buffer, remaining, buffer.Length - remaining) + remaining;
                            index = 0;
                        }
                        res = ((ulong)c & 0x0F) |
                            ((ulong)buffer[index + 0] << 4) |
                            ((ulong)buffer[index + 1] << 12) |
                            ((ulong)buffer[index + 2] << 20);
                        index += 3;
                    } else { // 4
                        if(remaining <= 3) {
                            if(remaining >= 1) buffer[0] = buffer[index++];
                            if(remaining >= 2) buffer[1] = buffer[index++];
                            if(remaining >= 3) buffer[2] = buffer[index++];
                            read = stream.Read(buffer, remaining, buffer.Length - remaining) + remaining;
                            index = 0;
                        }
                        res = ((ulong)c & 0x07) |
                            ((ulong)buffer[index + 0] << 3) |
                            ((ulong)buffer[index + 1] << 11) |
                            ((ulong)buffer[index + 2] << 19) |
                            ((ulong)buffer[index + 3] << 27);
                        index += 4;
                    }
                }
            } else { // 5-8
                if(c >= 0xFE) { // 7-8
                    if(c == 0xFF) { // 8
                        if(remaining <= 7) {
                            if(remaining >= 1) buffer[0] = buffer[index++];
                            if(remaining >= 2) buffer[1] = buffer[index++];
                            if(remaining >= 3) buffer[2] = buffer[index++];
                            if(remaining >= 4) buffer[3] = buffer[index++];
                            if(remaining >= 5) buffer[4] = buffer[index++];
                            if(remaining >= 6) buffer[5] = buffer[index++];
                            if(remaining >= 7) buffer[6] = buffer[index++];
                            read = stream.Read(buffer, remaining, buffer.Length - remaining) + remaining;
                            index = 0;
                        }
                        res = //((ulong)c & 0x00) |
                            ((ulong)buffer[index + 0] << 0) |
                            ((ulong)buffer[index + 1] << 8) |
                            ((ulong)buffer[index + 2] << 16) |
                            ((ulong)buffer[index + 3] << 24) |
                            ((ulong)buffer[index + 4] << 32) |
                            ((ulong)buffer[index + 5] << 40) |
                            ((ulong)buffer[index + 6] << 48) |
                            ((ulong)buffer[index + 7] << 56);
                        index += 8;
                    } else { // 7
                        if(remaining <= 6) {
                            if(remaining >= 1) buffer[0] = buffer[index++];
                            if(remaining >= 2) buffer[1] = buffer[index++];
                            if(remaining >= 3) buffer[2] = buffer[index++];
                            if(remaining >= 4) buffer[3] = buffer[index++];
                            if(remaining >= 5) buffer[4] = buffer[index++];
                            if(remaining >= 6) buffer[5] = buffer[index++];
                            read = stream.Read(buffer, remaining, buffer.Length - remaining) + remaining;
                            index = 0;
                        }
                        res = //((ulong)c & 0x00) |
                            ((ulong)buffer[index + 0] << 0) |
                            ((ulong)buffer[index + 1] << 8) |
                            ((ulong)buffer[index + 2] << 16) |
                            ((ulong)buffer[index + 3] << 24) |
                            ((ulong)buffer[index + 4] << 32) |
                            ((ulong)buffer[index + 5] << 40) |
                            ((ulong)buffer[index + 6] << 48);
                        index += 7;
                    }
                } else { // 5-6
                    if((c & 0xFC) == 0xF8) { // 5
                        if(remaining <= 4) {
                            if(remaining >= 1) buffer[0] = buffer[index++];
                            if(remaining >= 2) buffer[1] = buffer[index++];
                            if(remaining >= 3) buffer[2] = buffer[index++];
                            if(remaining >= 4) buffer[3] = buffer[index++];
                            read = stream.Read(buffer, remaining, buffer.Length - remaining) + remaining;
                            index = 0;
                        }
                        res = ((ulong)c & 0x03) |
                            ((ulong)buffer[index + 0] << 2) |
                            ((ulong)buffer[index + 1] << 10) |
                            ((ulong)buffer[index + 2] << 18) |
                            ((ulong)buffer[index + 3] << 26) |
                            ((ulong)buffer[index + 4] << 34);
                        index += 5;
                    } else { // 6
                        if(remaining <= 5) {
                            if(remaining >= 1) buffer[0] = buffer[index++];
                            if(remaining >= 2) buffer[1] = buffer[index++];
                            if(remaining >= 3) buffer[2] = buffer[index++];
                            if(remaining >= 4) buffer[3] = buffer[index++];
                            if(remaining >= 5) buffer[4] = buffer[index++];
                            read = stream.Read(buffer, remaining, buffer.Length - remaining) + remaining;
                            index = 0;
                        }
                        res = ((ulong)c & 0x01) |
                            ((ulong)buffer[index + 0] << 1) |
                            ((ulong)buffer[index + 1] << 9) |
                            ((ulong)buffer[index + 2] << 17) |
                            ((ulong)buffer[index + 3] << 25) |
                            ((ulong)buffer[index + 4] << 33) |
                            ((ulong)buffer[index + 5] << 41);
                        index += 6;
                    }
                }
            }
            return res;
        }
        #endregion
        #region static ReadVarUInt64_ThrowOnEOS()
        /// <summary>
        ///     Read variable length LE-encoded (little endian) uint64.
        ///     Throws on EndOfStream.
        ///     Use UnsignedToSigned() if you need to process a signed integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static ulong ReadVarUInt64_ThrowOnEOS(byte[] buffer, ref int index, ref int read, Stream stream) {
            // written specifically for optimum performance while keeping good compression

            if(index == read) {
                if((read = stream.Read(buffer, 0, buffer.Length)) < 1)
                    throw new EndOfStreamException();
                index = 0;
            }

            byte c = buffer[index++];

            if((c & 0x80) == 0x00) // 0
                return c;

            ulong res;
            int remaining = buffer.Length - index;

            if((c & 0xFC) < 0xF8) { // 1-4
                if((c & 0xF0) < 0xE0) { // 1-2
                    if((c & 0xC0) == 0x80) { // 1
                        if(remaining == 0) {
                            if((read = stream.Read(buffer, 0, buffer.Length)) < 1)
                                throw new EndOfStreamException();
                            index = 0;
                        }
                        res = ((ulong)c & 0x3F) |
                            ((ulong)buffer[index++] << 6);
                    } else { // 2
                        if(remaining <= 1) {
                            if(remaining >= 1) buffer[0] = buffer[index++];
                            if((read = stream.Read(buffer, remaining, buffer.Length - remaining) + remaining) < 2)
                                throw new EndOfStreamException();
                            index = 0;
                        }
                        res = ((ulong)c & 0x1F) |
                            ((ulong)buffer[index + 0] << 5) |
                            ((ulong)buffer[index + 1] << 13);
                        index += 2;
                    }
                } else { // 3-4
                    if((c & 0xF0) == 0xE0) { // 3
                        if(remaining <= 2) {
                            if(remaining >= 1) buffer[0] = buffer[index++];
                            if(remaining >= 2) buffer[1] = buffer[index++];
                            if((read = stream.Read(buffer, remaining, buffer.Length - remaining) + remaining) < 3)
                                throw new EndOfStreamException();
                            index = 0;
                        }
                        res = ((ulong)c & 0x0F) |
                            ((ulong)buffer[index + 0] << 4) |
                            ((ulong)buffer[index + 1] << 12) |
                            ((ulong)buffer[index + 2] << 20);
                        index += 3;
                    } else { // 4
                        if(remaining <= 3) {
                            if(remaining >= 1) buffer[0] = buffer[index++];
                            if(remaining >= 2) buffer[1] = buffer[index++];
                            if(remaining >= 3) buffer[2] = buffer[index++];
                            if((read = stream.Read(buffer, remaining, buffer.Length - remaining) + remaining) < 4)
                                throw new EndOfStreamException();
                            index = 0;
                        }
                        res = ((ulong)c & 0x07) |
                            ((ulong)buffer[index + 0] << 3) |
                            ((ulong)buffer[index + 1] << 11) |
                            ((ulong)buffer[index + 2] << 19) |
                            ((ulong)buffer[index + 3] << 27);
                        index += 4;
                    }
                }
            } else { // 5-8
                if(c >= 0xFE) { // 7-8
                    if(c == 0xFF) { // 8
                        if(remaining <= 7) {
                            if(remaining >= 1) buffer[0] = buffer[index++];
                            if(remaining >= 2) buffer[1] = buffer[index++];
                            if(remaining >= 3) buffer[2] = buffer[index++];
                            if(remaining >= 4) buffer[3] = buffer[index++];
                            if(remaining >= 5) buffer[4] = buffer[index++];
                            if(remaining >= 6) buffer[5] = buffer[index++];
                            if(remaining >= 7) buffer[6] = buffer[index++];
                            if((read = stream.Read(buffer, remaining, buffer.Length - remaining) + remaining) < 8)
                                throw new EndOfStreamException();
                            index = 0;
                        }
                        res = //((ulong)c & 0x00) |
                            ((ulong)buffer[index + 0] << 0) |
                            ((ulong)buffer[index + 1] << 8) |
                            ((ulong)buffer[index + 2] << 16) |
                            ((ulong)buffer[index + 3] << 24) |
                            ((ulong)buffer[index + 4] << 32) |
                            ((ulong)buffer[index + 5] << 40) |
                            ((ulong)buffer[index + 6] << 48) |
                            ((ulong)buffer[index + 7] << 56);
                        index += 8;
                    } else { // 7
                        if(remaining <= 6) {
                            if(remaining >= 1) buffer[0] = buffer[index++];
                            if(remaining >= 2) buffer[1] = buffer[index++];
                            if(remaining >= 3) buffer[2] = buffer[index++];
                            if(remaining >= 4) buffer[3] = buffer[index++];
                            if(remaining >= 5) buffer[4] = buffer[index++];
                            if(remaining >= 6) buffer[5] = buffer[index++];
                            if((read = stream.Read(buffer, remaining, buffer.Length - remaining) + remaining) < 7)
                                throw new EndOfStreamException();
                            index = 0;
                        }
                        res = //((ulong)c & 0x00) |
                            ((ulong)buffer[index + 0] << 0) |
                            ((ulong)buffer[index + 1] << 8) |
                            ((ulong)buffer[index + 2] << 16) |
                            ((ulong)buffer[index + 3] << 24) |
                            ((ulong)buffer[index + 4] << 32) |
                            ((ulong)buffer[index + 5] << 40) |
                            ((ulong)buffer[index + 6] << 48);
                        index += 7;
                    }
                } else { // 5-6
                    if((c & 0xFC) == 0xF8) { // 5
                        if(remaining <= 4) {
                            if(remaining >= 1) buffer[0] = buffer[index++];
                            if(remaining >= 2) buffer[1] = buffer[index++];
                            if(remaining >= 3) buffer[2] = buffer[index++];
                            if(remaining >= 4) buffer[3] = buffer[index++];
                            if((read = stream.Read(buffer, remaining, buffer.Length - remaining) + remaining) < 5)
                                throw new EndOfStreamException();
                            index = 0;
                        }
                        res = ((ulong)c & 0x03) |
                            ((ulong)buffer[index + 0] << 2) |
                            ((ulong)buffer[index + 1] << 10) |
                            ((ulong)buffer[index + 2] << 18) |
                            ((ulong)buffer[index + 3] << 26) |
                            ((ulong)buffer[index + 4] << 34);
                        index += 5;
                    } else { // 6
                        if(remaining <= 5) {
                            if(remaining >= 1) buffer[0] = buffer[index++];
                            if(remaining >= 2) buffer[1] = buffer[index++];
                            if(remaining >= 3) buffer[2] = buffer[index++];
                            if(remaining >= 4) buffer[3] = buffer[index++];
                            if(remaining >= 5) buffer[4] = buffer[index++];
                            if((read = stream.Read(buffer, remaining, buffer.Length - remaining) + remaining) < 6)
                                throw new EndOfStreamException();
                            index = 0;
                        }
                        res = ((ulong)c & 0x01) |
                            ((ulong)buffer[index + 0] << 1) |
                            ((ulong)buffer[index + 1] << 9) |
                            ((ulong)buffer[index + 2] << 17) |
                            ((ulong)buffer[index + 3] << 25) |
                            ((ulong)buffer[index + 4] << 33) |
                            ((ulong)buffer[index + 5] << 41);
                        index += 6;
                    }
                }
            }
            return res;
        }
        #endregion
        #region static WriteVarUInt64()
        /// <summary>
        ///     Write variable length LE-encoded (little endian) uint64.
        ///     Use SignedToUnsigned() if you need to process a signed integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static void WriteVarUInt64(byte[] buffer, ref int index, ulong value) {
            // 20% speedup for using "ref int index" instead of returning the new index

            // written specifically for optimum performance while keeping good compression

            // check for 1 byte first as it is most likely case
            // then check everything in a binary-search fashion, with favor towards small/large numbers (and less medium)
            // this is based on the theory that you're more likely to store int32s, and if you store int64s, its probably 
            // less about having the number being large but more about a hash result taking all 64 bits
            // also the code favors re-comparing the same values whenever possible, in the hopes of compiler optimizing those compares away using possibly int.CompareTo(int) and using that to avoid recomparing

            //    value                   first byte  bits
            //                            (encoded_bytes)
            // <= 0x0000_0000_0000_007F   0xxx xxxx   7
            // <= 0x0000_0000_0000_3FFF   10xx xxxx   14
            // <= 0x0000_0000_001F_FFFF   110x xxxx   21
            // <= 0x0000_0000_0FFF_FFFF   1110 xxxx   28
            // <= 0x0000_0007_FFFF_FFFF   1111 0xxx   35
            // <= 0x0000_03FF_FFFF_FFFF   1111 10xx   42
            // <= 0x0001_FFFF_FFFF_FFFF   1111 110x   49
            // <= 0x00FF_FFFF_FFFF_FFFF   1111 1110   56
            // <= 0xFFFF_FFFF_FFFF_FFFF   1111 1111   64

            if(value <= 0x0000_0000_0000_007Ful) { // 0
                buffer[index++] = unchecked((byte)value);
            } else {
                if(value <= 0x0000_0007_FFFF_FFFFul) { // 1-4
                    if(value <= 0x0000_0000_001F_FFFFul) { // 1-2
                        if(value <= 0x0000_0000_0000_3FFFul) { // 1
                            buffer[index + 0] = unchecked((byte)(0x80 | (int)(value & 0x3F)));
                            buffer[index + 1] = unchecked((byte)((value >> 6) & 0xFF));
                            index += 2;
                        } else { // 2
                            buffer[index + 0] = unchecked((byte)(0xC0 | (int)(value & 0x1F)));
                            buffer[index + 1] = unchecked((byte)((value >> 5) & 0xFF));
                            buffer[index + 2] = unchecked((byte)((value >> 13) & 0xFF));
                            index += 3;
                        }
                    } else { // 3-4
                        if(value <= 0x0000_0000_0FFF_FFFFul) { // 3
                            buffer[index + 0] = unchecked((byte)(0xE0 | (int)(value & 0x0F)));
                            buffer[index + 1] = unchecked((byte)((value >> 4) & 0xFF));
                            buffer[index + 2] = unchecked((byte)((value >> 12) & 0xFF));
                            buffer[index + 3] = unchecked((byte)((value >> 20) & 0xFF));
                            index += 4;
                        } else { // 4
                            buffer[index + 0] = unchecked((byte)(0xF0 | (int)(value & 0x07)));
                            buffer[index + 1] = unchecked((byte)((value >> 3) & 0xFF));
                            buffer[index + 2] = unchecked((byte)((value >> 11) & 0xFF));
                            buffer[index + 3] = unchecked((byte)((value >> 19) & 0xFF));
                            buffer[index + 4] = unchecked((byte)((value >> 27) & 0xFF));
                            index += 5;
                        }
                    }
                } else { // 5-8
                    if(value >= 0xFFFE_0000_0000_0000ul) { // 7-8
                        if(value >= 0xFF00_0000_0000_0000ul) { // 8
                            buffer[index + 0] = 0xFF;
                            buffer[index + 1] = unchecked((byte)((value >> 0) & 0xFF));
                            buffer[index + 2] = unchecked((byte)((value >> 8) & 0xFF));
                            buffer[index + 3] = unchecked((byte)((value >> 16) & 0xFF));
                            buffer[index + 4] = unchecked((byte)((value >> 24) & 0xFF));
                            buffer[index + 5] = unchecked((byte)((value >> 32) & 0xFF));
                            buffer[index + 6] = unchecked((byte)((value >> 40) & 0xFF));
                            buffer[index + 7] = unchecked((byte)((value >> 48) & 0xFF));
                            buffer[index + 8] = unchecked((byte)((value >> 56) & 0xFF));
                            index += 9;
                        } else { // 7
                            buffer[index + 0] = 0xFE;
                            buffer[index + 1] = unchecked((byte)((value >> 0) & 0xFF));
                            buffer[index + 2] = unchecked((byte)((value >> 8) & 0xFF));
                            buffer[index + 3] = unchecked((byte)((value >> 16) & 0xFF));
                            buffer[index + 4] = unchecked((byte)((value >> 24) & 0xFF));
                            buffer[index + 5] = unchecked((byte)((value >> 32) & 0xFF));
                            buffer[index + 6] = unchecked((byte)((value >> 40) & 0xFF));
                            buffer[index + 7] = unchecked((byte)((value >> 48) & 0xFF));
                            index += 8;
                        }
                    } else { // 5-6
                        if(value <= 0x0000_03FF_FFFF_FFFFul) { // 5
                            buffer[index + 0] = unchecked((byte)(0xF8 | (int)(value & 0x03)));
                            buffer[index + 1] = unchecked((byte)((value >> 2) & 0xFF));
                            buffer[index + 2] = unchecked((byte)((value >> 10) & 0xFF));
                            buffer[index + 3] = unchecked((byte)((value >> 18) & 0xFF));
                            buffer[index + 4] = unchecked((byte)((value >> 26) & 0xFF));
                            buffer[index + 5] = unchecked((byte)((value >> 34) & 0xFF));
                            index += 6;
                        } else { // 6
                            buffer[index + 0] = unchecked((byte)(0xFC | (int)(value & 0x01)));
                            buffer[index + 1] = unchecked((byte)((value >> 1) & 0xFF));
                            buffer[index + 2] = unchecked((byte)((value >> 9) & 0xFF));
                            buffer[index + 3] = unchecked((byte)((value >> 17) & 0xFF));
                            buffer[index + 4] = unchecked((byte)((value >> 25) & 0xFF));
                            buffer[index + 5] = unchecked((byte)((value >> 33) & 0xFF));
                            buffer[index + 6] = unchecked((byte)((value >> 41) & 0xFF));
                            index += 7;
                        }
                    }
                }
            }
        }
        /// <summary>
        ///     Write variable length LE-encoded (little endian) uint64.
        ///     Use SignedToUnsigned() if you need to process a signed integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static void WriteVarUInt64(byte[] buffer, ref int index, Stream stream, ulong value) {
            // 20% speedup for using "ref int index" instead of returning the new index

            // written specifically for optimum performance while keeping good compression

            // check for 1 byte first as it is most likely case
            // then check everything in a binary-search fashion, with favor towards small/large numbers (and less medium)
            // this is based on the theory that you're more likely to store int32s, and if you store int64s, its probably 
            // less about having the number being large but more about a hash result taking all 64 bits
            // also the code favors re-comparing the same values whenever possible, in the hopes of compiler optimizing those compares away using possibly int.CompareTo(int) and using that to avoid recomparing

            //    value                   first byte  bits
            //                            (encoded_bytes)
            // <= 0x0000_0000_0000_007F   0xxx xxxx   7
            // <= 0x0000_0000_0000_3FFF   10xx xxxx   14
            // <= 0x0000_0000_001F_FFFF   110x xxxx   21
            // <= 0x0000_0000_0FFF_FFFF   1110 xxxx   28
            // <= 0x0000_0007_FFFF_FFFF   1111 0xxx   35
            // <= 0x0000_03FF_FFFF_FFFF   1111 10xx   42
            // <= 0x0001_FFFF_FFFF_FFFF   1111 110x   49
            // <= 0x00FF_FFFF_FFFF_FFFF   1111 1110   56
            // <= 0xFFFF_FFFF_FFFF_FFFF   1111 1111   64

            if(value <= 0x0000_0000_0000_007Ful) { // 0
                buffer[index++] = unchecked((byte)value);
            } else {
                int remaining = buffer.Length - index;

                if(value <= 0x0000_0007_FFFF_FFFFul) { // 1-4
                    if(value <= 0x0000_0000_001F_FFFFul) { // 1-2
                        if(value <= 0x0000_0000_0000_3FFFul) { // 1
                            if(remaining <= 1) {
                                stream.Write(buffer, 0, index);
                                index = 0;
                            }
                            buffer[index + 0] = unchecked((byte)(0x80 | (int)(value & 0x3F)));
                            buffer[index + 1] = unchecked((byte)((value >> 6) & 0xFF));
                            index += 2;
                        } else { // 2
                            if(remaining <= 2) {
                                stream.Write(buffer, 0, index);
                                index = 0;
                            }
                            buffer[index + 0] = unchecked((byte)(0xC0 | (int)(value & 0x1F)));
                            buffer[index + 1] = unchecked((byte)((value >> 5) & 0xFF));
                            buffer[index + 2] = unchecked((byte)((value >> 13) & 0xFF));
                            index += 3;
                        }
                    } else { // 3-4
                        if(value <= 0x0000_0000_0FFF_FFFFul) { // 3
                            if(remaining <= 3) {
                                stream.Write(buffer, 0, index);
                                index = 0;
                            }
                            buffer[index + 0] = unchecked((byte)(0xE0 | (int)(value & 0x0F)));
                            buffer[index + 1] = unchecked((byte)((value >> 4) & 0xFF));
                            buffer[index + 2] = unchecked((byte)((value >> 12) & 0xFF));
                            buffer[index + 3] = unchecked((byte)((value >> 20) & 0xFF));
                            index += 4;
                        } else { // 4
                            if(remaining <= 4) {
                                stream.Write(buffer, 0, index);
                                index = 0;
                            }
                            buffer[index + 0] = unchecked((byte)(0xF0 | (int)(value & 0x07)));
                            buffer[index + 1] = unchecked((byte)((value >> 3) & 0xFF));
                            buffer[index + 2] = unchecked((byte)((value >> 11) & 0xFF));
                            buffer[index + 3] = unchecked((byte)((value >> 19) & 0xFF));
                            buffer[index + 4] = unchecked((byte)((value >> 27) & 0xFF));
                            index += 5;
                        }
                    }
                } else { // 5-8
                    if(value >= 0xFFFE_0000_0000_0000ul) { // 7-8
                        if(value >= 0xFF00_0000_0000_0000ul) { // 8
                            if(remaining <= 8) {
                                stream.Write(buffer, 0, index);
                                index = 0;
                            }
                            buffer[index + 0] = 0xFF;
                            buffer[index + 1] = unchecked((byte)((value >> 0) & 0xFF));
                            buffer[index + 2] = unchecked((byte)((value >> 8) & 0xFF));
                            buffer[index + 3] = unchecked((byte)((value >> 16) & 0xFF));
                            buffer[index + 4] = unchecked((byte)((value >> 24) & 0xFF));
                            buffer[index + 5] = unchecked((byte)((value >> 32) & 0xFF));
                            buffer[index + 6] = unchecked((byte)((value >> 40) & 0xFF));
                            buffer[index + 7] = unchecked((byte)((value >> 48) & 0xFF));
                            buffer[index + 8] = unchecked((byte)((value >> 56) & 0xFF));
                            index += 9;
                        } else { // 7
                            if(remaining <= 7) {
                                stream.Write(buffer, 0, index);
                                index = 0;
                            }
                            buffer[index + 0] = 0xFE;
                            buffer[index + 1] = unchecked((byte)((value >> 0) & 0xFF));
                            buffer[index + 2] = unchecked((byte)((value >> 8) & 0xFF));
                            buffer[index + 3] = unchecked((byte)((value >> 16) & 0xFF));
                            buffer[index + 4] = unchecked((byte)((value >> 24) & 0xFF));
                            buffer[index + 5] = unchecked((byte)((value >> 32) & 0xFF));
                            buffer[index + 6] = unchecked((byte)((value >> 40) & 0xFF));
                            buffer[index + 7] = unchecked((byte)((value >> 48) & 0xFF));
                            index += 8;
                        }
                    } else { // 5-6
                        if(value <= 0x0000_03FF_FFFF_FFFFul) { // 5
                            if(remaining <= 5) {
                                stream.Write(buffer, 0, index);
                                index = 0;
                            }
                            buffer[index + 0] = unchecked((byte)(0xF8 | (int)(value & 0x03)));
                            buffer[index + 1] = unchecked((byte)((value >> 2) & 0xFF));
                            buffer[index + 2] = unchecked((byte)((value >> 10) & 0xFF));
                            buffer[index + 3] = unchecked((byte)((value >> 18) & 0xFF));
                            buffer[index + 4] = unchecked((byte)((value >> 26) & 0xFF));
                            buffer[index + 5] = unchecked((byte)((value >> 34) & 0xFF));
                            index += 6;
                        } else { // 6
                            if(remaining <= 6) {
                                stream.Write(buffer, 0, index);
                                index = 0;
                            }
                            buffer[index + 0] = unchecked((byte)(0xFC | (int)(value & 0x01)));
                            buffer[index + 1] = unchecked((byte)((value >> 1) & 0xFF));
                            buffer[index + 2] = unchecked((byte)((value >> 9) & 0xFF));
                            buffer[index + 3] = unchecked((byte)((value >> 17) & 0xFF));
                            buffer[index + 4] = unchecked((byte)((value >> 25) & 0xFF));
                            buffer[index + 5] = unchecked((byte)((value >> 33) & 0xFF));
                            buffer[index + 6] = unchecked((byte)((value >> 41) & 0xFF));
                            index += 7;
                        }
                    }
                }
            }

            if(index == buffer.Length) {
                stream.Write(buffer, 0, index);
                index = 0;
            }
        }
        #endregion
        #region static CalculateVarInt64Length()
        /// <summary>
        ///     Returns the number of bytes needed to encode a variable length uint64.
        ///     Use SignedToUnsigned() if you need to process a signed integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int CalculateVarInt64Length(ulong value) {
            if(value <= 0x0000_0000_0000_007Ful) // 0
                return 1;

            if(value <= 0x0000_0007_FFFF_FFFFul) { // 1-4
                if(value <= 0x0000_0000_001F_FFFFul) { // 1-2
                    if(value <= 0x0000_0000_0000_3FFFul) // 1
                        return 2;
                    else // 2
                        return 3;
                } else { // 3-4
                    if(value <= 0x0000_0000_0FFF_FFFFul) // 3
                        return 4;
                    else // 4
                        return 5;
                }
            } else { // 5-8
                if(value >= 0xFFFE_0000_0000_0000ul) { // 7-8
                    if(value >= 0xFF00_0000_0000_0000ul) // 8
                        return 9;
                    else // 7
                        return 8;
                } else { // 5-6
                    if(value <= 0x0000_03FF_FFFF_FFFFul) // 5
                        return 6;
                    else // 6
                        return 7;
                }
            }
        }
        #endregion
        #region static ReadUInt64()
        /// <summary>
        ///     Read LE-encoded (little endian) uint64.
        ///     Use UnsignedToSigned() if you need to process a signed integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static ulong ReadUInt64(byte[] buffer, ref int index) {
            var res = buffer[index + 0] |
                ((ulong)buffer[index + 1] << 8) |
                ((ulong)buffer[index + 2] << 16) |
                ((ulong)buffer[index + 3] << 24) |
                ((ulong)buffer[index + 4] << 32) |
                ((ulong)buffer[index + 5] << 40) |
                ((ulong)buffer[index + 6] << 48) |
                ((ulong)buffer[index + 7] << 56);
            index += 8;
            return res;
        }
        /// <summary>
        ///     Read LE-encoded (little endian) uint64.
        ///     Use UnsignedToSigned() if you need to process a signed integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static ulong ReadUInt64(byte[] buffer, ref int index, int byte_count) {
            // if you crash here, the caller needs to check for "byte_count == 0" to avoid making this call
            Debug.Assert(byte_count >= 1 && byte_count <= 8);

            ulong res;

            if(byte_count <= 4) {
                if(byte_count <= 2) {
                    if(byte_count == 1)
                        res = buffer[index++];
                    else {
                        res = buffer[index + 0] |
                            ((ulong)buffer[index + 1] << 8);
                        index += 2;
                    }
                } else {
                    if(byte_count == 3) {
                        res = buffer[index + 0] |
                            ((ulong)buffer[index + 1] << 8) |
                            ((ulong)buffer[index + 2] << 16);
                        index += 3;
                    } else {
                        res = buffer[index + 0] |
                            ((ulong)buffer[index + 1] << 8) |
                            ((ulong)buffer[index + 2] << 16) |
                            ((ulong)buffer[index + 3] << 24);
                        index += 4;
                    }
                }
            } else {
                if(byte_count >= 7) {
                    if(byte_count == 8) {
                        res = buffer[index + 0] |
                            ((ulong)buffer[index + 1] << 8) |
                            ((ulong)buffer[index + 2] << 16) |
                            ((ulong)buffer[index + 3] << 24) |
                            ((ulong)buffer[index + 4] << 32) |
                            ((ulong)buffer[index + 5] << 40) |
                            ((ulong)buffer[index + 6] << 48) |
                            ((ulong)buffer[index + 7] << 56);
                        index += 8;
                    } else {
                        res = buffer[index + 0] |
                            ((ulong)buffer[index + 1] << 8) |
                            ((ulong)buffer[index + 2] << 16) |
                            ((ulong)buffer[index + 3] << 24) |
                            ((ulong)buffer[index + 4] << 32) |
                            ((ulong)buffer[index + 5] << 40) |
                            ((ulong)buffer[index + 6] << 48);
                        index += 7;
                    }
                } else {
                    if(byte_count == 5) {
                        res = buffer[index + 0] |
                            ((ulong)buffer[index + 1] << 8) |
                            ((ulong)buffer[index + 2] << 16) |
                            ((ulong)buffer[index + 3] << 24) |
                            ((ulong)buffer[index + 4] << 32);
                        index += 5;
                    } else {
                        res = buffer[index + 0] |
                            ((ulong)buffer[index + 1] << 8) |
                            ((ulong)buffer[index + 2] << 16) |
                            ((ulong)buffer[index + 3] << 24) |
                            ((ulong)buffer[index + 4] << 32) |
                            ((ulong)buffer[index + 5] << 40);
                        index += 6;
                    }
                }
            }
            return res;
        }
        #endregion
        #region static ReadUInt32()
        /// <summary>
        ///     Read LE-encoded (little endian) uint32.
        ///     Use UnsignedToSigned() if you need to process a signed integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static uint ReadUInt32(byte[] buffer, ref int index) {
            var res = buffer[index + 0] |
                ((uint)buffer[index + 1] << 8) |
                ((uint)buffer[index + 2] << 16) |
                ((uint)buffer[index + 3] << 24);
            index += 4;
            return res;
        }
        /// <summary>
        ///     Read LE-encoded (little endian) uint32.
        ///     Use UnsignedToSigned() if you need to process a signed integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static uint ReadUInt32(byte[] buffer, ref int index, int byte_count) {
            // if you crash here, the caller needs to check for "byte_count == 0" to avoid making this call
            Debug.Assert(byte_count >= 1 && byte_count <= 4);
            uint res;

            if(byte_count <= 2) {
                if(byte_count == 1)
                    res = buffer[index++];
                else {
                    res = buffer[index + 0] |
                        ((uint)buffer[index + 1] << 8);
                    index += 2;
                }
            } else {
                if(byte_count == 3) {
                    res = buffer[index + 0] |
                        ((uint)buffer[index + 1] << 8) |
                        ((uint)buffer[index + 2] << 16);
                    index += 3;
                } else {
                    res = buffer[index + 0] |
                        ((uint)buffer[index + 1] << 8) |
                        ((uint)buffer[index + 2] << 16) |
                        ((uint)buffer[index + 3] << 24);
                    index += 4;
                }
            }
            return res;
        }
        #endregion
        #region static WriteUInt64()
        /// <summary>
        ///     Write LE-encoded (little endian) uint64.
        ///     Use SignedToUnsigned() if you need to process a signed integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static void WriteUInt64(byte[] buffer, ref int index, ulong value) {
            buffer[index + 0] = unchecked((byte)((value >> 0) & 0xFF));
            buffer[index + 1] = unchecked((byte)((value >> 8) & 0xFF));
            buffer[index + 2] = unchecked((byte)((value >> 16) & 0xFF));
            buffer[index + 3] = unchecked((byte)((value >> 24) & 0xFF));
            buffer[index + 4] = unchecked((byte)((value >> 32) & 0xFF));
            buffer[index + 5] = unchecked((byte)((value >> 40) & 0xFF));
            buffer[index + 6] = unchecked((byte)((value >> 48) & 0xFF));
            buffer[index + 7] = unchecked((byte)((value >> 56) & 0xFF));
            index += 8;
        }
        /// <summary>
        ///     Write LE-encoded (little endian) uint64.
        ///     Use SignedToUnsigned() if you need to process a signed integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static void WriteUInt64(byte[] buffer, ref int index, ulong value, int byte_count) {
            // 20% speedup for using "ref int index" instead of returning the new index

            // if you crash here, the caller needs to check for "byte_count == 0" to avoid making this call
            Debug.Assert(byte_count >= 1 && byte_count <= 8);

            if(byte_count <= 4) {
                if(byte_count <= 2) {
                    if(byte_count == 1) {
                        buffer[index++] = unchecked((byte)((value >> 0) & 0xFF));
                    } else {
                        buffer[index + 0] = unchecked((byte)((value >> 0) & 0xFF));
                        buffer[index + 1] = unchecked((byte)((value >> 8) & 0xFF));
                        index += 2;
                    }
                } else {
                    if(byte_count == 3) {
                        buffer[index + 0] = unchecked((byte)((value >> 0) & 0xFF));
                        buffer[index + 1] = unchecked((byte)((value >> 8) & 0xFF));
                        buffer[index + 2] = unchecked((byte)((value >> 16) & 0xFF));
                        index += 3;
                    } else {
                        buffer[index + 0] = unchecked((byte)((value >> 0) & 0xFF));
                        buffer[index + 1] = unchecked((byte)((value >> 8) & 0xFF));
                        buffer[index + 2] = unchecked((byte)((value >> 16) & 0xFF));
                        buffer[index + 3] = unchecked((byte)((value >> 24) & 0xFF));
                        index += 4;
                    }
                }
            } else {
                if(byte_count >= 7) {
                    if(byte_count == 8) {
                        buffer[index + 0] = unchecked((byte)((value >> 0) & 0xFF));
                        buffer[index + 1] = unchecked((byte)((value >> 8) & 0xFF));
                        buffer[index + 2] = unchecked((byte)((value >> 16) & 0xFF));
                        buffer[index + 3] = unchecked((byte)((value >> 24) & 0xFF));
                        buffer[index + 4] = unchecked((byte)((value >> 32) & 0xFF));
                        buffer[index + 5] = unchecked((byte)((value >> 40) & 0xFF));
                        buffer[index + 6] = unchecked((byte)((value >> 48) & 0xFF));
                        buffer[index + 7] = unchecked((byte)((value >> 56) & 0xFF));
                        index += 8;
                    } else {
                        buffer[index + 0] = unchecked((byte)((value >> 0) & 0xFF));
                        buffer[index + 1] = unchecked((byte)((value >> 8) & 0xFF));
                        buffer[index + 2] = unchecked((byte)((value >> 16) & 0xFF));
                        buffer[index + 3] = unchecked((byte)((value >> 24) & 0xFF));
                        buffer[index + 4] = unchecked((byte)((value >> 32) & 0xFF));
                        buffer[index + 5] = unchecked((byte)((value >> 40) & 0xFF));
                        buffer[index + 6] = unchecked((byte)((value >> 48) & 0xFF));
                        index += 7;
                    }
                } else {
                    if(byte_count == 5) {
                        buffer[index + 0] = unchecked((byte)((value >> 0) & 0xFF));
                        buffer[index + 1] = unchecked((byte)((value >> 8) & 0xFF));
                        buffer[index + 2] = unchecked((byte)((value >> 16) & 0xFF));
                        buffer[index + 3] = unchecked((byte)((value >> 24) & 0xFF));
                        buffer[index + 4] = unchecked((byte)((value >> 32) & 0xFF));
                        index += 5;
                    } else {
                        buffer[index + 0] = unchecked((byte)((value >> 0) & 0xFF));
                        buffer[index + 1] = unchecked((byte)((value >> 8) & 0xFF));
                        buffer[index + 2] = unchecked((byte)((value >> 16) & 0xFF));
                        buffer[index + 3] = unchecked((byte)((value >> 24) & 0xFF));
                        buffer[index + 4] = unchecked((byte)((value >> 32) & 0xFF));
                        buffer[index + 5] = unchecked((byte)((value >> 40) & 0xFF));
                        index += 6;
                    }
                }
            }
        }
        #endregion
        #region static WriteUInt32()
        /// <summary>
        ///     Write LE-encoded (little endian) uint64.
        ///     Use SignedToUnsigned() if you need to process a signed integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static void WriteUInt32(byte[] buffer, ref int index, uint value) {
            buffer[index + 0] = unchecked((byte)((value >> 0) & 0xFF));
            buffer[index + 1] = unchecked((byte)((value >> 8) & 0xFF));
            buffer[index + 2] = unchecked((byte)((value >> 16) & 0xFF));
            buffer[index + 3] = unchecked((byte)((value >> 24) & 0xFF));
            index += 4;
        }
        /// <summary>
        ///     Write LE-encoded (little endian) uint64.
        ///     Use SignedToUnsigned() if you need to process a signed integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static void WriteUInt32(byte[] buffer, ref int index, uint value, int byte_count) {
            // 20% speedup for using "ref int index" instead of returning the new index

            // if you crash here, the caller needs to check for "byte_count == 0" to avoid making this call
            Debug.Assert(byte_count >= 1 && byte_count <= 4);

            if(byte_count <= 2) {
                if(byte_count == 1) {
                    buffer[index++] = unchecked((byte)((value >> 0) & 0xFF));
                } else {
                    buffer[index + 0] = unchecked((byte)((value >> 0) & 0xFF));
                    buffer[index + 1] = unchecked((byte)((value >> 8) & 0xFF));
                    index += 2;
                }
            } else {
                if(byte_count == 3) {
                    buffer[index + 0] = unchecked((byte)((value >> 0) & 0xFF));
                    buffer[index + 1] = unchecked((byte)((value >> 8) & 0xFF));
                    buffer[index + 2] = unchecked((byte)((value >> 16) & 0xFF));
                    index += 3;
                } else {
                    buffer[index + 0] = unchecked((byte)((value >> 0) & 0xFF));
                    buffer[index + 1] = unchecked((byte)((value >> 8) & 0xFF));
                    buffer[index + 2] = unchecked((byte)((value >> 16) & 0xFF));
                    buffer[index + 3] = unchecked((byte)((value >> 24) & 0xFF));
                    index += 4;
                }
            }
        }
        #endregion

        #region static EncodeByteArray()
        /// <summary>
        ///     Encodes a byte[] unto buffer, and if it overflows, on the destStream.
        ///     This will properly encode nulls and empty arrays.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static void EncodeByteArray(byte[] buffer, ref int offset, Stream destStream, byte[] value) {
            Debug.Assert(offset < buffer.Length);

            if(value == null)
                WriteVarUInt64(buffer, ref offset, 0);
            else if(value.Length == 0)
                WriteVarUInt64(buffer, ref offset, 1);
            else {
                int index = 0;
                int count = value.Length;
                var encoded_size = unchecked((ulong)count + 1);

                WriteVarUInt64(buffer, ref offset, destStream, encoded_size);
                // at this point buffer can't be full

                var copyable = Math.Min(buffer.Length - offset, count);
                if(copyable > 0) {
                    Buffer.BlockCopy(value, index, buffer, offset, copyable);

                    index += copyable;
                    count -= copyable;
                    offset += copyable;
                }

                if(offset == buffer.Length) {
                    destStream.Write(buffer, 0, offset);
                    offset = 0;

                    if(count > 0)
                        destStream.Write(value, index, count);
                }
                return;
            }

            if(offset == buffer.Length) {
                destStream.Write(buffer, 0, offset);
                offset = 0;
            }
        }
        /// <summary>
        ///     Encodes a byte[] unto buffer, and if it overflows, on the destStream.
        ///     This will properly encode nulls and empty arrays.
        ///     This overload will store the length on a separate stream, which is far more efficient processing-wise.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static void EncodeByteArray(Stream destDataStream, byte[] value, Action<ulong> storeEncodedLength) {
            if(value == null)
                storeEncodedLength(0);
            else if(value.Length == 0)
                storeEncodedLength(1);
            else {
                storeEncodedLength(unchecked((ulong)value.Length + 1));

                // because of stream.read(), you can't specify an offset > int.MaxValue, so we limit ourselves to 32 bits
                destDataStream.Write(value, 0, value.Length);
            }
        }
        #endregion
        #region static EncodeStream()
        /// <summary>
        ///     Encodes a Stream unto buffer, and if it overflows, on the destStream.
        ///     This will properly encode nulls and empty streams.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static void EncodeStream(byte[] buffer, ref int offset, Stream destStream, Stream value) {
            Debug.Assert(offset < buffer.Length);

            if(value == null)
                WriteVarUInt64(buffer, ref offset, 0);
            else if(value.Length == 0)
                WriteVarUInt64(buffer, ref offset, 1);
            else {
                long count = value.Length - value.Position;
                var encoded_size = unchecked((ulong)count + 1);

                WriteVarUInt64(buffer, ref offset, destStream, encoded_size);
                // at this point buffer can't be full

                var copyable = unchecked((int)Math.Min(buffer.Length - offset, count));
                if(copyable > 0) {
                    int read = value.Read(buffer, offset, copyable);

                    count -= read;
                    offset += read;
                }

                if(offset == buffer.Length) {
                    destStream.Write(buffer, 0, offset);
                    offset = 0;

                    if(count > 0) {
                        value.CopyTo(destStream);
                        //if(value.Position != original_value_length)
                        //    throw new InvalidOperationException("The stream to encode changed in size during encoding.");
                    }
                }
                return;
            }

            if(offset == buffer.Length) {
                destStream.Write(buffer, 0, offset);
                offset = 0;
            }
        }
        /// <summary>
        ///     Encodes a Stream unto buffer, and if it overflows, on the destStream.
        ///     This will properly encode nulls and empty streams.
        ///     This overload will store the length on a separate stream, which is far more efficient processing-wise.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static void EncodeStream(Stream destDataStream, Stream value, Action<ulong> storeEncodedLength) {
            if(value == null)
                storeEncodedLength(0);
            else {
                if(value.CanSeek) {
                    if(value.Length == 0)
                        storeEncodedLength(1);
                    else {
                        storeEncodedLength(unchecked((ulong)value.Length + 1));
                        StreamCopyTo(value, destDataStream);
                    }
                } else {
                    // avoid reading value.Length in case stream does not support it
                    var encodedLength = StreamCopyToWithLength(value, destDataStream);
                    storeEncodedLength(encodedLength + 1);
                }
            }
        }
        #endregion
        #region static EncodeString()
        private const int CHAR_BUFFER_SIZE         = 4096;
        private const int MIN_COMMIT_BUFFER_SIZE   = 4096;
        public  const int ENCODESTRING_BUFFER_SIZE = 32768;

        /// <summary>
        ///     Encodes an arbitrarily long string unto buffer, and if it overflows, on the destStream.
        ///     This will properly encode nulls and empty strings.
        /// </summary>
        /// <param name="buffer">Must be ENCODESTRING_BUFFER_SIZE sized. For performance reasons.</param>
        [MethodImpl(AggressiveInlining)]
        public static void EncodeString(byte[] buffer, ref int offset, Stream destStream, Encoding encoder, string value) {
            // must be able to fit CHAR_BUFFER_SIZE + MIN_COMMIT_BUFFER_SIZE character encodings
            Debug.Assert(ENCODESTRING_BUFFER_SIZE >= encoder.GetMaxByteCount(CHAR_BUFFER_SIZE) + MIN_COMMIT_BUFFER_SIZE); // 12291
            Debug.Assert(buffer.Length == ENCODESTRING_BUFFER_SIZE);
            Debug.Assert(offset < ENCODESTRING_BUFFER_SIZE);

            if(value == null)
                WriteVarUInt64(buffer, ref offset, 0);
            else if(value.Length == 0)
                WriteVarUInt64(buffer, ref offset, 1);
            else {
                // need to write size ahead of time, so short of storing the encoded string in memory, 
                // there simply aren't any great alternatives
                // this used to write lengths on a separate stream which fixed this issue
                // this was changed in order to standardize all data encodings into a single stream (forcing this)
                int count = encoder.GetByteCount(value);
                var encoded_size = unchecked((ulong)count + 1);

                WriteVarUInt64(buffer, ref offset, destStream, encoded_size);
                
                // fully fits in buffer
                if(ENCODESTRING_BUFFER_SIZE - offset >= count) {
                    var writtenBytes = encoder.GetBytes(value, 0, value.Length, buffer, offset);
                    offset += writtenBytes;

                    if(offset == ENCODESTRING_BUFFER_SIZE) {
                        destStream.Write(buffer, 0, ENCODESTRING_BUFFER_SIZE);
                        offset = 0;
                    }
                    return;
                }

                // we have to empty the buffer to make sure we can fit CHAR_BUFFER_SIZE UTF-8 characters in the empty buffer
                if(offset > 0) {
                    destStream.Write(buffer, 0, offset);
                    offset = 0;
                }

                int charIndex = 0;
                int remaining = value.Length;

                while(remaining > 0) {
                    int encodedChars = Math.Min(remaining, CHAR_BUFFER_SIZE);

                    var writtenBytes = encoder.GetBytes(value, charIndex, encodedChars, buffer, offset);

                    offset += writtenBytes;
                    remaining -= encodedChars;
                    charIndex += encodedChars;

                    if(offset >= MIN_COMMIT_BUFFER_SIZE) {
                        destStream.Write(buffer, 0, offset);
                        offset = 0;
                    }
                }
            }

            if(offset == ENCODESTRING_BUFFER_SIZE) {
                destStream.Write(buffer, 0, ENCODESTRING_BUFFER_SIZE);
                offset = 0;
            }
        }
        /// <summary>
        ///     Encodes an arbitrarily long string unto buffer, and if it overflows, on the destStream.
        ///     This will properly encode nulls and empty strings.
        ///     This overload will store the length on a separate stream, which is far more efficient processing-wise.
        /// </summary>
        /// <param name="buffer">Must be ENCODESTRING_BUFFER_SIZE sized. For performance reasons.</param>
        [MethodImpl(AggressiveInlining)]
        public static void EncodeString(byte[] buffer, ref int offset, Stream destDataStream, Encoding encoder, string value, Action<ulong> storeEncodedLength) {
            // must be able to fit CHAR_BUFFER_SIZE + MIN_COMMIT_BUFFER_SIZE character encodings
            Debug.Assert(ENCODESTRING_BUFFER_SIZE >= encoder.GetMaxByteCount(CHAR_BUFFER_SIZE) + MIN_COMMIT_BUFFER_SIZE); // 12291
            Debug.Assert(buffer.Length == ENCODESTRING_BUFFER_SIZE);
            Debug.Assert(offset < ENCODESTRING_BUFFER_SIZE);

            ulong encodedLength = 0;

            if(value != null) {
                int charIndex = 0;
                int remaining = value.Length;

                while(remaining > 0) {
                    int encodedChars = Math.Min(remaining, CHAR_BUFFER_SIZE);

                    var writtenBytes = encoder.GetBytes(value, charIndex, encodedChars, buffer, offset);

                    offset += writtenBytes;
                    remaining -= encodedChars;
                    charIndex += encodedChars;
                    encodedLength += unchecked((ulong)writtenBytes);

                    if(offset >= MIN_COMMIT_BUFFER_SIZE) {
                        destDataStream.Write(buffer, 0, offset);
                        offset = 0;
                    }
                }

                encodedLength++; // string.Empty stored as '1' length
            }

            storeEncodedLength(encodedLength);
        }
        #endregion
        #region static DecodeByteArray()
        /// <summary>
        ///     Decodes a byte[].
        ///     This will properly decode nulls and empty arrays.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static byte[] DecodeByteArray(byte[] buffer, ref int offset, ref int read, Stream sourceStream) {
            var encodedLength = unchecked((int)ReadVarUInt64_ThrowOnEOS(buffer, ref offset, ref read, sourceStream));

            if(encodedLength == 0)
                return null;
            else if(encodedLength == 1)
                return EMPTY_ARRAY;
            else {
                encodedLength--;

                var res = new byte[encodedLength];

                var copyable = Math.Max(Math.Min(read - offset, encodedLength), 0);
                if(copyable > 0) {
                    Buffer.BlockCopy(buffer, offset, res, 0, copyable);
                    offset += copyable;
                }

                var remaining = encodedLength - copyable;
                if(remaining > 0) {
                    sourceStream.Read(res, copyable, remaining);
                    // implicitly: read == offset
                }
                return res;
            }
        }
        private static readonly byte[] EMPTY_ARRAY = new byte[0];
        /// <summary>
        ///     Decodes a byte[].
        ///     This will properly decode nulls and empty arrays.
        ///     This overload will store the length on a separate stream, which is far more efficient processing-wise.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static byte[] DecodeByteArray(Stream sourceDataStream, Func<ulong> readEncodedLength) {
            var encodedLength = unchecked((int)readEncodedLength());

            if(encodedLength == 0)
                return null;
            else if(encodedLength == 1)
                return EMPTY_ARRAY;
            else {
                encodedLength--;

                var res = new byte[encodedLength];
                int read = sourceDataStream.Read(res, 0, encodedLength);
                if(read < encodedLength)
                    throw new EndOfStreamException($"Unable to read/decode the byte[].");
                return res;
            }
        }
        #endregion
        #region static DecodeStream()
        private static readonly Stream EMPTY_STREAM = new MemoryStream(new byte[0], 0, 0, false);

        /// <summary>
        ///     Decodes a stream.
        ///     This will properly decode nulls and empty streams.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static Stream DecodeStream(byte[] buffer, ref int offset, ref int read, Stream sourceStream) {
            var encodedLength = unchecked((int)ReadVarUInt64_ThrowOnEOS(buffer, ref offset, ref read, sourceStream));

            if(encodedLength == 0)
                return null;
            else if(encodedLength == 1)
                return EMPTY_STREAM;
            else {
                encodedLength--;

                var res = new DynamicMemoryStream(encodedLength);

                var copyable = Math.Min(Math.Max(read - offset, 0), encodedLength);
                if(copyable > 0) {
                    res.Write(buffer, offset, copyable);
                    offset += copyable;
                }

                var remaining = encodedLength;
                while(remaining > 0) {
                    var request = Math.Min(buffer.Length, remaining);
                    copyable = sourceStream.Read(buffer, 0, request);
                    if(copyable == 0)
                        throw new EndOfStreamException($"Unable to read/decode the (sub-)stream.");
                    res.Write(buffer, 0, copyable);
                    remaining -= copyable;
                }
                res.Position = 0;
                
                return res;
            }
        }
        /// <summary>
        ///     Decodes a stream.
        ///     This will properly decode nulls and empty streams.
        ///     This overload will store the length on a separate stream, which is far more efficient processing-wise.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static Stream DecodeStream(Stream sourceDataStream, ref long dataStreamPosition, Func<ulong> readEncodedLength) {
            var encodedLength = unchecked((long)readEncodedLength());

            if(encodedLength == 0)
                return null;
            else if(encodedLength == 1)
                return EMPTY_STREAM;
            else {
                encodedLength--;

                Stream res;
                if(sourceDataStream.CanSeek)
                    res = new SegmentedStream(sourceDataStream, dataStreamPosition, encodedLength);
                else {
                    res = new DynamicMemoryStream(unchecked((int)encodedLength));
                    sourceDataStream.CopyTo(res);
                    if(res.Length < encodedLength)
                        throw new EndOfStreamException($"Unable to read/decode the (sub-)stream.");
                    res.Position = 0;
                }

                dataStreamPosition += encodedLength;

                return res;
            }
        }
        #endregion
        #region static DecodeString()
        /// <summary>
        ///     Decodes a string.
        ///     This will properly decode nulls and empty string.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static string DecodeString(Stream sourceDataStream, byte[] buffer, char[] decompressBuffer, ref int bufferIndex, ref int readCount, Decoder decoder) {
            var encodedLength = unchecked((int)ReadVarUInt64_ThrowOnEOS(buffer, ref bufferIndex, ref readCount, sourceDataStream));
            return InternalDecodeString(sourceDataStream, buffer, decompressBuffer, ref bufferIndex, ref readCount, decoder, encodedLength);
        }
        /// <summary>
        ///     Decodes a string.
        ///     This will properly decode nulls and empty string.
        ///     This overload will store the length on a separate stream, which is far more efficient processing-wise.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static string DecodeString(Stream sourceDataStream, byte[] buffer, char[] decompressBuffer, ref int bufferIndex, ref int readCount, Decoder decoder, Func<ulong> readEncodedLength) {
            var encodedLength = unchecked((int)readEncodedLength());
            return InternalDecodeString(sourceDataStream, buffer, decompressBuffer, ref bufferIndex, ref readCount, decoder, encodedLength);
        }
        /// <summary>
        ///     Decodes a string.
        ///     This will properly decode nulls and empty string.
        ///     This overload will store the length on a separate stream, which is far more efficient processing-wise.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        private static string InternalDecodeString(Stream sourceDataStream, byte[] buffer, char[] decompressBuffer, ref int bufferIndex, ref int readCount, Decoder decoder, int encodedLength) {
            if(encodedLength == 0)
                return null;
            else if(encodedLength == 1)
                return string.Empty;
            else {
                encodedLength--;

                // we dont encode the string length, but its encoded length, which is a worst-case scenario (max possible string length)
                // the string builder is likely to be smaller in length when the UTF-8 is decoded
                var sb = new StringBuilder(encodedLength);

                while(encodedLength != 0) {
                    if(bufferIndex == readCount) {
                        readCount = sourceDataStream.Read(buffer, 0, buffer.Length);
                        if(readCount == 0)
                            throw new EndOfStreamException($"Unable to read/decode the string.");
                        bufferIndex = 0;
                    }

                    int request = Math.Min(encodedLength, readCount - bufferIndex);
                    int readChars = decoder.GetChars(buffer, bufferIndex, request, decompressBuffer, 0);

                    encodedLength -= request;
                    bufferIndex += request;

                    if(readChars > 0)
                        sb.Append(decompressBuffer, 0, readChars);
                }

                return sb.ToString();
            }
        }
        #endregion
        #region static SkipVarSizedObject()
        /// <summary>
        ///     Skips reading a var-sized encoded object.
        ///     e.g.: EncodeString()/EncodeByteArray()/EncodeStream().
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static void SkipVarSizedObject(byte[] buffer, ref int offset, ref int read, Stream sourceStream) {
            var encodedLength = ReadVarUInt64_ThrowOnEOS(buffer, ref offset, ref read, sourceStream);

            if(encodedLength <= 1)
                return;
            
            encodedLength--;

            var remaining = unchecked((ulong)Math.Max(read - offset, 0));
            if(encodedLength < remaining)
                offset += unchecked((int)encodedLength);
            else {
                sourceStream.Seek(unchecked((long)(encodedLength - remaining)), SeekOrigin.Current);
                // mark the buffer as fully read
                offset = read;
            }
        }
        /// <summary>
        ///     Skips reading a var-sized encoded object.
        ///     e.g.: EncodeString()/EncodeByteArray()/EncodeStream().
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static void SkipVarSizedObject(Stream sourceDataStream, ref long dataStreamPosition, Func<ulong> readEncodedLength) {
            var encodedLength = unchecked((long)readEncodedLength());

            if(encodedLength <= 1)
                return;
            
            sourceDataStream.Seek(encodedLength - 1, SeekOrigin.Current);
        }
        #endregion

        #region static ConvertToBits()
        [MethodImpl(AggressiveInlining)]
        public static ulong ConvertToBits(double value) {
            //return new UnionDouble() { Value = value }.Binary;
            return unchecked((ulong)BitConverter.DoubleToInt64Bits(value));
            //unsafe{ return *(ulong*)(&value); }
        }
        //[SecuritySafeCritical]
        [MethodImpl(AggressiveInlining)]
        public static uint ConvertToBits(float value) {
#if NON_PORTABLE_CODE
            Debug.Assert(BitConverter.IsLittleEndian);
            return new UnionFloat() { Value = value }.Binary;
            //unsafe{ return *(uint*)(&value); }
#else
            throw new NotImplementedException();
#endif
        }
        [MethodImpl(AggressiveInlining)]
        public static void ConvertToBits(decimal value, byte[] buffer, ref int index) {
            var bits = decimal.GetBits(value);

            // technically could be compressed since theres some unused ranges
            // int[3] bits [30-24] and [0-15] are always zero

            int bit = bits[0];
            buffer[index++] = unchecked((byte)((bit >> 0) & 0xFF));
            buffer[index++] = unchecked((byte)((bit >> 8) & 0xFF));
            buffer[index++] = unchecked((byte)((bit >> 16) & 0xFF));
            buffer[index++] = unchecked((byte)((bit >> 24) & 0xFF));
            bit = bits[1];
            buffer[index++] = unchecked((byte)((bit >> 0) & 0xFF));
            buffer[index++] = unchecked((byte)((bit >> 8) & 0xFF));
            buffer[index++] = unchecked((byte)((bit >> 16) & 0xFF));
            buffer[index++] = unchecked((byte)((bit >> 24) & 0xFF));
            bit = bits[2];
            buffer[index++] = unchecked((byte)((bit >> 0) & 0xFF));
            buffer[index++] = unchecked((byte)((bit >> 8) & 0xFF));
            buffer[index++] = unchecked((byte)((bit >> 16) & 0xFF));
            buffer[index++] = unchecked((byte)((bit >> 24) & 0xFF));
            bit = bits[3];
            buffer[index++] = unchecked((byte)((bit >> 0) & 0xFF));
            buffer[index++] = unchecked((byte)((bit >> 8) & 0xFF));
            buffer[index++] = unchecked((byte)((bit >> 16) & 0xFF));
            buffer[index++] = unchecked((byte)((bit >> 24) & 0xFF));
        }
        [StructLayout(LayoutKind.Explicit)]
        private struct UnionFloat {
            [FieldOffset(0)] public float Value; // only works with BitConverter.IsLittleEndian
            [FieldOffset(0)] public uint Binary;
        }
        #endregion
        #region static ConvertToDouble()
        [MethodImpl(AggressiveInlining)]
        public static double ConvertToDouble(ulong value) {
            //return new Union() { Binary = value }.Value;
            //Unsafe.As<double>(value);
            return BitConverter.Int64BitsToDouble(unchecked((long)value));
        }
        #endregion
        #region static ConvertToFloat()
        //[SecuritySafeCritical]
        [MethodImpl(AggressiveInlining)]
        public static float ConvertToFloat(uint value) {
#if NON_PORTABLE_CODE
            Debug.Assert(BitConverter.IsLittleEndian);
            return new UnionFloat() { Binary = value }.Value;
            //unsafe{ return *(float*)(&value); }
#else
            throw new NotImplementedException();
#endif
        }
        #endregion
        #region static ConvertToDecimal()
        [MethodImpl(AggressiveInlining)]
        public static decimal ConvertToDecimal(byte[] buffer, ref int index) {
            var bits = new int[4];

            bits[0] =
                (buffer[index + 0] << 0) |
                (buffer[index + 1] << 8) |
                (buffer[index + 2] << 16) |
                (buffer[index + 3] << 24);
            bits[1] =
                (buffer[index + 4] << 0) |
                (buffer[index + 5] << 8) |
                (buffer[index + 6] << 16) |
                (buffer[index + 7] << 24);
            bits[2] =
                (buffer[index + 8] << 0) |
                (buffer[index + 9] << 8) |
                (buffer[index + 10] << 16) |
                (buffer[index + 11] << 24);
            bits[3] =
                (buffer[index + 12] << 0) |
                (buffer[index + 13] << 8) |
                (buffer[index + 14] << 16) |
                (buffer[index + 15] << 24);

            index += 16;

            return new decimal(bits);
        }
        #endregion

        #region static HexEncode()
        /// <summary>
        ///     Writes the bytes in hexadecimal format, with no prepending of any kind (0x) and in uppercase.
        /// </summary>
        //[MethodImpl(AggressiveInlining)] // most likely a slowdown if called on many places, due to worse branch prediction if enabled
        public static void HexEncode(byte[] destBuffer, ref int destOffset, Stream destStream, byte[] sourceBuffer, int sourceOffset, int count) {
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
        /// <summary>
        ///     Writes the bytes in hexadecimal format, with no prepending of any kind (0x) and in uppercase.
        /// </summary>
        //[MethodImpl(AggressiveInlining)] // most likely a slowdown if called on many places, due to worse branch prediction if enabled
        public static void HexEncode(StringBuilder sb, byte[] buffer, int offset, int count) {
            const int WRITE_BUFFER_SIZE = 4096 / sizeof(char);
            
            int writeIndex = 0;
            char[] writeBuffer = new char[Math.Min(count * 2, WRITE_BUFFER_SIZE)];

            while(count-- > 0) {
                int c = buffer[offset++];
                
                int low  = c & 0x0F;
                int high = c >> 4;

                writeBuffer[writeIndex + 0] = high < 10 ? unchecked((char)('0' + high)) : unchecked((char)('A' + high - 10));
                writeBuffer[writeIndex + 1] = low < 10  ? unchecked((char)('0' + low))  : unchecked((char)('A' + low - 10));
                writeIndex += 2;

                if(writeIndex == WRITE_BUFFER_SIZE) {
                    writeIndex = 0;
                    sb.Append(writeBuffer, 0, WRITE_BUFFER_SIZE);
                }
            }
            if(writeIndex > 0)
                sb.Append(writeBuffer, 0, writeIndex);
        }
        /// <summary>
        ///     Writes the bytes in hexadecimal format, with no prepending of any kind (0x) and in uppercase.
        /// </summary>
        //[MethodImpl(AggressiveInlining)] // most likely a slowdown if called on many places, due to worse branch prediction if enabled
        public static void HexEncode(StringBuilder sb, Stream source) {
            const int READ_BUFFER_SIZE  = 4096 / sizeof(byte);
            const int WRITE_BUFFER_SIZE = 4096 / sizeof(char);
            
            int count = unchecked((int)(source.Length - source.Position));
            int writeIndex = 0;
            char[] writeBuffer = new char[Math.Min(count * 2, WRITE_BUFFER_SIZE)];

            byte[] readBuffer = new byte[READ_BUFFER_SIZE];
            int readOffset = 0;
            int read = 0;

            while(true) {
                if(readOffset == read) {
                    readOffset = 0;
                    read = source.Read(readBuffer, 0, READ_BUFFER_SIZE);
                    if(read == 0)
                        break;
                }

                int c = readBuffer[readOffset++];
                
                int low  = c & 0x0F;
                int high = c >> 4;

                writeBuffer[writeIndex + 0] = high < 10 ? unchecked((char)('0' + high)) : unchecked((char)('A' + high - 10));
                writeBuffer[writeIndex + 1] = low < 10  ? unchecked((char)('0' + low))  : unchecked((char)('A' + low - 10));
                writeIndex += 2;

                if(writeIndex == WRITE_BUFFER_SIZE) {
                    writeIndex = 0;
                    sb.Append(writeBuffer, 0, WRITE_BUFFER_SIZE);
                }
            }
            if(writeIndex > 0)
                sb.Append(writeBuffer, 0, writeIndex);
        }
        #endregion
        #region static HexDecode()
        /// <summary>
        ///     Reads the bytes in hexadecimal format, assuming no prepending of any kind (0x).
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static void HexDecode(string source, int sourceOffset, int sourceCount, byte[] target, ref int offset) {
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
        /// <summary>
        ///     Reads the bytes in hexadecimal format, assuming no prepending of any kind (0x).
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static void HexDecode(string source, int sourceOffset, int sourceCount, byte[] buffer, ref int bufferOffset, Stream destination) {
            if(sourceCount <= 0)
                return;
            if(sourceCount % 2 != 0)
                throw new ArgumentException("Not a multiple of 2.", nameof(sourceCount));

            while(sourceCount > 0) {
                buffer[bufferOffset++] = unchecked((byte)((HexDecode(source[sourceOffset + 0]) << 4) & HexDecode(source[sourceOffset + 1])));
                sourceOffset += 2;
                if(bufferOffset == buffer.Length) {
                    destination.Write(buffer, 0, bufferOffset);
                    bufferOffset = 0;
                }
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

        #region static CountDigits10()
        /// <summary>
        ///     Returns the number of characters needed to represent the number.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int CountDigits10(byte value) {
            if(value < 10)  return 1;
            if(value < 100) return 2;
            return 3;
        }
        /// <summary>
        ///     Returns the number of characters needed to represent the number.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int CountDigits10(ushort value) {
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
        public static int CountDigits10(uint value) {
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
        public static int CountDigits10(ulong value) {
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
        /// <summary>
        ///     Returns the number of characters needed to represent the number.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int CountDigits10(sbyte value) {
            int count;
            if(value >= 0)
                count = 0;
            else {
                count = 1;
                value = unchecked((sbyte)-value);
            }
            if(value < 10)  return count + 1;
            if(value < 100) return count + 2;
            return count + 3;
        }
        /// <summary>
        ///     Returns the number of characters needed to represent the number.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int CountDigits10(short value) {
            int count;
            if(value >= 0)
                count = 0;
            else {
                count = 1;
                value = unchecked((short)-value);
            }
            if(value < 10)    return count + 1;
            if(value < 100)   return count + 2;
            if(value < 1000)  return count + 3;
            if(value < 10000) return count + 4;
            return count + 5;
        }
        /// <summary>
        ///     Returns the number of characters needed to represent the number.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int CountDigits10(int value) {
            int count;
            if(value >= 0)
                count = 0;
            else {
                count = 1;
                value = -value;
            }
            if(value < 10)   return count + 1;
            if(value < 100)  return count + 2;
            if(value < 1000) return count + 3;

            if(value < 10000000) { // 4-7
                if(value < 100000)
                    return count + (value < 10000 ? 4 : 5);
                else
                    return count + (value < 1000000 ? 6 : 7);
            } else { // 8-10
                if(value < 1000000000)
                    return count + (value < 100000000 ? 8 : 9);
                else
                    return count + 10;
            }
        }
        /// <summary>
        ///     Returns the number of characters needed to represent the number.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int CountDigits10(long value) {
            int count;
            if(value >= 0)
                count = 0;
            else {
                count = 1;
                value = -value;
            }
            if(value < 10)    return count + 1;
            if(value < 100)   return count + 2;
            if(value < 1000)  return count + 3;
            if(value < 10000) return count + 4;

            if(value < 1000000000000) { // 5-12
                if(value < 100000000) { // 5-8
                    if(value < 1000000)
                        return count + (value < 100000 ? 5 : 6);
                    else
                        return count + (value < 10000000 ? 7 : 8);
                } else { // 9-12
                    if(value < 10000000000)
                        return count + (value < 1000000000 ? 9 : 10);
                    else
                        return count + (value < 100000000000 ? 11 : 12);
                }
            } else { // 13-19
                if(value < 10000000000000000) { // 13-16
                    if(value < 100000000000000)
                        return count + (value < 10000000000000 ? 13 : 14);
                    else
                        return count + (value < 1000000000000000 ? 15 : 16);
                } else { // 17-19
                    if(value < 1000000000000000000)
                        return count + (value < 100000000000000000 ? 17 : 18);
                    else
                        return count + 19;
                }
            }
        }
        #endregion
        #region static Fast_ItoA()
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
        public static void Fast_ItoA(byte[] buffer, ref int offset, byte value) {
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
        public static void Fast_ItoA(byte[] buffer, ref int offset, ushort value) {
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
        public static void Fast_ItoA(byte[] buffer, ref int offset, uint value) {
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
        public static void Fast_ItoA(byte[] buffer, ref int offset, ulong value) {
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
        public static void Fast_ItoA(byte[] buffer, ref int offset, sbyte value) {
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
        public static void Fast_ItoA(byte[] buffer, ref int offset, short value) {
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
        public static void Fast_ItoA(byte[] buffer, ref int offset, int value) {
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
        public static void Fast_ItoA(byte[] buffer, ref int offset, long value) {
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
        #region static Fast_AtoI_UInt8()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static byte Fast_AtoI_UInt8(byte[] buffer, int offset, int count) {
            byte res = 0;
            switch(count) {
                case 3: res  = unchecked((byte)((buffer[offset + count - 3] - '0') * 100)); goto case 2;
                case 2: res += unchecked((byte)((buffer[offset + count - 2] - '0') * 10));  goto case 1;
                case 1: res += unchecked((byte)((buffer[offset + count - 1] - '0') * 1));   break;
                default:
                    throw new FormatException();
            }
            return res;
        }
        #endregion
        #region static Fast_AtoI_UInt16()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static ushort Fast_AtoI_UInt16(byte[] buffer, int offset, int count) {
            ushort res = 0;
            switch(count) {
                case 5: res  = unchecked((ushort)((buffer[offset + count - 5] - '0') * 10000)); goto case 4;
                case 4: res += unchecked((ushort)((buffer[offset + count - 4] - '0') * 1000));  goto case 3;
                case 3: res += unchecked((ushort)((buffer[offset + count - 3] - '0') * 100));   goto case 2;
                case 2: res += unchecked((ushort)((buffer[offset + count - 2] - '0') * 10));    goto case 1;
                case 1: res += unchecked((ushort)((buffer[offset + count - 1] - '0') * 1));     break;
                default:
                    throw new FormatException();
            }
            return res;
        }
        #endregion
        #region static Fast_AtoI_UInt32()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static uint Fast_AtoI_UInt32(byte[] buffer, int offset, int count) {
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
        #region static Fast_AtoI_UInt64()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static ulong Fast_AtoI_UInt64(byte[] buffer, int offset, int count) {
            ulong res = 0;
            switch(count) {
                case 20: res  = unchecked((ulong)(buffer[offset + count - 20] - '0')) * 10000000000000000000; goto case 19;
                case 19: res += unchecked((ulong)(buffer[offset + count - 19] - '0')) * 1000000000000000000;  goto case 18;
                case 18: res += unchecked((ulong)(buffer[offset + count - 18] - '0')) * 100000000000000000;   goto case 17;
                case 17: res += unchecked((ulong)(buffer[offset + count - 17] - '0')) * 10000000000000000;    goto case 16;
                case 16: res += unchecked((ulong)(buffer[offset + count - 16] - '0')) * 1000000000000000;     goto case 15;
                case 15: res += unchecked((ulong)(buffer[offset + count - 15] - '0')) * 100000000000000;      goto case 14;
                case 14: res += unchecked((ulong)(buffer[offset + count - 14] - '0')) * 10000000000000;       goto case 13;
                case 13: res += unchecked((ulong)(buffer[offset + count - 13] - '0')) * 1000000000000;        goto case 12;
                case 12: res += unchecked((ulong)(buffer[offset + count - 12] - '0')) * 100000000000;         goto case 11;
                case 11: res += unchecked((ulong)(buffer[offset + count - 11] - '0')) * 10000000000;          goto case 10;
                case 10: res += unchecked((ulong)(buffer[offset + count - 10] - '0')) * 1000000000;           goto case 9;
                case 9:  res += unchecked((ulong)(buffer[offset + count -  9] - '0')) * 100000000;            goto case 8;
                case 8:  res += unchecked((ulong)(buffer[offset + count -  8] - '0')) * 10000000;             goto case 7;
                case 7:  res += unchecked((ulong)(buffer[offset + count -  7] - '0')) * 1000000;              goto case 6;
                case 6:  res += unchecked((ulong)(buffer[offset + count -  6] - '0')) * 100000;               goto case 5;
                case 5:  res += unchecked((ulong)(buffer[offset + count -  5] - '0')) * 10000;                goto case 4;
                case 4:  res += unchecked((ulong)(buffer[offset + count -  4] - '0')) * 1000;                 goto case 3;
                case 3:  res += unchecked((ulong)(buffer[offset + count -  3] - '0')) * 100;                  goto case 2;
                case 2:  res += unchecked((ulong)(buffer[offset + count -  2] - '0')) * 10;                   goto case 1;
                case 1:  res += unchecked((ulong)(buffer[offset + count -  1] - '0')) * 1;                    break;
                default:
                    throw new FormatException();
            }
            return res;
        }
        #endregion
        #region static Fast_AtoI_Int8()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static sbyte Fast_AtoI_Int8(byte[] buffer, int offset, int count) {
            bool is_negative = false;
            if(buffer[offset] == '-') {
                is_negative = true;
                offset++;
                count--;
            }
            sbyte res = 0;
            switch(count) {
                case 3: res  = unchecked((sbyte)((buffer[offset + count - 3] - '0') * 100)); goto case 2;
                case 2: res += unchecked((sbyte)((buffer[offset + count - 2] - '0') * 10));  goto case 1;
                case 1: res += unchecked((sbyte)((buffer[offset + count - 1] - '0') * 1));   break;
                default:
                    throw new FormatException();
            }
            return !is_negative ? res : unchecked((sbyte)-res);
        }
        #endregion
        #region static Fast_AtoI_Int16()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static short Fast_AtoI_Int16(byte[] buffer, int offset, int count) {
            bool is_negative = false;
            if(buffer[offset] == '-') {
                is_negative = true;
                offset++;
                count--;
            }
            short res = 0;
            switch(count) {
                case 5: res  = unchecked((short)((buffer[offset + count - 5] - '0') * 10000)); goto case 4;
                case 4: res += unchecked((short)((buffer[offset + count - 4] - '0') * 1000));  goto case 3;
                case 3: res += unchecked((short)((buffer[offset + count - 3] - '0') * 100));   goto case 2;
                case 2: res += unchecked((short)((buffer[offset + count - 2] - '0') * 10));    goto case 1;
                case 1: res += unchecked((short)((buffer[offset + count - 1] - '0') * 1));     break;
                default:
                    throw new FormatException();
            }
            return !is_negative ? res : unchecked((short)-res);
        }
        #endregion
        #region static Fast_AtoI_Int32()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int Fast_AtoI_Int32(byte[] buffer, int offset, int count) {
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
        #region static Fast_AtoI_Int64()
        /// <summary>
        ///     Fast ascii-to-integer.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static long Fast_AtoI_Int64(byte[] buffer, int offset, int count) {
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

        #region static HumanizeByteSize()
        [MethodImpl(AggressiveInlining)]
        public static string HumanizeByteSize(long size) {
            if(size < 1024)
                return size.ToString("0 B", CultureInfo.InvariantCulture);                                // Byte
            if(size < 1048576)
                return (size / 1024d).ToString("0.0 KB", CultureInfo.InvariantCulture);                   // KiloByte
            if(size < 1073741824)
                return (size / 1048576d).ToString("0.0 MB", CultureInfo.InvariantCulture);                // MegaByte
            if(size < 1099511627776)
                return (size / 1073741824d).ToString("0.0 GB", CultureInfo.InvariantCulture);             // GigaByte
            if(size < 1125899906842624)
                return (size / 1099511627776d).ToString("0.0 TB", CultureInfo.InvariantCulture);          // TerraByte

            return (size / 1125899906842624d).ToString("0.0 PB", CultureInfo.InvariantCulture);           // PetaByte

            //if(size < 1152921504606846976)
            //    return (size / 1125899906842624d).ToString("0.0 PB", CultureInfo.InvariantCulture);       // PetaByte
            //if(size < 1180591620717411303424)
            //    return (size / 1152921504606846976d).ToString("0.0 EB", CultureInfo.InvariantCulture);    // ExaByte
            //if(size < 1208925819614629174706176)
            //    return (size / 1180591620717411303424d).ToString("0.0 ZB", CultureInfo.InvariantCulture); // ZettaByte
            //return (size / 1208925819614629174706176d).ToString("0.0 YB", CultureInfo.InvariantCulture);  // YottaByte
        }
        #endregion
        #region static HumanizeQuantity()
        [MethodImpl(AggressiveInlining)]
        public static string HumanizeQuantity(long qty) {
            if(qty < 1000)
                return qty.ToString(CultureInfo.InvariantCulture);
            if(qty < 1000000)
                return (qty / 1000d).ToString("0.0 K", CultureInfo.InvariantCulture);                   // Kilo
            if(qty < 1000000000)
                return (qty / 1000000d).ToString("0.0 M", CultureInfo.InvariantCulture);                // Mega
            if(qty < 1000000000000)
                return (qty / 1000000000d).ToString("0.0 G", CultureInfo.InvariantCulture);             // Giga
            if(qty < 1000000000000000)
                return (qty / 1000000000000d).ToString("0.0 T", CultureInfo.InvariantCulture);          // Terra

            return (qty / 1000000000000000d).ToString("0.0 P", CultureInfo.InvariantCulture);           // Peta

            //if(qty < 1000000000000000000)
            //    return (qty / 1000000000000000d).ToString("0.0 P", CultureInfo.InvariantCulture);       // Peta
            //if(qty < 1000000000000000000000)
            //    return (qty / 1000000000000000000d).ToString("0.0 E", CultureInfo.InvariantCulture);    // Exa
            //if(qty < 1000000000000000000000000)
            //    return (qty / 1000000000000000000000d).ToString("0.0 Z", CultureInfo.InvariantCulture); // Zetta
            //return (qty / 1000000000000000000000000d).ToString("0.0 Y", CultureInfo.InvariantCulture);  // Yotta
        }
        #endregion

        #region static BlockCopy()
        /// <summary>
        ///     Copies a specified number of bytes from a source array starting at a particular
        ///     offset to a destination array starting at a particular offset.
        ///     [This method is about 3x faster than Buffer.BlockCopy().]
        /// </summary>
        /// <param name="src">The source buffer.</param>
        /// <param name="srcOffset">The zero-based byte offset into src.</param>
        /// <param name="dst">The destination buffer.</param>
        /// <param name="dstOffset">The zero-based byte offset into dst.</param>
        /// <param name="count">The number of bytes to copy.</param>
        /// <exception cref="System.ArgumentNullException">src or dst is null.</exception>
        /// <exception cref="System.ArgumentException">src or dst is not an array of primitives.-or- The number of bytes in src is less
        ///     than srcOffset plus count.-or- The number of bytes in dst is less than dstOffset
        ///     plus count.</exception>
        /// <exception cref="System.ArgumentOutOfRangeException">srcOffset, dstOffset, or count is less than 0.</exception>
        [MethodImpl(AggressiveInlining)]
        [SecuritySafeCritical]
        public static void BlockCopy(byte[] src, int srcOffset, byte[] dst, int dstOffset, int count) {
#if USING_SYSTEM_RUNTIME_COMPILERSERVICES_UNSAFE_NUGET_PACKAGE
            // this is about 3x faster than Buffer.BlockCopy()
            // it is very slightly slower than using Buffer.MemoryCopy(), but does not require 'unsafe' compile flag
            Unsafe.CopyBlock(ref dst[dstOffset], ref src[srcOffset], unchecked((uint)count));
#else
#if UNSAFE
            fixed(void* s = &src[srcOffset])
            fixed(void* d = &dst[dstOffset]) {
                Buffer.MemoryCopy(s, d, count, count);
            }
#else
            Buffer.BlockCopy(src, srcOffset, dst, dstOffset, count);
#endif
#endif
        }
        #endregion
        #region static ByteArrayCompare()
        [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
        private static extern int memcmp(byte[] array1, byte[] array2, long count);

        /// <summary>
        ///     Compares bytes in the arrays and returns the first higher/lower result.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int ByteArrayCompare(byte[] array1, byte[] array2) {
            var min = Math.Min(array1.Length, array2.Length);
            var diff = memcmp(array1, array2, min);

            if(diff != 0)
                return diff;

            return array1.Length.CompareTo(array2.Length);
        }
        /// <summary>
        ///     Compares bytes in the arrays and returns the first higher/lower result.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int ByteArrayCompare(byte[] array1, byte[] array2, long count) {
            return memcmp(array1, array2, count);
        }
        #endregion
        #region static ByteArrayCompareIndex()
        /// <summary>
        ///     Returns the index where the data differs.
        ///     Returns -1 if equal.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int ByteArrayCompareIndex(ReadOnlySpan<byte> span1, ReadOnlySpan<byte> span2) {
            if(span1.Length != span2.Length)
                throw new ArgumentException();

            // this code is actually portable
            var longspan1 = span1.NonPortableCast<byte, long>();
            var longspan2 = span2.NonPortableCast<byte, long>();

            // detect rough location at 8-byte granularity
            var max = longspan1.Length;
            for(int i = 0; i < max; i++) {
                if(longspan1[i] != longspan2[i]) {
                    int pos = i * sizeof(long);
                    for(i = 0; i < 7; i++) {
                        if(span1[pos] != span2[pos])
                            return pos;
                        pos++;
                    }
                    return pos; // 8th byte
                }
            }

            // check for diff in 7- remaining bytes
            int count = span1.Length;
            var remaining = count % sizeof(long);
            int pos2 = count - remaining;
            while(remaining-- > 0) {
                if(span1[pos2] != span2[pos2])
                    return pos2;
                pos2++;
            }

            return -1;
        }
        /// <summary>
        ///     Returns the index where the data differs.
        ///     Returns -1 if equal.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int ByteArrayCompareIndex(byte[] array1, byte[] array2, int count) {
            return ByteArrayCompareIndex(
                new ReadOnlySpan<byte>(array1, 0, count), 
                new ReadOnlySpan<byte>(array2, 0, count));
        }
        #endregion

        #region static StreamCopyTo()
        [MethodImpl(AggressiveInlining)]
        public static void StreamCopyTo(Stream source, Stream destination) {
            source.Flush();

            source.Position = 0;
            source.CopyTo(destination);
        }
        #endregion
        #region static StreamCopyToWithLength()
        [MethodImpl(AggressiveInlining)]
        public static ulong StreamCopyToWithLength(Stream source, Stream destination) {
            // largest multiple of 4096 that is still smaller than the large object heap threshold (85K)
            // The buffer is short-lived and is likely to be collected at Gen0, and it offers a significant improvement in Copy performance
            const int defaultCopyBufferSize = 81920;
            var buffer = new byte[defaultCopyBufferSize];

            int read;
            long total = 0;
            while((read = source.Read(buffer, 0, defaultCopyBufferSize)) != 0) {
                destination.Write(buffer, 0, read);
                total += read;
            }

            return unchecked((ulong)total);
        }
        #endregion
        #region static StreamCompare()
        /// <summary>
        ///     Compares bytes in the stream and returns the first higher/lower result.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static int StreamCompare(Stream stream1, Stream stream2) {
            const int BUFFER_SIZE = 4096;
            var buffer1 = new byte[BUFFER_SIZE];
            var buffer2 = new byte[BUFFER_SIZE];

            while(true) {
                var read1 = stream1.Read(buffer1, 0, BUFFER_SIZE);
                var read2 = stream2.Read(buffer2, 0, BUFFER_SIZE);

                var min = Math.Min(read1, read2);
                int diff = ByteArrayCompare(buffer1, buffer2, min);
                if(diff != 0)
                    return diff;

                diff = read1.CompareTo(read2);
                if(diff != 0)
                    return diff;
                if(read1 == 0)
                    break;
            }

            return 0;
        }
        #endregion
        #region static StreamCompareIndex()
        /// <summary>
        ///     Returns the index where the data differs.
        ///     Returns -1 if equal.
        /// </summary>
        [MethodImpl(AggressiveInlining)]
        public static long StreamCompareIndex(Stream stream1, Stream stream2) {
            const int BUFFER_SIZE = 4096;

            long pos = 0;
            var buffer1 = new byte[BUFFER_SIZE];
            var buffer2 = new byte[BUFFER_SIZE];

            while(true) {
                var read1 = stream1.Read(buffer1, 0, BUFFER_SIZE);
                var read2 = stream2.Read(buffer2, 0, BUFFER_SIZE);

                var min = Math.Min(read1, read2);
                int index = ByteArrayCompareIndex(buffer1, buffer2, min);
                if(index >= 0)
                    return pos + index;

                int diff = read1.CompareTo(read2);
                if(diff != 0)
                    return pos + min;
                if(read1 == 0)
                    break;

                pos += read1;
            }

            return -1;
        }
        #endregion

        #region static ApplyDeltaToBuffer()
        /// <summary>
        ///     Changes the buffer values for the delta (differences) between every item.
        /// </summary>
        public static void ApplyDeltaToBuffer(ulong[] buffer, int offset, int count, ref ulong prev) {
            var prevCopy = prev;
            if(Vector.IsHardwareAccelerated) {
                var chunk = Vector<ulong>.Count;
                if(count > chunk) {
                    var prevVector = new Vector<ulong>(buffer, offset);
                    buffer[offset] = prevCopy;
                    Vector<ulong> current = default;
                    while(count > chunk) {
                        current = new Vector<ulong>(buffer, offset + 1);
                        var next = new Vector<ulong>(buffer, offset + chunk);
                        (current - prevVector).CopyTo(buffer, offset + 1);
                        prevVector = next;
                        offset += chunk;
                        count -= chunk;
                    }
                    offset++;
                    count--;
                    prevCopy = current[chunk - 1];
                }
            } else {
                while(count >= 16) {
                    InternalDelta(ref buffer[offset + 0], ref prevCopy);
                    InternalDelta(ref buffer[offset + 1], ref prevCopy);
                    InternalDelta(ref buffer[offset + 2], ref prevCopy);
                    InternalDelta(ref buffer[offset + 3], ref prevCopy);
                    InternalDelta(ref buffer[offset + 4], ref prevCopy);
                    InternalDelta(ref buffer[offset + 5], ref prevCopy);
                    InternalDelta(ref buffer[offset + 6], ref prevCopy);
                    InternalDelta(ref buffer[offset + 7], ref prevCopy);
                    InternalDelta(ref buffer[offset + 8], ref prevCopy);
                    InternalDelta(ref buffer[offset + 9], ref prevCopy);
                    InternalDelta(ref buffer[offset + 10], ref prevCopy);
                    InternalDelta(ref buffer[offset + 11], ref prevCopy);
                    InternalDelta(ref buffer[offset + 12], ref prevCopy);
                    InternalDelta(ref buffer[offset + 13], ref prevCopy);
                    InternalDelta(ref buffer[offset + 14], ref prevCopy);
                    InternalDelta(ref buffer[offset + 15], ref prevCopy);
                    offset += 16;
                    count -= 16;
                }
            }
            while(count-- > 0)
                InternalDelta(ref buffer[offset++], ref prevCopy);
            prev = prevCopy;
        }
        /// <summary>
        ///     Changes the buffer values for the delta (differences) between every item.
        /// </summary>
        public static void ApplyDeltaToBuffer(uint[] buffer, int offset, int count, ref uint prev) {
            var prevCopy = prev;
            if(Vector.IsHardwareAccelerated) {
                var chunk = Vector<uint>.Count;
                if(count > chunk) {
                    var prevVector = new Vector<uint>(buffer, offset);
                    buffer[offset] -= prevCopy;
                    Vector<uint> current = default;
                    while(count > chunk) {
                        current = new Vector<uint>(buffer, offset + 1);
                        var next = new Vector<uint>(buffer, offset + chunk);
                        (current - prevVector).CopyTo(buffer, offset + 1);
                        prevVector = next;
                        offset += chunk;
                        count -= chunk;
                    }
                    offset++;
                    count--;
                    prevCopy = current[chunk - 1];
                }
            } else {
                while(count >= 16) {
                    InternalDelta(ref buffer[offset + 0], ref prevCopy);
                    InternalDelta(ref buffer[offset + 1], ref prevCopy);
                    InternalDelta(ref buffer[offset + 2], ref prevCopy);
                    InternalDelta(ref buffer[offset + 3], ref prevCopy);
                    InternalDelta(ref buffer[offset + 4], ref prevCopy);
                    InternalDelta(ref buffer[offset + 5], ref prevCopy);
                    InternalDelta(ref buffer[offset + 6], ref prevCopy);
                    InternalDelta(ref buffer[offset + 7], ref prevCopy);
                    InternalDelta(ref buffer[offset + 8], ref prevCopy);
                    InternalDelta(ref buffer[offset + 9], ref prevCopy);
                    InternalDelta(ref buffer[offset + 10], ref prevCopy);
                    InternalDelta(ref buffer[offset + 11], ref prevCopy);
                    InternalDelta(ref buffer[offset + 12], ref prevCopy);
                    InternalDelta(ref buffer[offset + 13], ref prevCopy);
                    InternalDelta(ref buffer[offset + 14], ref prevCopy);
                    InternalDelta(ref buffer[offset + 15], ref prevCopy);
                    offset += 16;
                    count -= 16;
                }
            }
            while(count-- > 0)
                InternalDelta(ref buffer[offset++], ref prevCopy);
            prev = prevCopy;
        }
        /// <summary>
        ///     Changes the buffer values for the delta (differences) between every item.
        /// </summary>
        public static void ApplyDeltaToBuffer(ushort[] buffer, int offset, int count, ref ushort prev) {
            var prevCopy = prev;
            if(Vector.IsHardwareAccelerated) {
                var chunk = Vector<ushort>.Count;
                if(count > chunk) {
                    var prevVector = new Vector<ushort>(buffer, offset);
                    buffer[offset] -= prevCopy;
                    Vector<ushort> current = default;
                    while(count > chunk) {
                        current = new Vector<ushort>(buffer, offset + 1);
                        var next = new Vector<ushort>(buffer, offset + chunk);
                        (current - prevVector).CopyTo(buffer, offset + 1);
                        prevVector = next;
                        offset += chunk;
                        count -= chunk;
                    }
                    offset++;
                    count--;
                    prevCopy = current[chunk - 1];
                }
            } else {
                while(count >= 16) {
                    InternalDelta(ref buffer[offset + 0], ref prevCopy);
                    InternalDelta(ref buffer[offset + 1], ref prevCopy);
                    InternalDelta(ref buffer[offset + 2], ref prevCopy);
                    InternalDelta(ref buffer[offset + 3], ref prevCopy);
                    InternalDelta(ref buffer[offset + 4], ref prevCopy);
                    InternalDelta(ref buffer[offset + 5], ref prevCopy);
                    InternalDelta(ref buffer[offset + 6], ref prevCopy);
                    InternalDelta(ref buffer[offset + 7], ref prevCopy);
                    InternalDelta(ref buffer[offset + 8], ref prevCopy);
                    InternalDelta(ref buffer[offset + 9], ref prevCopy);
                    InternalDelta(ref buffer[offset + 10], ref prevCopy);
                    InternalDelta(ref buffer[offset + 11], ref prevCopy);
                    InternalDelta(ref buffer[offset + 12], ref prevCopy);
                    InternalDelta(ref buffer[offset + 13], ref prevCopy);
                    InternalDelta(ref buffer[offset + 14], ref prevCopy);
                    InternalDelta(ref buffer[offset + 15], ref prevCopy);
                    offset += 16;
                    count -= 16;
                }
            }
            while(count-- > 0)
                InternalDelta(ref buffer[offset++], ref prevCopy);
            prev = prevCopy;
        }
        /// <summary>
        ///     Changes the buffer values for the delta (differences) between every item.
        /// </summary>
        public static void ApplyDeltaToBuffer(byte[] buffer, int offset, int count, ref byte prev) {
            var prevCopy = prev;
            if(Vector.IsHardwareAccelerated) {
                var chunk = Vector<byte>.Count;
                if(count > chunk) {
                    var prevVector = new Vector<byte>(buffer, offset);
                    buffer[offset] -= prevCopy;
                    Vector<byte> current = default;
                    while(count > chunk) {
                        current = new Vector<byte>(buffer, offset + 1);
                        var next = new Vector<byte>(buffer, offset + chunk);
                        (current - prevVector).CopyTo(buffer, offset + 1);
                        prevVector = next;
                        offset += chunk;
                        count -= chunk;
                    }
                    offset++;
                    count--;
                    prevCopy = current[chunk - 1];
                }
            } else {
                while(count >= 16) {
                    InternalDelta(ref buffer[offset + 0], ref prevCopy);
                    InternalDelta(ref buffer[offset + 1], ref prevCopy);
                    InternalDelta(ref buffer[offset + 2], ref prevCopy);
                    InternalDelta(ref buffer[offset + 3], ref prevCopy);
                    InternalDelta(ref buffer[offset + 4], ref prevCopy);
                    InternalDelta(ref buffer[offset + 5], ref prevCopy);
                    InternalDelta(ref buffer[offset + 6], ref prevCopy);
                    InternalDelta(ref buffer[offset + 7], ref prevCopy);
                    InternalDelta(ref buffer[offset + 8], ref prevCopy);
                    InternalDelta(ref buffer[offset + 9], ref prevCopy);
                    InternalDelta(ref buffer[offset + 10], ref prevCopy);
                    InternalDelta(ref buffer[offset + 11], ref prevCopy);
                    InternalDelta(ref buffer[offset + 12], ref prevCopy);
                    InternalDelta(ref buffer[offset + 13], ref prevCopy);
                    InternalDelta(ref buffer[offset + 14], ref prevCopy);
                    InternalDelta(ref buffer[offset + 15], ref prevCopy);
                    offset += 16;
                    count -= 16;
                }
            }
            while(count-- > 0)
                InternalDelta(ref buffer[offset++], ref prevCopy);
            prev = prevCopy;
        }
        /// <summary>
        ///     Changes the buffer values for the delta (differences) between every item.
        /// </summary>
        public static void ApplyDeltaToBuffer(long[] buffer, int offset, int count, ref long prev) {
            var prevCopy = prev;
            if(Vector.IsHardwareAccelerated) {
                var chunk = Vector<long>.Count;
                if(count > chunk) {
                    var prevVector = new Vector<long>(buffer, offset);
                    buffer[offset] -= prevCopy;
                    Vector<long> current = default;
                    while(count > chunk) {
                        current = new Vector<long>(buffer, offset + 1);
                        var next = new Vector<long>(buffer, offset + chunk);
                        (current - prevVector).CopyTo(buffer, offset + 1);
                        prevVector = next;
                        offset += chunk;
                        count -= chunk;
                    }
                    offset++;
                    count--;
                    prevCopy = current[chunk - 1];
                }
            } else {
                while(count >= 16) {
                    InternalDelta(ref buffer[offset + 0], ref prevCopy);
                    InternalDelta(ref buffer[offset + 1], ref prevCopy);
                    InternalDelta(ref buffer[offset + 2], ref prevCopy);
                    InternalDelta(ref buffer[offset + 3], ref prevCopy);
                    InternalDelta(ref buffer[offset + 4], ref prevCopy);
                    InternalDelta(ref buffer[offset + 5], ref prevCopy);
                    InternalDelta(ref buffer[offset + 6], ref prevCopy);
                    InternalDelta(ref buffer[offset + 7], ref prevCopy);
                    InternalDelta(ref buffer[offset + 8], ref prevCopy);
                    InternalDelta(ref buffer[offset + 9], ref prevCopy);
                    InternalDelta(ref buffer[offset + 10], ref prevCopy);
                    InternalDelta(ref buffer[offset + 11], ref prevCopy);
                    InternalDelta(ref buffer[offset + 12], ref prevCopy);
                    InternalDelta(ref buffer[offset + 13], ref prevCopy);
                    InternalDelta(ref buffer[offset + 14], ref prevCopy);
                    InternalDelta(ref buffer[offset + 15], ref prevCopy);
                    offset += 16;
                    count -= 16;
                }
            }
            while(count-- > 0)
                InternalDelta(ref buffer[offset++], ref prevCopy);
            prev = prevCopy;
        }
        /// <summary>
        ///     Changes the buffer values for the delta (differences) between every item.
        /// </summary>
        public static void ApplyDeltaToBuffer(int[] buffer, int offset, int count, ref int prev) {
            var prevCopy = prev;
            if(Vector.IsHardwareAccelerated) {
                var chunk = Vector<int>.Count;
                if(count > chunk) {
                    var prevVector = new Vector<int>(buffer, offset);
                    buffer[offset] -= prevCopy;
                    Vector<int> current = default;
                    while(count > chunk) {
                        current = new Vector<int>(buffer, offset + 1);
                        var next = new Vector<int>(buffer, offset + chunk);
                        (current - prevVector).CopyTo(buffer, offset + 1);
                        prevVector = next;
                        offset += chunk;
                        count -= chunk;
                    }
                    offset++;
                    count--;
                    prevCopy = current[chunk - 1];
                }
            } else {
                while(count >= 16) {
                    InternalDelta(ref buffer[offset + 0], ref prevCopy);
                    InternalDelta(ref buffer[offset + 1], ref prevCopy);
                    InternalDelta(ref buffer[offset + 2], ref prevCopy);
                    InternalDelta(ref buffer[offset + 3], ref prevCopy);
                    InternalDelta(ref buffer[offset + 4], ref prevCopy);
                    InternalDelta(ref buffer[offset + 5], ref prevCopy);
                    InternalDelta(ref buffer[offset + 6], ref prevCopy);
                    InternalDelta(ref buffer[offset + 7], ref prevCopy);
                    InternalDelta(ref buffer[offset + 8], ref prevCopy);
                    InternalDelta(ref buffer[offset + 9], ref prevCopy);
                    InternalDelta(ref buffer[offset + 10], ref prevCopy);
                    InternalDelta(ref buffer[offset + 11], ref prevCopy);
                    InternalDelta(ref buffer[offset + 12], ref prevCopy);
                    InternalDelta(ref buffer[offset + 13], ref prevCopy);
                    InternalDelta(ref buffer[offset + 14], ref prevCopy);
                    InternalDelta(ref buffer[offset + 15], ref prevCopy);
                    offset += 16;
                    count -= 16;
                }
            }
            while(count-- > 0)
                InternalDelta(ref buffer[offset++], ref prevCopy);
            prev = prevCopy;
        }
        /// <summary>
        ///     Changes the buffer values for the delta (differences) between every item.
        /// </summary>
        public static void ApplyDeltaToBuffer(short[] buffer, int offset, int count, ref short prev) {
            var prevCopy = prev;
            if(Vector.IsHardwareAccelerated) {
                var chunk = Vector<short>.Count;
                if(count > chunk) {
                    var prevVector = new Vector<short>(buffer, offset);
                    buffer[offset] -= prevCopy;
                    Vector<short> current = default;
                    while(count > chunk) {
                        current = new Vector<short>(buffer, offset + 1);
                        var next = new Vector<short>(buffer, offset + chunk);
                        (current - prevVector).CopyTo(buffer, offset + 1);
                        prevVector = next;
                        offset += chunk;
                        count -= chunk;
                    }
                    offset++;
                    count--;
                    prevCopy = current[chunk - 1];
                }
            } else {
                while(count >= 16) {
                    InternalDelta(ref buffer[offset + 0], ref prevCopy);
                    InternalDelta(ref buffer[offset + 1], ref prevCopy);
                    InternalDelta(ref buffer[offset + 2], ref prevCopy);
                    InternalDelta(ref buffer[offset + 3], ref prevCopy);
                    InternalDelta(ref buffer[offset + 4], ref prevCopy);
                    InternalDelta(ref buffer[offset + 5], ref prevCopy);
                    InternalDelta(ref buffer[offset + 6], ref prevCopy);
                    InternalDelta(ref buffer[offset + 7], ref prevCopy);
                    InternalDelta(ref buffer[offset + 8], ref prevCopy);
                    InternalDelta(ref buffer[offset + 9], ref prevCopy);
                    InternalDelta(ref buffer[offset + 10], ref prevCopy);
                    InternalDelta(ref buffer[offset + 11], ref prevCopy);
                    InternalDelta(ref buffer[offset + 12], ref prevCopy);
                    InternalDelta(ref buffer[offset + 13], ref prevCopy);
                    InternalDelta(ref buffer[offset + 14], ref prevCopy);
                    InternalDelta(ref buffer[offset + 15], ref prevCopy);
                    offset += 16;
                    count -= 16;
                }
            }
            while(count-- > 0)
                InternalDelta(ref buffer[offset++], ref prevCopy);
            prev = prevCopy;
        }
        /// <summary>
        ///     Changes the buffer values for the delta (differences) between every item.
        /// </summary>
        public static void ApplyDeltaToBuffer(sbyte[] buffer, int offset, int count, ref sbyte prev) {
            var prevCopy = prev;
            if(Vector.IsHardwareAccelerated) {
                var chunk = Vector<sbyte>.Count;
                if(count > chunk) {
                    var prevVector = new Vector<sbyte>(buffer, offset);
                    buffer[offset] -= prevCopy;
                    Vector<sbyte> current = default;
                    while(count > chunk) {
                        current = new Vector<sbyte>(buffer, offset + 1);
                        var next = new Vector<sbyte>(buffer, offset + chunk);
                        (current - prevVector).CopyTo(buffer, offset + 1);
                        prevVector = next;
                        offset += chunk;
                        count -= chunk;
                    }
                    offset++;
                    count--;
                    prevCopy = current[chunk - 1];
                }
            } else {
                while(count >= 16) {
                    InternalDelta(ref buffer[offset + 0], ref prevCopy);
                    InternalDelta(ref buffer[offset + 1], ref prevCopy);
                    InternalDelta(ref buffer[offset + 2], ref prevCopy);
                    InternalDelta(ref buffer[offset + 3], ref prevCopy);
                    InternalDelta(ref buffer[offset + 4], ref prevCopy);
                    InternalDelta(ref buffer[offset + 5], ref prevCopy);
                    InternalDelta(ref buffer[offset + 6], ref prevCopy);
                    InternalDelta(ref buffer[offset + 7], ref prevCopy);
                    InternalDelta(ref buffer[offset + 8], ref prevCopy);
                    InternalDelta(ref buffer[offset + 9], ref prevCopy);
                    InternalDelta(ref buffer[offset + 10], ref prevCopy);
                    InternalDelta(ref buffer[offset + 11], ref prevCopy);
                    InternalDelta(ref buffer[offset + 12], ref prevCopy);
                    InternalDelta(ref buffer[offset + 13], ref prevCopy);
                    InternalDelta(ref buffer[offset + 14], ref prevCopy);
                    InternalDelta(ref buffer[offset + 15], ref prevCopy);
                    offset += 16;
                    count -= 16;
                }
            }
            while(count-- > 0)
                InternalDelta(ref buffer[offset++], ref prevCopy);
            prev = prevCopy;
        }

        [MethodImpl(AggressiveInlining)]
        private static void InternalDelta(ref ulong value, ref ulong m_prev) {
            var prev = m_prev;
            m_prev = value;
            value -= prev;
        }
        [MethodImpl(AggressiveInlining)]
        private static void InternalDelta(ref uint value, ref uint m_prev) {
            var prev = m_prev;
            m_prev = value;
            value -= prev;
        }
        [MethodImpl(AggressiveInlining)]
        private static void InternalDelta(ref ushort value, ref ushort m_prev) {
            var prev = m_prev;
            m_prev = value;
            value -= prev;
        }
        [MethodImpl(AggressiveInlining)]
        private static void InternalDelta(ref byte value, ref byte m_prev) {
            var prev = m_prev;
            m_prev = value;
            value -= prev;
        }
        [MethodImpl(AggressiveInlining)]
        private static void InternalDelta(ref long value, ref long m_prev) {
            var prev = m_prev;
            m_prev = value;
            value -= prev;
        }
        [MethodImpl(AggressiveInlining)]
        private static void InternalDelta(ref int value, ref int m_prev) {
            var prev = m_prev;
            m_prev = value;
            value -= prev;
        }
        [MethodImpl(AggressiveInlining)]
        private static void InternalDelta(ref short value, ref short m_prev) {
            var prev = m_prev;
            m_prev = value;
            value -= prev;
        }
        [MethodImpl(AggressiveInlining)]
        private static void InternalDelta(ref sbyte value, ref sbyte m_prev) {
            var prev = m_prev;
            m_prev = value;
            value -= prev;
        }
        #endregion

        #region static GenerateCombinations()
        /// <summary>
        ///     Generates all the combinations possible for the given source.
        ///     This will maintain the ordering.
        ///     For performance reasons, the same instance will be returned continually, so clone it if needed.
        ///     Outputs source.Aggregate(0, (curr, o) => curr * o.Length) results (ie: multiplicative product).
        ///     
        ///     ex: this([3,2]) = { [0,0], [0,1], [1,0], [1,1], [2,0], [2,1] }
        ///     ex: this({[A,B], [C,D]}) -> {[A,C], [A,D], [B,C], [B,D]}
        /// </summary>
        public static IEnumerable<int[]> GenerateCombinations(IEnumerable<int> item_counts) {
            bool found;
            int[] counts = item_counts.ToArray();
            int[] path = new int[counts.Length];
            int max = counts.Length - 1;

            yield return path;

            var count = counts.Length;
            for(int i = 0; i < count; i++)
                counts[i]--;

            do {
                found = false;
                int dimension = max;
                while(dimension >= 0) {
                    if(path[dimension] < counts[dimension]) {
                        path[dimension]++;
                        while(dimension < max)
                            path[++dimension] = 0;
                        yield return path;
                        found = true;
                        break;
                    } else
                        dimension--;
                }
            } while(found);
        }
        /// <summary>
        ///     Generates all the combinations possible for the given source.
        ///     This will maintain the ordering.
        ///     For performance reasons, the same instance will be returned continually, so clone it if needed.
        ///     Outputs source.Aggregate(0, (curr, o) => curr * o.Length) results (ie: multiplicative product).
        ///     
        ///     ex: this({[A,B], [C,D]}) -> {[A,C], [A,D], [B,C], [B,D]}
        /// </summary>
        public static IEnumerable<T[]> GenerateCombinations<T>(IEnumerable<T[]> source) {
            bool found;
            T[][] values = source.ToArray();
            int[] counts = new int[values.Length];
            T[] path = new T[values.Length];
            int[] pathIndex = new int[counts.Length];
            int max = values.Length - 1;

            var count = values.Length;
            for(int i = 0; i < count; i++) {
                var v = values[i];
                path[i] = v[0];
                counts[i] = v.Length - 1;
            }

            yield return path;

            do {
                found = false;
                int dimension = max;
                while(dimension >= 0) {
                    if(pathIndex[dimension] < counts[dimension]) {
                        var index = pathIndex[dimension] + 1;
                        pathIndex[dimension] = index;
                        path[dimension] = values[dimension][index];
                        while(dimension < max) {
                            pathIndex[++dimension] = 0;
                            path[dimension] = values[dimension][0];
                        }
                        yield return path;
                        found = true;
                        break;
                    } else
                        dimension--;
                }
            } while(found);
        }
        /// <summary>
        ///     Generates all the combinations possible for the given source.
        ///     This will maintain the ordering.
        ///     For performance reasons, the same instance will be returned continually, so clone it if needed.
        ///     Outputs source.Aggregate(0, (curr, o) => curr * o.Length) results (ie: multiplicative product).
        ///     
        ///     ex: {[A,B], [C,D]} -> {[A,C], [A,D], [B,C], [B,D]}
        /// </summary>
        /// <param name="filter">Filters sub results, short-circuiting the needless processings.</param>
        /// <param name="selector">Generates sub-items for one dimension/level/bucket.</param>
        public static IEnumerable<T[]> GenerateCombinations<T>(IEnumerable<T[]> source, CombinationEnumerator<T>.SubResultFilter filter = null, CombinationEnumerator<T>.SubResultSelector selector = null) {
            return new CombinationEnumerator<T>(source, filter, selector).List();
        }
        public sealed class CombinationEnumerator<T> {
            private readonly T[][] m_sourceBackup;
            private readonly int m_dimensions;
            private readonly T[] m_result;
            private readonly SubResultFilter m_filter;
            private readonly SubResultSelector m_selector;

            public CombinationEnumerator(IEnumerable<T[]> source, SubResultFilter filter = null, SubResultSelector selector = null) {
                m_sourceBackup = source as T[][] ?? source.ToArray();
                m_dimensions = m_sourceBackup.Length;
                m_result = new T[m_sourceBackup.Length];
                m_filter = filter;
                m_selector = selector;
            }
            public IEnumerable<T[]> List() {
                if(m_sourceBackup.All(o => o == null || o.Length == 0))
                    return Enumerable.Empty<T[]>();

                // implement 4 times because combinations can generate a ton of results
                // so any speed boost will matter

                if(m_filter == null) {
                    return m_selector == null ?
                        this.ListImplementation_NoFilterNoSelector() :
                        this.ListImplementation_NoFilterSelector(0);
                } else {
                    return m_selector == null ?
                        this.ListImplementation_FilterNoSelector(0) :
                        // would be weird/inefficient to both have a selector and a filter (and kind of redundant), but who knows
                        this.ListImplementation_FilterSelector(0);
                }
            }
            private IEnumerable<T[]> ListImplementation_NoFilterNoSelector() {
                return GenerateCombinations(m_sourceBackup);
            }
            private IEnumerable<T[]> ListImplementation_FilterNoSelector(int depth) {
                var dimension = m_sourceBackup[depth];
                var dimension_size = dimension == null ? 0 : dimension.Length;

                if(dimension_size == 0) {
                    if(depth + 1 < m_dimensions) {
                        // recurse until last layer
                        foreach(var item in this.ListImplementation_FilterNoSelector(depth + 1))
                            yield return item;
                    } else
                        // last layer
                        yield return m_result;
                } else {
                    for(int i = 0; i < dimension_size; i++) {
                        m_result[depth] = dimension[i];

                        if(!m_filter(m_result, depth + 1))
                            continue;

                        if(depth + 1 < m_dimensions) {
                            // recurse until last layer
                            foreach(var item in this.ListImplementation_FilterNoSelector(depth + 1))
                                yield return item;
                        } else
                            // last layer
                            yield return m_result;
                    }
                }
            }
            private IEnumerable<T[]> ListImplementation_NoFilterSelector(int depth) {
                var dimension = m_sourceBackup[depth];

                if(dimension == null || dimension.Length == 0) {
                    if(depth + 1 < m_dimensions) {
                        // recurse until last layer
                        foreach(var item in this.ListImplementation_NoFilterSelector(depth + 1))
                            yield return item;
                    } else
                        // last layer
                        yield return m_result;
                } else {
                    foreach(var current in m_selector(dimension, m_result, depth)) {
                        m_result[depth] = current;

                        if(depth + 1 < m_dimensions) {
                            // recurse until last layer
                            foreach(var item in this.ListImplementation_NoFilterSelector(depth + 1))
                                yield return item;
                        } else
                            // last layer
                            yield return m_result;
                    }
                }
            }
            private IEnumerable<T[]> ListImplementation_FilterSelector(int depth) {
                var dimension = m_sourceBackup[depth];

                if(dimension == null || dimension.Length == 0) {
                    if(depth + 1 < m_dimensions) {
                        // recurse until last layer
                        foreach(var item in this.ListImplementation_FilterSelector(depth + 1))
                            yield return item;
                    } else
                        // last layer
                        yield return m_result;
                } else {
                    foreach(var current in m_selector(dimension, m_result, depth)) {
                        m_result[depth] = current;

                        if(!m_filter(m_result, depth + 1))
                            continue;

                        if(depth + 1 < m_dimensions) {
                            // recurse until last layer
                            foreach(var item in this.ListImplementation_FilterSelector(depth + 1))
                                yield return item;
                        } else
                            // last layer
                            yield return m_result;
                    }
                }
            }

            /// <param name="len">The number of result[] that are written to. The current item is included in result[].</param>
            public delegate bool SubResultFilter(T[] result, int len);
            /// <param name="len">The number of result[] that are written to. The current item is included in result[].</param>
            public delegate IEnumerable<T> SubResultSelector(IEnumerable<T> current_dimension, T[] result, int len);
        }
        #endregion
        #region static GeneratePermutations()
        /// <summary>
        ///     Generates all the permutations possible for the given source such that all possible orderings are returned.
        ///     Use result.ToArray() if you want to store the results, as the same instance is returned every time.
        ///     Outputs 'source.Length!' (factorial) results.
        ///     ex: [A,B,C] -> {[A,B,C], [A,B,C], [B,A,C], [B,C,A], [C,A,B], [C,B,A]}
        /// </summary>
        /// <param name="filter">Filters sub results, short-circuiting the needless processings.</param>
        /// <param name="selector">Generates sub-items for one dimension/level/bucket. default = remaining.Where(o => o.Count > 0)</param>
        public static IEnumerable<T[]> GeneratePermutations<T>(IEnumerable<T> source, PermutationEnumerator<T>.SubResultFilter filter = null, PermutationEnumerator<T>.SubResultSelector selector = null) {
            return new PermutationEnumerator<T>(source, filter, selector).List();
        }
        public sealed class PermutationEnumerator<T> {
            private readonly List<T> m_source;
            private readonly T[] m_current;
            private readonly int m_count;
            private int m_currentIndex = 0;
            private readonly SubResultFilter m_filter;
            private readonly SubResultSelector m_selector;
            private readonly ItemToken[] m_remaining;
            /// <param name="filter">Filters sub results, short-circuiting the needless processings.</param>
            /// <param name="selector">Generates sub-items for one dimension/level/bucket. default = remaining.Where(o => o.Count > 0)</param>
            public PermutationEnumerator(IEnumerable<T> source, SubResultFilter filter = null, SubResultSelector selector = null) {
                m_source = source as List<T> ?? source.ToList();
                m_count = m_source.Count;
                m_current = new T[m_count];
                m_filter = filter;
                m_selector = selector;

                m_remaining = m_source
                    .GroupBy(o => o)
                    .Select(o => new ItemToken() { Item = o.Key, Count = o.Count() })
                    .ToArray();
            }
            public IEnumerable<T[]> List() {
                m_currentIndex = 0;
                if(m_count == 0)
                    return Enumerable.Empty<T[]>();

                // implement 4 times because permutations can generate a ton of results
                // so any speed boost will matter

                if(m_filter == null) {
                    return m_selector == null ?
                        this.ListImplementation_NoFilterNoSelector() :
                        this.ListImplementation_NoFilterSelector();
                } else {
                    return m_selector == null ?
                        this.ListImplementation_FilterNoSelector() :
                        // would be weird/inefficient to both have a selector and a filter (and kind of redundant), but who knows
                        this.ListImplementation_FilterSelector();
                }
            }
            private IEnumerable<T[]> ListImplementation_NoFilterNoSelector() {
                foreach(var item in m_remaining.Where(o => o.Count > 0).ToList()) {
                    m_current[m_currentIndex++] = item.Item;
                    item.Count--;
                    
                    if(m_currentIndex < m_count) {
                        // recurse until last layer
                        foreach(var x in this.ListImplementation_NoFilterNoSelector())
                            yield return x;
                    } else
                        // last layer
                        yield return m_current;

                    m_currentIndex--;
                    item.Count++;
                }
            }
            private IEnumerable<T[]> ListImplementation_FilterNoSelector() {
                foreach(var item in m_remaining.Where(o => o.Count > 0).ToList()) {
                    m_current[m_currentIndex++] = item.Item;
                    item.Count--;

                    if(m_filter(m_current, m_currentIndex)) {
                        if(m_currentIndex < m_count) {
                            // recurse until last layer
                            foreach(var x in this.ListImplementation_FilterNoSelector())
                                yield return x;
                        } else
                            // last layer
                            yield return m_current;
                    }

                    m_currentIndex--;
                    item.Count++;
                }
            }
            private IEnumerable<T[]> ListImplementation_NoFilterSelector() {
                foreach(var item in m_selector(m_remaining, m_current, m_currentIndex).ToList()) {
                    m_current[m_currentIndex++] = item.Item;
                    item.Count--;

                    if(m_currentIndex < m_count) {
                        // recurse until last layer
                        foreach(var x in this.ListImplementation_NoFilterSelector())
                            yield return x;
                    } else
                        // last layer
                        yield return m_current;

                    m_currentIndex--;
                    item.Count++;
                }
            }
            private IEnumerable<T[]> ListImplementation_FilterSelector() {
                foreach(var item in m_selector(m_remaining, m_current, m_currentIndex).ToList()) {
                    m_current[m_currentIndex++] = item.Item;
                    item.Count--;

                    if(m_filter(m_current, m_currentIndex)) {
                        if(m_currentIndex < m_count) {
                            // recurse until last layer
                            foreach(var x in this.ListImplementation_FilterSelector())
                                yield return x;
                        } else
                            // last layer
                            yield return m_current;
                    }

                    m_currentIndex--;
                    item.Count++;
                }
            }

            public class ItemToken {
                public T Item;
                public int Count;
                public override string ToString() {
                    return string.Format("[{0}] {1}", this.Count, this.Item);
                }
            }

            public delegate bool SubResultFilter(T[] result, int len);
            public delegate IEnumerable<ItemToken> SubResultSelector(ItemToken[] remaining, T[] result, int len);
        }
        #endregion
        #region static GenerateBucketedCombinations()
        /// <summary>
        ///     Buckets the items into separate dimensions.
        ///     The results include the combinations per dimensions, but not the permutations.
        ///     Ensures the items are uniquely assigned, not repeated per dimension per result.
        ///     Keep in mind that this would generate an enormous amount of results; consequently the permutation method is to be handled by the caller.
        ///     
        ///     This will maintain dimensional ordering.
        ///     
        ///     For performance reasons, the same instance will be returned continually, so clone it if needed.
        ///     Outputs [multiplicative product] of [2 exponent to the dimension size] results. (ie: 2^4 * 2^5 * 2^6)
        /// </summary>
        /// <param name="source">The items, filtered by the dimensions in which they can appear. The items may repeat across dimension filters, but the result will only bucket them into one dimension.</param>
        /// <param name="filter">Filters sub results, short-circuiting the needless processings. You'll receive already valid result parts by this point, meaning no items will be duplicates.</param>
        /// <param name="selector">default: (IEnumerable[T] current_dimension, T[] result, int len) => GenerateCombinations(current_dimension)</param>
        /// <returns>T[dimension/bucket][items within bucket]. dimension/bucket matches source.Count().</returns>
        public static IEnumerable<T[][]> GenerateBucketedCombinations<T>(IEnumerable<HashSet<T>> source, CombinationEnumerator<T[]>.SubResultFilter filter = null, CombinationEnumerator<T[]>.SubResultSelector selector = null) {
            // example use:
            //
            //var itemss = new[] {
            //    new HashSet<string>("pu1,dp1,pu2,dp2,pu3,dp3,pu4,dp4,pu5,dp5".Split(',')),
            //    new HashSet<string>("pu1,dp1,pu2,dp2,pu3,dp3,pu4,dp4,pu5,dp5".Split(',')),
            //};
            //var now = DateTime.UtcNow;
            //var res = Miscellaneous.GenerateBucketedCombinations(
            //    itemss,
            //    null,
            //    //new Miscellaneous.CombinationEnumerator<string[]>.SubResultFilter((string[][] res2, int len) => {
            //    //    // since results are not permuted/re-ordered, we must assume that within-dimension data is ordered 'legally'
            //    //    if(len > 1) {
            //    //        // don't let a drop occur before a pickup
            //    //        var hash = new HashSet<string>();
            //    //        for(int i = 0; i < len; i++) {
            //    //            var d = res2[i];
            //    //            var count = d.Length;
            //    //            for(int j = 0; j < count; j++) {
            //    //                if(d[j] != null && d[j].StartsWith("pu"))
            //    //                    hash.Add(d[j]);
            //    //            }
            //    //            for(int j = 0; j < count; j++) {
            //    //                if(d[j] != null && d[j].StartsWith("dp") && !hash.Remove("pu" + d[j].Substring(2)))
            //    //                    return false;
            //    //            }
            //    //        }
            //    //    }
            //    //    return true;
            //    //}),
            //    new Miscellaneous.CombinationEnumerator<string[]>.SubResultSelector((IEnumerable<string[]> current_dimension, string[][] result, int len) => {
            //        // don't let a drop occur before a pickup
            //        var prev_pickups_missing_drop = new HashSet<string>();
            //        for(int i = 0; i < len; i++) {
            //            var d = result[i];
            //            var count = d.Length;
            //            for(int j = 0; j < count; j++) {
            //                if(d[j] != null && d[j].StartsWith("pu"))
            //                    prev_pickups_missing_drop.Add(d[j]);
            //            }
            //            for(int j = 0; j < count; j++) {
            //                if(d[j] != null && d[j].StartsWith("dp") && !prev_pickups_missing_drop.Remove("pu" + d[j].Substring(2)))
            //                    throw new InvalidOperationException("Generated drop before pickup.");
            //            }
            //        }
            //
            //        return Miscellaneous.GenerateCombinations(current_dimension)
            //            .SelectMany(o => Miscellaneous.GeneratePermutations(o, new Miscellaneous.PermutationEnumerator<string>.SubResultFilter((string[] k, int len2) => {
            //                var top = k[len2 - 1];
            //                if(top != null && top.StartsWith("dp")) {
            //                    var lookFor = "pu" + top.Substring(2);
            //                    if(!prev_pickups_missing_drop.Contains(lookFor) &&
            //                        !k.Take(len2 - 1).Any(m => string.CompareOrdinal(m, lookFor) == 0))
            //                        // the drop needs the pickup within this bucket/dimension, which it hasn't seen yet,
            //                        // so use another permutation until we do it properly
            //                        return false;
            //                }
            //                if(len2 < 2)
            //                    return true;
            //                if(top != null) {
            //                    // if at any point prior to current we have a hole (null with values on both sides), 
            //                    // skip permutation since we want all nulls to be at the end (in order to remove redundant results, ie: [null,1,2] == [1,2,null])
            //                    if(k.Skip(len2 - 2).First() == null)
            //                        return false;
            //                }
            //
            //                return true;
            //            })));
            //    })
            //).Select(o => {
            //    var x = (string[][])o.Clone();
            //    for(int i = 0; i < x.Length; i++)
            //        x[i] = x[i]?.Where(k => k != null).ToArray();
            //    return x;
            //}).Select(o => string.Join(",", o.Select(k => "{" + string.Join(",", k ?? new string[0]) + "}")))
            //.ToList();
            //var bench = DateTime.UtcNow - now;

            return new BucketedCombinationEnumerator<T>(source, filter, selector).List();
        }
        /// <summary>
        ///     Same as a CombinationEnumerator, but allows items to be present into multiple possible buckets.
        ///     Only one bucket will be selected per item, and all of them will be assigned within every result.
        /// </summary>
        private sealed class BucketedCombinationEnumerator<T> {
            private readonly HashSet<T>[] m_sourceBackup;
            private readonly T[][] m_result;
            private readonly int m_dimensions;
            private readonly HashSet<T>[] m_itemsMustExistByThatDimension;
            private readonly HashSet<T>[] m_itemsUniqueAtDimension;
            private readonly HashSet<T> m_assigned;
            private readonly int m_uniqueItemsCount; // non-default values unique count
            private readonly CombinationEnumerator<T[]>.SubResultFilter m_filter;
            private readonly CombinationEnumerator<T[]>.SubResultSelector m_selector;

            /// <param name="source">The sub-items may be null or empty arrays.</param>
            /// <param name="selector">default: (IEnumerable[T] current_dimension, T[] result, int len) => GenerateCombinations(current_dimension)</param>
            public BucketedCombinationEnumerator(IEnumerable<HashSet<T>> source, CombinationEnumerator<T[]>.SubResultFilter filter = null, CombinationEnumerator<T[]>.SubResultSelector selector = null) {
                m_sourceBackup = source as HashSet<T>[] ?? source.ToArray();
                m_dimensions = m_sourceBackup.Length;
                m_result = new T[m_sourceBackup.Length][];
                m_filter = filter;
                m_selector = selector;
                m_assigned = new HashSet<T>();

                for(int i = 0; i < m_dimensions; i++)
                    if(m_sourceBackup[i].Any(o => object.Equals(o, default(T))))
                        // the problem with some dimension having a valid default(T), is that we can't differentiate between
                        // whether or not that was the default(T) or whether the item in bucket is just not present within that bucket
                        // this could be fixed with sentinel value and/or keeping track of the columns that are default(T) rather than not present
                        // but this significantly increases complexity for a case that isn't needed 
                        throw new ArgumentException("Cannot generate combinations where one item is null/default(T).");

                m_uniqueItemsCount = m_sourceBackup.SelectMany(o => o).Where(o => !object.Equals(o, default(T))).Distinct().Count();

                var dimensions = m_sourceBackup.Length;
                m_itemsUniqueAtDimension = new HashSet<T>[dimensions];
                for(int i = 0; i < dimensions; i++) {
                    var dimension = m_sourceBackup[i];

                    m_itemsUniqueAtDimension[i] = new HashSet<T>(dimension.Where(o => {
                        return !m_sourceBackup.Any(k => k != dimension && k.Contains(o));
                    }));
                }

                // to do that efficiently, record the dimensions by which items must be found (since they dont exist later on)
                m_itemsMustExistByThatDimension = new HashSet<T>[dimensions];
                for(int i = 0; i < dimensions; i++) {
                    var hash = new HashSet<T>();
                    foreach(var item in m_sourceBackup[i]) {
                        if(!m_sourceBackup.Skip(i + 1).Any(o => o.Contains(item))) {
                            int start = 0;
                            while(start < i) {
                                if(m_sourceBackup[start].Contains(item))
                                    break;
                                start++;
                            }
                            hash.Add(item);
                        }
                    }
                    m_itemsMustExistByThatDimension[i] = hash;
                }
            }
            public IEnumerable<T[][]> List() {
                // there are many ways to code this, but using
                // GenerateCombinations = 2^n results
                // GeneratePermutations = n! results
                // thus, we avoid at all costs calling GeneratePermutations(), offloading the task to the caller

                // example usage
                // -> trying to find all the scenarios in which a dispatch of 2 shipments (pickup/drop) can be done on an existing trip
                // -> we try and insert pickup/drops within availabilities or travels
                // -> trip: [availability] -> [pickup] -> [travel] -> [drop] -> [availability]
                // -> every one of those avail/travel represent a dimention on which many combination/permutation of pickup/drop can be added unto
                //
                //                  (dimension 1)                (dimension 2)         (dimension 3)
                //               | [availability] -> [pickup] -> [travel] -> [drop] -> [availability]
                // --------------|---------------------------------------------------------------
                // ship=1,scen=1 | pu1+dp1
                // ship=1,scen=2 | pu1                           dp1
                // ship=1,scen=3 | pu1                                                 dp1
                // ship=1,scen=4 |                               pu1+dp1
                // ship=1,scen=5 |                               pu1                   dp1
                // ship=1,scen=6 |                                                     pu1+dp1
                // ship=2,scen=1 | pu2+dp2
                // ship=2,scen=2 | pu2                           dp2
                // ship=2,scen=3 | pu2                                                 dp2
                // ship=2,scen=4 |                               pu2+dp2
                // ship=2,scen=5 |                               pu2                   dp2
                // ship=2,scen=6 |                                                     pu2+dp2
                //
                // we try to bucket the pu1/dp1/pu2/dp2 across the dimensions, such that every one of them are assigned into just 1 dimension per result
                //
                // the algorithm works like this:
                // foreach(dimension)
                //    res = GenerateCombinations(elements_possible_within_dimension(dimension)) // combinations including nulls
                //    filter(res)
                // res = GenerateCombinations(all dimensions results)
                // filter(res)
                // return res
                //
                // if you need to GeneratePermutations(), do it on the results

                if(m_sourceBackup.All(o => o == null || o.Count == 0))
                    return Enumerable.Empty<T[][]>();

                return this.List(0);
            }
            private IEnumerable<T[][]> List(int depth) {
                if(depth == m_dimensions) {
                    // if last layer
                    yield return m_result;
                    yield break;
                }

                var dimension = m_sourceBackup[depth];

                if(dimension == null || dimension.Count == 0) {
                    // recurse
                    foreach(var item in this.List(depth + 1))
                        yield return item;
                } else {
                    // if everything is assigned
                    if(m_assigned.Count == m_uniqueItemsCount) { // mini speedup 0.85s -> 0.79s
                        // no need to recurse
                        //Array.Clear(m_result, depth, m_dimensions - depth);
                        for(int i = depth; i < m_dimensions; i++)
                            m_result[i] = null; // new T[0]
                        yield return m_result;
                        yield break;
                    }

                    var must_exists = m_itemsUniqueAtDimension[depth];
                    var ensure_exists = m_itemsMustExistByThatDimension[depth];

                    // generate sub-dimension items
                    var remaining_items = dimension
                        .Where(o => {
                            // don't list items we have already returned previously
                            return !m_assigned.Contains(o);
                        }).Select(o => {
                            bool non_nullable = must_exists.Contains(o) ||
                                (ensure_exists.Contains(o) && !m_assigned.Contains(o));
                            return non_nullable ?
                                new[] { o } : // speedup: if n is only valid within one dimension, force it to always occur in that dimension
                                new[] { o, default };
                        }).ToList();

                    // if all items have already been assigned
                    if(remaining_items.Count == 0) {
                        // no need to recurse
                        //Array.Clear(m_result, depth, m_dimensions - depth);
                        for(int i = depth; i < m_dimensions; i++)
                            m_result[i] = null; // new T[0]
                        yield return m_result;
                        yield break;
                    }

                    var dimension_items = m_selector == null ?
                        GenerateCombinations(remaining_items) :
                        m_selector(remaining_items, m_result, depth);

                    foreach(var current in dimension_items) {
                        m_result[depth] = current;

                        var count = current.Length;
                        for(int i = 0; i < count; i++) {
                            var temp = current[i];
                            if(!object.Equals(temp, default(T)))
                                m_assigned.Add(temp);
                        }

                        if(m_filter == null || m_filter(m_result, depth + 1)) {
                            // recurse until last layer
                            foreach(var item in this.List(depth + 1))
                                yield return item;
                        }

                        for(int i = 0; i < count; i++) {
                            var temp = current[i];
                            if(!object.Equals(temp, default(T)))
                                m_assigned.Remove(temp);
                        }
                    }
                }
            }
        }
        #endregion
    }
}
