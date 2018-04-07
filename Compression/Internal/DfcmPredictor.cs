using System;
using System.Runtime.CompilerServices;

// EXPLANATION OF HASH ALGORITHM
//
// IEEE 754 double-precision floating point number representation:
// double
// 1000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000
// ^ sign (1 bit)
//  ^ exponent (11 bits)
//                ^ mantissa/fraction (52 bits)
//
// float
// 1000 0000 0000 0000 0000 0000 0000 0000
// ^ sign (1 bit)
//  ^ exponent (8 bits)
//            ^ mantissa/fraction (23 bits)
//
// decimal
// 1000 0000 0000 0000 0000 0000 0000 0000 int[3] - see below ------------\
// 0000 0000 0000 0000 0000 0000 0000 0000 int[2] >                       |
// 0000 0000 0000 0000 0000 0000 0000 0000 int[1] > 96 bits integer       |
// 0000 0000 0000 0000 0000 0000 0000 0000 int[0] >                       v
// ^ sign (1 bit)                                                       (int[3])
//  ^ not used must be zero (7 bits)                                    (int[3])
//           ^ exponent (8 bits) (power of 10 to divide the integer by) (int[3])
//                     ^ not used must be zero (16 bits)                (int[3])
// 
// by using a custom hash algorithm, we try and use this knowledge to optimise 
// the parts that are likely to be similar from one value to the next in the case of gradually increasing values
// double: "(m_lastHash << 5) ^ (value >> 50)"
// float:  "(m_lastHash << 5) ^ (value >> 21)"

namespace TimeSeriesDB.Internal
{

    /// <summary>
    ///     DFCM Predictor (Differential Finite Context Method).
    ///     Uses 1st order DFCM.
    /// </summary>
    public sealed class DfcmPredictor_Double {
        // must be a power of 2 to be efficient
        // if you set this to a value that is not a power of 2, you must change the 
        private const uint TABLE_SIZE = 128; // Debug.Assert((tableSize & (tableSize - 1)) == 0)

        private uint m_lastHash;
        private ulong m_lastValue;
        private readonly ulong[] m_table = new ulong[TABLE_SIZE];

        public DfcmPredictor_Double() {
            m_lastHash = 0;
            m_lastValue = 0;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong PredictNext() {
            return m_table[m_lastHash] + m_lastValue;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Update(ulong value) {
            var delta = value - m_lastValue;
            m_table[m_lastHash] = delta;
            // "& (TABLE_SIZE - 1)" is the same thing as "% TABLE_SIZE" for powers of 2
            m_lastHash = unchecked((uint)(((m_lastHash << 5) ^ (delta >> 50)) % TABLE_SIZE));
            m_lastValue = value;
        }
        public void Reset() {
            m_lastHash = 0;
            m_lastValue = 0;
            Array.Clear(m_table, 0, m_table.Length);
        }

        // usage:
        // --- write ---
        // var predicted = this.PredictNext();
        // var ulongDouble = BitMethods.ConvertToBits(doubleValue);
        // this.Update(ulongDouble);
        // var value = predicted ^ ulongDouble;
        // --- read ---
        // ulongDouble ^= this.PredictNext();
        // this.Update(ulongDouble);
        // return BitMethods.ConvertToDouble(ulongDouble);
    }

    /// <summary>
    ///     DFCM Predictor (Differential Finite Context Method).
    ///     Uses 1st order DFCM.
    /// </summary>
    public sealed class DfcmPredictor_Float {
        // must be a power of 2 to be efficient
        // if you set this to a value that is not a power of 2, you must change the 
        private const uint TABLE_SIZE = 128; // Debug.Assert((tableSize & (tableSize - 1)) == 0)

        private uint m_lastHash;
        private uint m_lastValue;
        private readonly uint[] m_table = new uint[TABLE_SIZE];

        public DfcmPredictor_Float() {
            m_lastHash = 0;
            m_lastValue = 0;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public uint PredictNext() {
            return m_table[m_lastHash] + m_lastValue;
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Update(uint value) {
            var delta = value - m_lastValue;
            m_table[m_lastHash] = delta;
            // "& (TABLE_SIZE - 1)" is the same thing as "% TABLE_SIZE" for powers of 2
            m_lastHash = ((m_lastHash << 5) ^ (delta >> 21)) % TABLE_SIZE;
            m_lastValue = value;
        }
        public void Reset() {
            m_lastHash = 0;
            m_lastValue = 0;
            Array.Clear(m_table, 0, m_table.Length);
        }

        // usage:
        // --- write ---
        // var predicted = this.PredictNext();
        // var uintFloat = BitMethods.ConvertToBits(floatValue);
        // this.Update(uintFloat);
        // var value = predicted ^ uintFloat;
        // --- read ---
        // uintFloat ^= this.PredictNext();
        // this.Update(uintFloat);
        // return BitMethods.ConvertToFloat(uintFloat);
    }
}
