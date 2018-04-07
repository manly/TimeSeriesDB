using System;
using System.Text;
using System.Diagnostics;


namespace TimeSeriesDB
{
    /// <summary>
    ///     Represent the max size individual items take in terms of bits.
    ///     Also supports variable size, which means every individual item will be preceded by its length.
    /// </summary>
    [DebuggerDisplay("{ToDebuggerDisplayString()}")]
    public struct BitSize : IComparable, IComparable<BitSize>, IEquatable<BitSize> {
        public static readonly BitSize VARIABLE = new BitSize();

        public readonly int? Bits;
        public bool IsVariable => !this.Bits.HasValue;

        #region constructors
        public BitSize(int? value) : this() {
            this.Bits = value;
        }
        public BitSize(string value) : this() {
            if(string.IsNullOrEmpty(value) ||
                string.Compare(value, "var", true) == 0 ||
                string.Compare(value, "null", true) == 0)
                this.Bits = null;
            else {
                if(!int.TryParse(value, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out int bits))
                    throw new FormatException($"Invalid bitsize format ({value}). Try an integer or {{'var','null'}} for variable length.");

                this.Bits = bits > 0 ? bits : (int?)null;
            }
        }
        #endregion

        #region static implicit operators
        public static implicit operator string(BitSize value) {
            return value.ToString();
        }
        public static implicit operator BitSize(string value) {
            return new BitSize(value);
        }
        public static implicit operator BitSize(int? value) {
            return new BitSize(value);
        }
        public static implicit operator int? (BitSize value) {
            return value.Bits;
        }
        public static bool operator ==(BitSize value1, BitSize value2) {
            return value1.CompareTo(value2) == 0;
        }
        public static bool operator !=(BitSize value1, BitSize value2) {
            return !(value1 == value2);
        }
        #endregion

        #region CompareTo()
        public int CompareTo(object obj) {
            return this.CompareTo((BitSize)obj);
        }
        public int CompareTo(BitSize other) {
            if(!this.Bits.HasValue)
                return !other.Bits.HasValue ? 0 : 1;
            if(!other.Bits.HasValue)
                return -1;

            return this.Bits.Value.CompareTo(other.Bits.Value);
        }
        #endregion
        #region Equals()
        public override bool Equals(object obj) {
            return this.CompareTo(obj) == 0;
        }
        public bool Equals(BitSize obj) {
            return this.CompareTo(obj) == 0;
        }
        #endregion
        #region GetHashCode()
        public override int GetHashCode() {
            return this.Bits.GetHashCode();
        }
        #endregion

        #region ToString()
        public override string ToString() {
            return this.Bits.HasValue ?
                this.Bits.Value.ToString(System.Globalization.CultureInfo.InvariantCulture) :
                "var";
        }
        #endregion
        #region private ToDebuggerDisplayString()
        private string ToDebuggerDisplayString() {
            var sb = new StringBuilder();
            sb.Append('{');
            if(this.Bits.HasValue) {
                sb.Append(this.Bits.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
                sb.Append(" bits");
            } else {
                sb.Append("variable_length bits");
            }
            sb.Append('}');
            return sb.ToString();
        }
        #endregion
    }
}
