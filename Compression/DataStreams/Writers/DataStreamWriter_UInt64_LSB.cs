using System;
using System.IO;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;


namespace TimeSeriesDB.DataStreams.Writers
{
    using BaseClasses;
    using Internal;

    /// <summary>
    ///     Stores efficiently a stream of ulong values whose content is stored in LSB format (least significant bytes).
    ///     Will cut the most significant bytes that are zeroes.
    ///     Does automatic RLE on zero values.
    ///     Use SignedToUnsigned() for efficient negative values encoding.
    /// </summary>
    /// <remarks>
    ///     Encoding format explained in Constants_UInt64Encoding_LSB.
    /// </remarks>
    public sealed class DataStreamWriter_UInt64_LSB : StreamWriterBase, IDataStreamWriter<ulong>, IResumableDataStreamWriter {
        private const int FLUSH_AFTER = BUFFER_SIZE - Constants_UInt64Encoding_LSB.MAX_ENCODE_FRAME_SIZE + 1;

        private ulong m_prev;

        //void IDataStreamWriter.Init(IEnumerable<Stream> channels) {
        //    base.Init(channels);
        //}

        #region Write()
        /// <summary>
        ///     Adds values to the stream.
        ///     Considerably faster than Write(item).
        /// </summary>
        public void Write(ulong[] values, int offset, int count) {
            // align pairs
            while(m_hasPrev && count > 0) {
                if(m_consecutiveZeroes == 0) {
                    this.Write(values[offset++]);
                    count--;
                } else {
                    // if we have zeroes, then finish up that pair
                    while(count > 0) {
                        if(values[offset] == 0) {
                            offset++;
                            count--;
                            if(++m_consecutiveZeroes >= Constants_UInt64Encoding_LSB.MAX_CONSECUTIVE_ZEROES_RLE) {
                                this.WriteByte(Constants_UInt64Encoding_LSB.EncodeConsecutiveZeroes(Constants_UInt64Encoding_LSB.MAX_CONSECUTIVE_ZEROES_RLE));
                                if(m_index >= FLUSH_AFTER)
                                    this.InternalFlush();

                                m_hasPrev = false;
                                m_consecutiveZeroes = 0; // necessary?
                                break;
                            }
                        } else {
                            if(m_consecutiveZeroes >= 2) {
                                this.WriteByte(Constants_UInt64Encoding_LSB.EncodeConsecutiveZeroes(m_consecutiveZeroes));
                                if(m_index >= FLUSH_AFTER)
                                    this.InternalFlush();

                                m_hasPrev = false;
                                m_consecutiveZeroes = 0; // necessary?
                            } else {
                                this.Write(values[offset++]);
                                count--;
                            }
                            break;
                        }
                    }
                }
            }

            while(count > 0) {
                // m_hasPrev == false
                m_prev = values[offset++];
                m_prevFlag = 8 - BitMethods.CountLeadingZeroBytes(m_prev);

                count--;

                if(m_prevFlag == 0) { // same check as "m_prev == 0"
                    bool skip = false;
                    m_consecutiveZeroes = 1;
                    // if we have zeroes, then count
                    while(count > 0) {
                        if(values[offset] == 0) {
                            offset++;
                            count--;
                            if(++m_consecutiveZeroes >= Constants_UInt64Encoding_LSB.MAX_CONSECUTIVE_ZEROES_RLE) {
                                this.WriteByte(Constants_UInt64Encoding_LSB.EncodeConsecutiveZeroes(Constants_UInt64Encoding_LSB.MAX_CONSECUTIVE_ZEROES_RLE));
                                if(m_index >= FLUSH_AFTER)
                                    this.InternalFlush();

                                m_consecutiveZeroes = 0;
                                skip = true;
                                break;
                            }
                        } else
                            break;
                    }
                    // if we are done processing the current array, then wait to see if theres more zeroes coming next
                    if(count == 0) {
                        m_hasPrev = true;
                        break;
                    }
                    if(skip)
                        continue;
                    // encode zeroes
                    if(m_consecutiveZeroes >= 2) {
                        this.WriteByte(Constants_UInt64Encoding_LSB.EncodeConsecutiveZeroes(m_consecutiveZeroes));
                        if(m_index >= FLUSH_AFTER)
                            this.InternalFlush();

                        m_consecutiveZeroes = 0;
                        continue;
                    }
                    // if theres just one zero, then code the next value normally (ie: 0x0?)
                }

                // at this point:
                // m_hasPrev == false (but logically equivalent to true)
                // m_prev = prev value
                // m_prevFlag = prev length
                // and there is no RLE encoding possible

                if(count == 0) {
                    // then make the next commit functional
                    m_hasPrev = true;
                    //m_consecutiveZeroes = 0; // not sure if needed
                    break;
                }

                var value = values[offset++];
                var flag = 8 - BitMethods.CountLeadingZeroBytes(value);

                // encode as 2 normal values
                var flags = (m_prevFlag << 4) | flag;

                this.WriteByte(unchecked((byte)flags));
                this.WriteUInt64(m_prev, m_prevFlag);
                this.WriteUInt64(value, flag);

                if(m_index >= FLUSH_AFTER)
                    this.InternalFlush();

                count--;
            }
        }
        public void Write(ulong value) {
            int flag = 8 - BitMethods.CountLeadingZeroBytes(value);

            if(!m_hasPrev) {
                m_prev = value;
                m_prevFlag = flag;
                m_hasPrev = true;

                m_consecutiveZeroes = flag == 0 ? 1 : 0;
            } else {
                if(m_consecutiveZeroes == 0) {
                    var flags = (m_prevFlag << 4) | flag;

                    this.WriteByte(unchecked((byte)flags));
                    this.WriteUInt64(m_prev, m_prevFlag);
                    this.WriteUInt64(value, flag);

                    if(m_index >= FLUSH_AFTER)
                        this.InternalFlush();

                    m_hasPrev = false;
                } else { // m_hasPrev && m_consecutiveZeroes > 0
                    if(flag == 0) { // same check as "value == 0"
                        if(++m_consecutiveZeroes >= Constants_UInt64Encoding_LSB.MAX_CONSECUTIVE_ZEROES_RLE) {
                            this.WriteByte(Constants_UInt64Encoding_LSB.EncodeConsecutiveZeroes(Constants_UInt64Encoding_LSB.MAX_CONSECUTIVE_ZEROES_RLE));
                            if(m_index >= FLUSH_AFTER)
                                this.InternalFlush();

                            m_hasPrev = false;
                        }
                    } else { // m_hasPrev && m_consecutiveZeroes > 0 && flag != 0
                        if(m_consecutiveZeroes == 1) {
                            var flags = (m_prevFlag << 4) | flag;

                            this.WriteByte(unchecked((byte)flags));
                            this.WriteUInt64(m_prev, m_prevFlag);
                            this.WriteUInt64(value, flag);

                            if(m_index >= FLUSH_AFTER)
                                this.InternalFlush();

                            m_hasPrev = false;
                        } else { // m_hasPrev && m_consecutiveZeroes >= 2 && flag != 0
                            // works for 2+ consecutive zeroes
                            this.WriteByte(Constants_UInt64Encoding_LSB.EncodeConsecutiveZeroes(m_consecutiveZeroes));

                            if(m_index >= FLUSH_AFTER)
                                this.InternalFlush();

                            m_prev = value;
                            m_prevFlag = flag;
                            //m_hasPrev = true;
                            m_consecutiveZeroes = 0;
                        }
                    }
                }
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((ulong[])values, offset, count);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(object value) {
            this.Write((ulong)value);
        }
        #endregion
        #region Commit()
        /// <summary>
        ///     Commits the remaining values unto the stream.
        /// </summary>
        public override void Commit() {
            // if already committed
            if(!m_hasPrev) {
                //m_consecutiveZeroes = 0;
                return;
            }

            if(m_consecutiveZeroes <= 1) {
                // special case, indicate that we have incomplete data
                // this will cause the reads to skip the empty entry
                // later implementation can take care of trying to re-use the flag byte, 
                // but for now we dont
                var flags = (m_prevFlag << 4) | Constants_UInt64Encoding_LSB.SIGNAL_SINGLE_ITEM;

                this.WriteByte(unchecked((byte)flags));
                this.WriteUInt64(m_prev, m_prevFlag);
            } else {
                // works for 2+ consecutive zeroes
                this.WriteByte(Constants_UInt64Encoding_LSB.EncodeConsecutiveZeroes(m_consecutiveZeroes));
                //m_consecutiveZeroes = 0;
            }

            if(m_index >= FLUSH_AFTER)
                this.InternalFlush();

            m_hasPrev = false;
        }
        #endregion
        #region CreateReader()
        public Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_UInt64_LSB();
        }
        #endregion

        #region CalculateEncodedLength()
        public int CalculateEncodedLength() {
            if(!m_hasPrev)
                return m_index;
            else if(m_consecutiveZeroes <= 1)
                return m_index + 1 + m_prevFlag;
            else
                return m_index + 1;
        }
        #endregion

        #region Resume()
        public void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount) {
            this.ResumeByRebuilding(resumableChannels, rowCount, ref m_prev);

            #region invalid code - more efficient but requires knowing the uncompressed size
            //var channelsBackup = resumableChannels.ToList();
            //this.Init(channelsBackup.Select(o => o.WriteOnly));
            //
            //// need to recompress in case new data affects existing bytes
            //foreach(var channel in channelsBackup)
            //    channel.ReadOnly.CopyTo(channel.WriteOnly);
            #endregion
        }
        #endregion
    }
}
