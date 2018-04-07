using System;
using System.Runtime.CompilerServices;
using System.Collections.Generic;
using System.IO;


namespace TimeSeriesDB.DataStreams.Writers
{
    using BaseClasses;
    using Internal;

    /// <summary>
    ///     Stores efficiently a stream of uint values.
    ///     Will store either the top-most or bottom-most bytes, depending on the side having the most zeroes.
    ///     Does automatic RLE on zero values.
    /// </summary>
    /// <remarks>
    ///     Encoding format explained in Constants_UInt32Encoding.
    /// </remarks>
    public sealed class DataStreamWriter_UInt32 : StreamWriterBase, IDataStreamWriter<uint>, IResumableDataStreamWriter {
        private const int FLUSH_AFTER = BUFFER_SIZE - Constants_UInt32Encoding.MAX_ENCODE_FRAME_SIZE + 1;

        private uint m_prev;

        //void IDataStreamWriter.Init(IEnumerable<Stream> channels) {
        //    base.Init(channels);
        //}

        #region Write()
        public void Write(uint[] values, int offset, int count) {
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
                            if(++m_consecutiveZeroes >= Constants_UInt32Encoding.MAX_CONSECUTIVE_ZEROES_RLE) {
                                this.WriteByte(Constants_UInt32Encoding.EncodeConsecutiveZeroes(Constants_UInt32Encoding.MAX_CONSECUTIVE_ZEROES_RLE));
                                if(m_index >= FLUSH_AFTER)
                                    this.InternalFlush();

                                m_hasPrev = false;
                                m_consecutiveZeroes = 0;
                                break;
                            }
                        } else {
                            this.WriteByte(Constants_UInt32Encoding.EncodeConsecutiveZeroes(m_consecutiveZeroes));
                            if(m_index >= FLUSH_AFTER)
                                this.InternalFlush();

                            m_hasPrev = false;
                            m_consecutiveZeroes = 0;
                            break;
                        }
                    }
                    break; // normally wouldn't do a break here, but m_hasPrev=false or count==0
                }
            }

            while(count > 0) {
                // m_hasPrev == false
                m_prev = values[offset++];

                BitMethods.CountZeroBytes(m_prev, out BitMethods.Zeroes zeroes);
                //var leading_bytes = BitMethods.CountLeadingZeroBytes(m_prev);
                //var trailing_bytes = BitMethods.CountTrailingZeroBytes(m_prev);

                if(zeroes.TrailingZeroes <= zeroes.LeadingZeroes) {
                    var nbytes = 4 - zeroes.LeadingZeroes;
                    m_prevFlag = nbytes << Constants_UInt32Encoding.NBYTES_SHIFT; //m_prevFlag = nbytes;
                } else {
                    // high value & low precision values
                    // if value==0, can't make it here
                    var nbytes = 4 - zeroes.TrailingZeroes;
                    m_prevFlag = Constants_UInt32Encoding.LEADING_BIT_MASK | (nbytes << Constants_UInt32Encoding.NBYTES_SHIFT);//m_prevFlag = Constants_UInt32Encoding.LEADING_BIT_MASK | nbytes;
                }

                count--;

                if(m_prev == 0) {
                    bool skip = false;
                    m_consecutiveZeroes = 1;
                    // if we have zeroes, then count
                    while(count > 0) {
                        if(values[offset] == 0) {
                            offset++;
                            count--;
                            if(++m_consecutiveZeroes >= Constants_UInt32Encoding.MAX_CONSECUTIVE_ZEROES_RLE) {
                                this.WriteByte(Constants_UInt32Encoding.EncodeConsecutiveZeroes(Constants_UInt32Encoding.MAX_CONSECUTIVE_ZEROES_RLE));
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
                    if(m_consecutiveZeroes > 0) {
                        this.WriteByte(Constants_UInt32Encoding.EncodeConsecutiveZeroes(m_consecutiveZeroes));
                        if(m_index >= FLUSH_AFTER)
                            this.InternalFlush();

                        m_consecutiveZeroes = 0;
                        continue;
                    }
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

                BitMethods.CountZeroBytes(value, out zeroes);
                //leading_bytes = BitMethods.CountLeadingZeroBytes(value);
                //trailing_bytes = BitMethods.CountTrailingZeroBytes(value);

                int flag;
                if(zeroes.TrailingZeroes <= zeroes.LeadingZeroes) {
                    var nbytes = 4 - zeroes.LeadingZeroes;
                    flag = nbytes << Constants_UInt32Encoding.NBYTES_SHIFT; //flag = nbytes;
                } else {
                    // high value & low precision values
                    // if value==0, can't make it here
                    var nbytes = 4 - zeroes.TrailingZeroes;
                    flag = Constants_UInt32Encoding.LEADING_BIT_MASK | (nbytes << Constants_UInt32Encoding.NBYTES_SHIFT); //flag = Constants_UInt32Encoding.LEADING_BIT_MASK | nbytes;
                }

                // encode as 2 normal values
                var flags = (m_prevFlag << 4) | flag;

                this.WriteByte(unchecked((byte)flags));
                this.WriteValue(m_prev, unchecked((byte)m_prevFlag));
                this.WriteValue(value, unchecked((byte)flag));

                if(m_index >= FLUSH_AFTER)
                    this.InternalFlush();

                count--;
            }
        }
        public void Write(uint value) {
            BitMethods.CountZeroBytes(value, out BitMethods.Zeroes zeroes);
            //var leading_bytes  = BitMethods.CountLeadingZeroBytes(value);
            //var trailing_bytes = BitMethods.CountTrailingZeroBytes(value);

            int nbytes;
            int flag;
            if(zeroes.TrailingZeroes <= zeroes.LeadingZeroes) {
                if(value != 0) {
                    nbytes = 4 - zeroes.LeadingZeroes;
                    flag = nbytes << Constants_UInt32Encoding.NBYTES_SHIFT; //flag = nbytes;

                    // if we had a streak of zeroes, but we just broke it, then we need to encode the zeroes
                    if(m_hasPrev && m_consecutiveZeroes > 0) {
                        this.WriteByte(Constants_UInt32Encoding.EncodeConsecutiveZeroes(m_consecutiveZeroes));
                        if(m_index >= FLUSH_AFTER)
                            this.InternalFlush();

                        m_hasPrev = false;
                        //m_consecutiveZeroes = 0;
                    }

                    // allow to continue 
                } else {
                    if(m_hasPrev) {
                        if(m_consecutiveZeroes != 0) {
                            if(++m_consecutiveZeroes >= Constants_UInt32Encoding.MAX_CONSECUTIVE_ZEROES_RLE) {
                                this.WriteByte(Constants_UInt32Encoding.EncodeConsecutiveZeroes(Constants_UInt32Encoding.MAX_CONSECUTIVE_ZEROES_RLE));
                                if(m_index >= FLUSH_AFTER)
                                    this.InternalFlush();

                                m_hasPrev = false;
                                m_consecutiveZeroes = 0;
                            }
                        } else {
                            // special case : first value is non-zero and the current value is a zero
                            this.WriteByte(unchecked((byte)(m_prevFlag << 4))); // | 0x00 (to signal a zero on 2nd item)
                            this.WriteValue(m_prev, unchecked((byte)m_prevFlag));
                            //this.WriteValue(value, unchecked((byte)flag)); // write a 'zero'

                            if(m_index >= FLUSH_AFTER)
                                this.InternalFlush();

                            m_consecutiveZeroes = 1;
                        }
                    } else { // !m_hasPrev && value==0
                        m_hasPrev = true;
                        m_consecutiveZeroes = 1;
                    }
                    return;
                }
            } else {
                // high value & low precision values
                // if value==0, can't make it here

                nbytes = 4 - zeroes.TrailingZeroes;
                flag = Constants_UInt32Encoding.LEADING_BIT_MASK | (nbytes << Constants_UInt32Encoding.NBYTES_SHIFT); //flag = Constants_UInt32Encoding.LEADING_BIT_MASK | nbytes;

                // if were breaking a zeroes streak
                if(m_hasPrev && m_consecutiveZeroes > 0) {
                    this.WriteByte(unchecked((byte)(Constants_UInt64Encoding.SIGNAL_REPEATING_ZEROES | (m_consecutiveZeroes - 1))));
                    if(m_index >= FLUSH_AFTER)
                        this.InternalFlush();

                    m_hasPrev = false;
                    //m_consecutiveZeroes = 0;
                }

                // allow to continue 
            }

            if(!m_hasPrev) {
                m_hasPrev = true;
                m_prevFlag = flag;
                m_prev = value;
                m_consecutiveZeroes = 0;
            } else {
                m_hasPrev = false;
                this.WriteByte(unchecked((byte)((m_prevFlag << 4) | flag)));
                this.WriteValue(m_prev, unchecked((byte)m_prevFlag));
                this.WriteValue(value, unchecked((byte)flag));

                if(m_index >= FLUSH_AFTER)
                    this.InternalFlush();

                //m_consecutiveZeroes = 0;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(Array values, int offset, int count) {
            this.Write((uint[])values, offset, count);
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Write(object value) {
            this.Write((uint)value);
        }
        #endregion
        #region Commit()
        /// <summary>
        ///     Commits the remaining values unto the stream.
        /// </summary>
        public override void Commit() {
            if(!m_hasPrev) {
                //m_consecutiveZeroes = 0;
                return;
            }
            
            if(m_consecutiveZeroes <= 1) {
                // special case, indicate that we have incomplete data
                // this will cause the reads to skip the empty entry
                // later implementation can take care of trying to re-use the flag byte, 
                // but for now we dont
                var flags = (m_prevFlag << 4) | Constants_UInt32Encoding.SIGNAL_SINGLE_ITEM;

                this.WriteByte(unchecked((byte)flags));
                this.WriteValue(m_prev, unchecked((byte)m_prevFlag));
            } else {
                // works for 2+ consecutive zeroes
                this.WriteByte(Constants_UInt32Encoding.EncodeConsecutiveZeroes(m_consecutiveZeroes));
                //m_consecutiveZeroes = 0;
            }

            if(m_index >= FLUSH_AFTER)
                this.InternalFlush();

            m_hasPrev = false;
        }
        #endregion
        #region CreateReader()
        public Readers.IDataStreamReader CreateReader() {
            return new Readers.DataStreamReader_UInt32();
        }
        #endregion

        #region CalculateEncodedLength()
        public int CalculateEncodedLength() {
            if(!m_hasPrev)
                return m_index;
            else if(m_consecutiveZeroes <= 1)
                return m_index + 1 + (m_prevFlag & Constants_UInt32Encoding.NBYTES_BIT_MASK);
            else
                return m_index + 1;
        }
        #endregion

        #region private WriteValue()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteValue(uint value, byte flag) {
            var nbytes = ((flag & Constants_UInt32Encoding.NBYTES_BIT_MASK) >> Constants_UInt32Encoding.NBYTES_SHIFT); //var nbytes = (flag & Constants_UInt32Encoding.NBYTES_BIT_MASK) + 1;

            // if leading
            if((flag & Constants_UInt32Encoding.LEADING_BIT_MASK) != 0)
                value >>= (4 - nbytes) * 8;

            base.WriteUInt32(value, nbytes);
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
