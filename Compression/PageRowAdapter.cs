using System;
using System.IO;
using System.Text;
using System.Runtime.CompilerServices;


namespace TimeSeriesDB
{
    using DataStreams.BaseClasses;
    using Internal;
    using IO;
    
    /// <summary>
    ///     Reads/Writes TRow rows unto a Page.
    /// </summary>
    public abstract class PageRowAdapter<TRow> {
        // should be a multiple of FRAMESIZE in case of any deltadelta encoding anywhere
        // round down to framesize boundaries
        protected const int BUFFER_SIZE = (4096 / sizeof(long) / Constants_DeltaDelta.FRAME_SIZE) * Constants_DeltaDelta.FRAME_SIZE;

        #region Page
        private Page m_page = null;
        public Page Page {
            get => m_page;
            set {
                if(value == m_page)
                    return;
                if(m_page != null)
                    throw new InvalidOperationException();

                m_page = value ?? throw new ArgumentNullException(nameof(value));
                m_remainingRowCount = value.RowCount;
            }
        }
        #endregion

        private int m_index = 0;
        private int m_read = 0;
        private ulong m_remainingRowCount = 0;
        private readonly TRow[] m_buffer = new TRow[BUFFER_SIZE];

        /// <summary>
        ///     Read the buffer from Page.Columns.
        ///     Count will never exceed BUFFER_SIZE.
        /// </summary>
        protected abstract int InternalReadRows(TRow[] buffer, int offset, int count);
        /// <summary>
        ///     Write the buffer into the Page.Columns.
        ///     Count will never exceed BUFFER_SIZE.
        /// </summary>
        protected abstract void InternalWriteRows(TRow[] buffer, int offset, int count);

        #region Write()
        public void Put(TRow[] values, int offset, int count) {
            // try to align
            while(m_index != 0 && count > 0) {
                var write = Math.Min(count, BUFFER_SIZE - m_index);

                Array.Copy(values, offset, m_buffer, m_index, write);

                offset += write;
                m_index += write;
                count -= write;

                if(m_index == BUFFER_SIZE) {
                    m_index = 0;
                    this.WriteRows(m_buffer, 0, BUFFER_SIZE);
                }
            }

            // write by chunks of page_size
            while(count >= BUFFER_SIZE) {
                this.WriteRows(values, offset, BUFFER_SIZE);

                offset += BUFFER_SIZE;
                count -= BUFFER_SIZE;
            }

            // accumulate remaining
            while(count > 0) {
                var write = Math.Min(count, BUFFER_SIZE - m_index);

                Array.Copy(values, offset, m_buffer, m_index, write);

                offset += write;
                m_index += write;
                count -= write;

                if(m_index == BUFFER_SIZE) {
                    m_index = 0;
                    this.WriteRows(m_buffer, 0, BUFFER_SIZE);
                }
            }
        }
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Put(TRow value) {
            m_buffer[m_index++] = value;

            if(m_index == BUFFER_SIZE) {
                m_index = 0;
                this.WriteRows(m_buffer, 0, BUFFER_SIZE);
            }
        }
        #endregion
        #region Commit()
        public void Commit() {
            if(m_index > 0) {
                this.WriteRows(m_buffer, 0, m_index);
                m_index = 0;
            }
        }
        #endregion
        #region Reset()
        public void Reset() {
            //this.Page.Reset();
            m_index = 0;
            //m_read = 0;
            //m_remainingRowCount = 0;
        }
        #endregion

        #region Read()
        public int Read(TRow[] items, int offset, int count) {
            int total = 0;

            // try to finish read buffer
            while(m_index != m_read && count > 0) {
                var read = Math.Min(count, m_read - m_index);

                Array.Copy(m_buffer, m_index, items, offset, read);

                offset += read;
                m_index += read;
                count -= read;
                total += read;
                //m_remainingRowCount -= unchecked((ulong)read);
            }

            // read by chunks of page_size
            while(m_index == m_read && count >= BUFFER_SIZE && m_remainingRowCount >= BUFFER_SIZE) {
                var read = this.InternalReadRows(items, 0, BUFFER_SIZE);

                offset += read;
                count -= read;
                total += read;
                m_remainingRowCount -= unchecked((ulong)read);
            }

            while(count > 0) {
                if(!this.RefreshBuffer())
                    return total;
                
                var read = Math.Min(count, m_read - m_index);

                Array.Copy(m_buffer, m_index, items, offset, read);

                offset += read;
                m_index += read;
                count -= read;
                total += read;
                //m_remainingRowCount -= unchecked((ulong)read);
            }

            return total;
        }
        #endregion
        #region ReadOne()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TRow ReadOne() {
            if(!this.RefreshBuffer())
                return default;
            
            return m_buffer[m_index++];
        }
        #endregion

        #region private void RefreshBuffer()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool RefreshBuffer() {
            if(m_index == m_read) {
                if(m_remainingRowCount > 0) {
                    var readTemp = this.InternalReadRows(m_buffer, 0, unchecked((int)Math.Min(BUFFER_SIZE, m_remainingRowCount)));
                
                    m_index = 0;
                    m_read = readTemp;
                    m_remainingRowCount -= unchecked((ulong)readTemp);
                } else
                    return false;
            }
            return true;
        }
        #endregion
        #region private void WriteRows()
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteRows(TRow[] buffer, int offset, int count) {
            this.Page.RowCount += unchecked((uint)count);

            this.InternalWriteRows(buffer, offset, count);
        }
        #endregion
    }
}
