using System;
using System.IO;
using System.Text;


namespace TimeSeriesDB
{
    using IO;
    using Internal;
    using DataStreams.Readers;
    using DataStreams.Writers;

    public sealed class Column {
        public ColumnDefinition Definition { get; private set; }

        public IDataStreamReader ReadOnly { get; private set; }
        public IDataStreamWriter WriteOnly { get; private set; }

        #region constructors
        public Column(ColumnDefinition definition) {
            this.Definition = definition ?? throw new ArgumentNullException(nameof(definition));
        }
        #endregion

        #region internal Load()
        internal void Load(MultiChannelStream channels, int channel_id, ulong itemCount) {
            this.ReadOnly = this.Definition.CreateReader(channels, channel_id, itemCount); 
            this.WriteOnly = null;
        }
        #endregion
        #region internal Create()
        internal void Create(MultiChannelStream channels, int channel_id) {
            this.ReadOnly = null;
            this.WriteOnly = this.Definition.CreateWriter(channels, channel_id);
        }
        #endregion
        #region internal Reset()
        internal void Reset() {
            this.WriteOnly.Reset();
        }
        #endregion

        #region ToString()
        public override string ToString() {
            return string.Format("{0} ({1})", this.Definition, this.ReadOnly != null ? "read" : (this.WriteOnly != null ? "write" : "no read/write"));
        }
        #endregion
    }
}
