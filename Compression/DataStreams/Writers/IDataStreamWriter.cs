using System;
using System.IO;
using System.Collections.Generic;


namespace TimeSeriesDB.DataStreams.Writers
{
    /// <summary>
    ///     Represents a class that serializes items of the same type indefinitely on a stream.
    /// </summary>
    public interface IDataStreamWriter {
        /// <summary>
        ///     Makes sure that even half-entered data gets put on the buffer even if incomplete.
        /// </summary>
        void Commit();
        void Reset();
        /// <summary>
        ///     Writes buffers to channels.
        /// </summary>
        void Flush();

        /// <summary>
        ///     The number of channels the data stream uses.
        ///     This is used when calling Init().
        /// </summary>
        int ChannelCount { get; }
        /// <param name="channels">.Count() = ChannelCount</param>
        void Init(IEnumerable<Stream> channels);
        
        void Write(Array values, int offset, int count);
        void Write(object value);

        Readers.IDataStreamReader CreateReader();
    }

    /// <summary>
    ///     Represents a class that serializes TItemTypes indefinitely on a stream.
    /// </summary>
    public interface IDataStreamWriter<TItemType> : IDataStreamWriter {
        void Write(TItemType[] values, int offset, int count);
        void Write(TItemType value);
    }
}