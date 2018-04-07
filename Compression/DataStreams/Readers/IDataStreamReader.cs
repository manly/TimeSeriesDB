using System;
using System.IO;
using System.Collections.Generic;


namespace TimeSeriesDB.DataStreams.Readers 
{
    /// <summary>
    ///     Represents a class that deserializes items of the same type indefinitely on a stream.
    /// </summary>
    public interface IDataStreamReader {
        /// <summary>
        ///     Optional. The number of items in total that can be read.
        ///     This is necessary because channels may be compressed, and compressed streams return padded buffers when requesting past encoded length.
        ///     
        ///     Default: ulong.MaxValue (to ignore).
        /// </summary>
        ulong ItemCount { get; set; }
        /// <summary>
        ///     The number of channels the data stream uses.
        ///     This is used when calling Init().
        /// </summary>
        int ChannelCount { get; }
        /// <param name="channels">.Count() = ChannelCount</param>
        void Init(IEnumerable<Stream> channels);

        int Read(Array items, int offset, int count);
        object ReadOne();

        /// <summary>
        ///     Skips n entries, until end of stream reached.
        /// </summary>
        void Skip(int count);

        Writers.IDataStreamWriter CreateWriter();
    }

    /// <summary>
    ///     Represents a class that deserializes TItemTypes indefinitely on a stream.
    /// </summary>
    public interface IDataStreamReader<TItemType> : IDataStreamReader {
        int Read(TItemType[] items, int offset, int count);
        new TItemType ReadOne();
    }
}