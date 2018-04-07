using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Runtime.CompilerServices;


namespace TimeSeriesDB.DataStreams.Writers
{
    /// <summary>
    ///     Represents a class that supports resuming writing (ie: append).
    ///     This only applies to non-compressed streams/channels.
    /// </summary>
    public interface IResumableDataStreamWriter {
        /// <summary>
        ///     Sets up the writer in append-mode, jumping to the end of stream.
        ///     This call replaces Init().
        ///     This must be called prior to any write being done.
        /// </summary>
        /// <param name="channels">.Count() = ChannelCount</param>
        /// <param name="rowCount">The total number of items stored in the stream. Do not pass the index at which you expect to resume; this info is required in order to resume the stream.</param>
        void Resume(IEnumerable<Stream> channels, long rowCount);
        /// <summary>
        ///     Sets up the writer in append-mode, jumping to the end of stream.
        ///     This call replaces Init().
        ///     This must be called prior to any write being done.
        /// </summary>
        /// <param name="resumableChannels">.Count() = ChannelCount</param>
        /// <param name="rowCount">The total number of items stored in the stream. Do not pass the index at which you expect to resume; this info is required in order to resume the stream.</param>
        void Resume(IEnumerable<ResumableChannel> resumableChannels, long rowCount);
    }

    public class ResumableChannel {
        public Stream ReadOnly;
        public Stream WriteOnly;
    }
}

namespace TimeSeriesDB
{
    using IO;

    public static partial class Extensions {
        /// <summary>
        ///     Sets up the writer in append-mode, jumping to the end of stream.
        ///     This call replaces Init().
        ///     This must be called prior to any write being done.
        /// </summary>
        /// <param name="channels">.Count() = ChannelCount</param>
        /// <param name="rowCount">The total number of items stored in the stream. Do not pass the index at which you expect to resume; this info is required in order to resume the stream.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Resume(this DataStreams.Writers.IResumableDataStreamWriter source, IEnumerable<Stream> channels, long rowCount, CompressionSetting compressionSetting) {
            if(compressionSetting == null)
                compressionSetting = CompressionSetting.None;

            var channelsBackup = channels.ToList();

            // if no compress, call the normal resume
            if(compressionSetting.Algorithm == CompressionAlgorithm.NoCompress) {
                source.Resume(channelsBackup, rowCount);
                return;
            }

            var newChannels = new List<DataStreams.Writers.ResumableChannel>(channelsBackup.Count);
        
            foreach(var channel in channelsBackup) {
                var manager = new ResumableStreamManager(channel);
                
                var compressed = new DataStreams.Writers.ResumableChannel() {
                    ReadOnly  = compressionSetting.CreateReadStream(manager.ReadStream),
                    WriteOnly = compressionSetting.CreateWriteStream(manager.WriteStream),
                };
                
                newChannels.Add(compressed);
            }
        
            source.Resume(newChannels, rowCount);
        }

        /// <summary>
        ///     Rebuilds the entire stream.
        ///     This basically calls Read() on all items, and then Write().
        ///     
        ///     This method is necessary when we do not know the size of the uncompressed data (due to say, variable-sized items/encodings).
        ///     It is required because doing old_compressed_stream.read() -> new_compressed_stream().write() as the compressed stream does not know
        ///     how to interpret the final compressed byte (how many decompressed bytes it contains). Furthermore, since writing is append-only, 
        ///     the compressed streams simply cannot have this info ahead of time, resulting in channel read()/write() code being generally incorrect.
        ///     This method solves this issue by basically rebuilding the entire stream.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ResumeByRebuilding<T>(this DataStreams.Writers.IResumableDataStreamWriter source, IEnumerable<DataStreams.Writers.ResumableChannel> resumableChannels, long rowCount) {
            var source2 = source as DataStreams.Writers.IDataStreamWriter;
            var channelsBackup = resumableChannels.ToList();
            var reader = source2.CreateReader();

            reader.Init(channelsBackup.Select(o => o.ReadOnly));
            source2.Init(channelsBackup.Select(o => o.WriteOnly));
            
            const int BUFFER_SIZE = 4096;
            var size = unchecked((int)Math.Min(rowCount, BUFFER_SIZE));
            var buffer = new T[size];

            while(rowCount > 0) {
                int request = unchecked((int)Math.Min(rowCount, BUFFER_SIZE));
                int read = reader.Read(buffer, 0, request);
                if(read == 0)
                    break;
                source2.Write(buffer, 0, read);
                rowCount -= read;
            }
        }
        /// <summary>
        ///     Rebuilds the entire stream.
        ///     This basically calls Read() on all items, and then Write().
        ///     
        ///     This method is necessary when we do not know the size of the uncompressed data (due to say, variable-sized items/encodings).
        ///     It is required because doing old_compressed_stream.read() -> new_compressed_stream().write() as the compressed stream does not know
        ///     how to interpret the final compressed byte (how many decompressed bytes it contains). Furthermore, since writing is append-only, 
        ///     the compressed streams simply cannot have this info ahead of time, resulting in channel read()/write() code being generally incorrect.
        ///     This method solves this issue by basically rebuilding the entire stream.
        /// </summary>
        /// <param name="m_prev">not used -- only there for automatic <typeparamref name="T"/>.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void ResumeByRebuilding<T>(this DataStreams.Writers.IResumableDataStreamWriter source, IEnumerable<DataStreams.Writers.ResumableChannel> resumableChannels, long rowCount, ref T m_prev) {
            ResumeByRebuilding<T>(source, resumableChannels, rowCount);
        }
    }
}