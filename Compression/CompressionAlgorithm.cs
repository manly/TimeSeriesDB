using System;


namespace TimeSeriesDB
{
    public enum CompressionAlgorithm {
        NoCompress,
        /// <summary>
        ///     Uses the .NET provided compression (LZ77).
        ///     Be aware that this is like 16x slower than zstd, and gives worse results in every case.
        ///     It is only included in case there are absolutely no external libraries allowed.
        ///     Uses System.IO.Compression.GZipStream. Includes signature+hash.
        ///     Default System.IO.Compression.CompressionLevel = Fastest.
        /// </summary>
        zlib,
        /// <summary>
        ///     Uses LZ4 (https://github.com/lz4/lz4).
        ///     This compression is meant for real-time compression.
        ///     Provides lower compression ratio (about 33-50% less than zstd) but at 4-5x the speed of zstd.
        ///     This is used by most storage engines.
        ///     Uses Lz4Net.Lz4CompressionStream.
        ///     Default Lz4Net.Lz4Mode = Fast.
        /// </summary>
        lz4,
        /// <summary>
        ///     Uses zstandard (http://facebook.github.io/zstd/).
        ///     This format is meant for more current processor architectures and yields significant performance improvements (10-16x speed, size too) over zlib.
        ///     Uses Zstandard.Net.ZstandardStream. Includes signature+hash.
        ///     Default level = 3.
        /// </summary>
        zstd,
    }
}
