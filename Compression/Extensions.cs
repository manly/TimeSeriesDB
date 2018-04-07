using System;
using System.IO;
using System.Text;
using System.Linq;
using System.Collections;
using System.Collections.Generic;


namespace TimeSeriesDB
{
    using Internal;
    using IO;
    
    public static partial class Extensions {
        #region static Page.GetChannels()
        /// <summary>
        ///     Returns the channels used internally by a given column.
        /// </summary>
        public static IEnumerable<MultiChannelStream.ChannelStream> GetChannels(this Page page, Column column) {
            int column_count = page.Columns.Count;
            int channel_id = Page.RESERVED_CHANNELS;

            for(int i = 0; i < column_count; i++) {
                var current = page.Columns[i];
                int channel_count;

                switch(page.Mode) {
                    case Page.PageMode.Read:
                        channel_count = current.ReadOnly.ChannelCount;
                        break;
                    case Page.PageMode.Write:
                        channel_count = current.WriteOnly.ChannelCount;
                        break;
                    default:
                        throw new NotImplementedException();
                }

                if(current == column) {
                    for(int j = 0; j < channel_count; j++)
                        yield return page.Channels.List.ElementAt(channel_id + j);
                    yield break;
                }

                channel_id += channel_count;
            }
        }
        #endregion
        #region static Page.GetColumn()
        /// <summary>
        ///     Returns the column using the given channel.
        /// </summary>
        public static Column GetColumn(this Page page, MultiChannelStream.ChannelStream channel) {
            int column_count = page.Columns.Count;
            int channel_id = Page.RESERVED_CHANNELS;

            for(int i = 0; i < column_count; i++) {
                var current = page.Columns[i];
                int channel_count;

                switch(page.Mode) {
                    case Page.PageMode.Read:
                        channel_count = current.ReadOnly.ChannelCount;
                        break;
                    case Page.PageMode.Write:
                        channel_count = current.WriteOnly.ChannelCount;
                        break;
                    default:
                        throw new NotImplementedException();
                }

                channel_id += channel_count;

                if(channel.ID < channel_id)
                    return current;
            }

            return null;
        }
        #endregion

        #region static Page.AddColumn()
        /// <summary>
        ///     Adds a new column to the page.
        ///     This will generate a new page.
        /// </summary>
        /// <param name="rows">Must contain RowCount entries from offset.</param>
        public static void AddColumn(this Page source, Stream destination, ColumnDefinition new_column, Array rows, int offset) {
            if(rows.LongLength - (long)offset < (long)source.RowCount)
                throw new ArgumentOutOfRangeException(nameof(rows));
            
            var destSerie = new DataSerieDefinition(source.SerieDefinition, source.SerieDefinition.Columns.Concat(new[] { new_column }));
            using(var destinationPage = Page.CreateNew(destSerie, destination)) {
                foreach(var column in source.Columns)
                    CopyColumnData(source, destinationPage, column.Definition);

                // write the new column rows
                var newColumnWriter = destinationPage.Columns[destinationPage.Columns.Count - 1].WriteOnly;
                newColumnWriter.Write(rows, offset, (int)source.RowCount);
                newColumnWriter.Commit();
                newColumnWriter.Flush();

                destinationPage.RowCount = source.RowCount;
                destinationPage.SaveRowCount();

                // have to close the page because we dont support resuming pages yet, meaning the buffers/compression streams 
                // would not work for all previous columns
                destinationPage.Close();
            }
        }
        #endregion
        #region static Page.RemoveColumn()
        /// <summary>
        ///     Removes a column from the page.
        ///     This will generate a new page.
        /// </summary>
        public static void RemoveColumn(this Page source, Stream destination, ColumnDefinition remove_column) {
            var destSerie = new DataSerieDefinition(source.SerieDefinition, source.SerieDefinition.Columns.Where(o => o != remove_column));
            using(var destinationPage = Page.CreateNew(destSerie, destination)) {
                foreach(var column in destinationPage.Columns)
                    CopyColumnData(source, destinationPage, column.Definition);

                destinationPage.RowCount = source.RowCount;
                destinationPage.SaveRowCount();

                // have to close the page because we dont support resuming pages yet, meaning the buffers/compression streams 
                // would not work for all previous columns
                destinationPage.Close();
            }
        }
        #endregion
        #region static Page.ReorderColumns()
        /// <summary>
        ///     Re-orders the columns of the page.
        ///     This will generate a new page.
        /// </summary>
        public static void ReorderColumns(this Page source, Stream destination, ColumnDefinition[] columns) {
            var destSerie = new DataSerieDefinition(source.SerieDefinition, columns);
            using(var destinationPage = Page.CreateNew(destSerie, destination)) {
                foreach(var column in destinationPage.Columns)
                    CopyColumnData(source, destinationPage, column.Definition);

                destinationPage.RowCount = source.RowCount;
                destinationPage.SaveRowCount();

                // have to close the page because we dont support resuming pages yet, meaning the buffers/compression streams 
                // would not work for all previous columns
                destinationPage.Close();
            }
        }
        #endregion
        #region static Page.ChangeColumnDataType()
        /// <summary>
        ///     Changes a column datatype on the page.
        ///     This will generate a new page.
        /// </summary>
        public static void ChangeColumnDataType(this Page source, Stream destination, ColumnDefinition column, DataType newDataType) {
            var columns = source.SerieDefinition.Columns.ToArray();
            int columnIndex = -1;
            for(int i = 0; i < columns.Length; i++) {
                if(columns[i] == column) {
                    columnIndex = i;
                    columns[i] = ColumnDefinition.ChangeDataType(column, newDataType);
                    break;
                }
            }
            if(columnIndex < 0)
                throw new ArgumentException(nameof(column));

            var destSerie = new DataSerieDefinition(source.SerieDefinition, columns);
            using(var destinationPage = Page.CreateNew(destSerie, destination)) {
                for(int i = 0; i < columns.Length; i++) {
                    if(i != columnIndex)
                        CopyColumnData(source, destinationPage, destinationPage.Columns[i].Definition);
                }

                int count = (int)source.RowCount;
                var destType    = destinationPage.Columns[columnIndex].Definition.Type;
                var sourceArray = Array.CreateInstance(source.Columns[columnIndex].Definition.Type, count);
                var destArray   = Array.CreateInstance(destType, count);
                var sourceList  = sourceArray as System.Collections.IList;
                var destList    = destArray as System.Collections.IList;

                var read = source.Columns[columnIndex].ReadOnly.Read(sourceArray, 0, count);
                // convert
                for(int i = 0; i < read; i++)
                    destList[i] = Convert.ChangeType(sourceList[i], destType);

                var writer = destinationPage.Columns[columnIndex].WriteOnly;
                writer.Write(destArray, 0, count);
                writer.Commit();
                writer.Flush();

                destinationPage.RowCount = source.RowCount;
                destinationPage.SaveRowCount();

                // have to close the page because we dont support resuming pages yet, meaning the buffers/compression streams 
                // would not work for all previous columns
                destinationPage.Close();
            }
        }
        #endregion
        #region private static CopyColumnData()
        private static void CopyColumnData(Page source, Page destination, ColumnDefinition column) {
            var sourceColumn = source.Columns.First(o => o.Definition == column);
            var destColumn   = destination.Columns.First(o => o.Definition == column);

            var sourceColumnChannels = GetChannels(source, sourceColumn).ToList();
            var destColumnChannels   = GetChannels(destination, destColumn).ToList();

            for(int i = 0; i < sourceColumnChannels.Count; i++) {
                var sourceChannel = sourceColumnChannels[i];
                var destChannel   = destColumnChannels[i];

                if(sourceChannel.Length == 0)
                    continue;

                sourceChannel.CopyTo(destChannel);
                destChannel.Flush();
            }
        }
        #endregion
        #region static Page.Benchmark()
        public static string Benchmark(this Page source, BenchmarkDisplay display = BenchmarkDisplay.Default) {
            // this method was very hastily written
            // apologies

            const int MAX_ROWS = 10000000;
            
            // read every rows/columns to give a quick read-benchmark
            var res = new StringBuilder();
            string line = null;
            var tokens = new BenchmarkData_Column[source.Columns.Count];
            for(int i = 0; i < tokens.Length; i++) {
                tokens[i] = new BenchmarkData_Column() {
                    Column = source.Columns[i],
                    Data = Array.CreateInstance(source.Columns[i].Definition.Type, Math.Min((int)source.RowCount, MAX_ROWS)),
                    //RowCount = (int)source.RowCount,
                };
                var s = new System.Diagnostics.Stopwatch();
                s.Start();
                tokens[i].RowCount = tokens[i].Column.ReadOnly.Read(tokens[i].Data, 0, tokens[i].Data.Length);
                s.Stop();
                tokens[i].OriginalReadDuration = s.Elapsed;
                tokens[i].UncompressedDataSize = ((tokens[i].Column.Definition.BitSize.IsVariable ? -1 : (long)tokens[i].Column.Definition.BitSize.Bits.Value) / 8) * tokens[i].RowCount;
                line = $"[{i + 1}/{tokens.Length}] {tokens[i].Column.Definition.Name} ({tokens[i].Column.Definition.DataType}) - {BitMethods.HumanizeQuantity(tokens[i].RowCount)} rows @ {tokens[i].OriginalReadDuration}";
                Console.WriteLine(line);
                res.AppendLine(line);
            }
            Console.WriteLine();
            res.AppendLine();

            for(int i = 0; i < tokens.Length; i++) {
                var combinations = ColumnDefinition.GetCombinations(tokens[i].Column.Definition.DataType)
                    // skip sub-byte combinations
                    .Where(o => o.BitSize.IsVariable || o.BitSize.Bits.Value >= 8)
                    // drop zlib its just so god awful in every way
                    .Where(o => o.Compression.Algorithm != CompressionAlgorithm.zlib)
                    .OrderByDescending(o => o.Compression.Algorithm)
                    .ThenByDescending(o => o.Encoding)
                    .ToArray();

                line = $"[{i + 1}/{tokens.Length}] {tokens[i].Column.Definition.Name} ({tokens[i].Column.Definition.DataType}) - {BitMethods.HumanizeQuantity(tokens[i].RowCount)} rows";
                Console.WriteLine();
                Console.WriteLine();
                Console.WriteLine(line);
                Console.WriteLine();
                res.AppendLine();
                res.AppendLine();
                res.AppendLine(line);
                res.AppendLine();
                
                var preview = PrintPreviewMatrix(tokens[i].Data.Cast<object>().Select(o => o.ToString()), 4, 79);
                foreach(var previewLine in preview.Split('\n')) {
                    Console.WriteLine(previewLine);
                    res.AppendLine(previewLine);
                }

                for(int j=0; j<combinations.Length; j++){
                    var destSerie = new DataSerieDefinition(source.SerieDefinition, new[] { combinations[j] });
                    using(var destinationPage = Page.CreateNew(destSerie, new NullStream())) {
                        var data = (Array)tokens[i].Data.Clone(); // need to clone because some writers will write on the input

                        var s = new System.Diagnostics.Stopwatch();
                        s.Start();

                        // write the new column rows
                        var newColumnWriter = destinationPage.Columns[0].WriteOnly;
                        newColumnWriter.Write(data, 0, tokens[i].RowCount);
                        newColumnWriter.Commit();
                        newColumnWriter.Flush();

                        s.Stop();

                        var storage_size = GetChannels(destinationPage, destinationPage.Columns[0]).Sum(o => o.Length);

                        tokens[i].Writes.Add(new BenchmarkData_Column_Write() {
                            Owner = tokens[i],
                            ColumnDef = combinations[j],
                            Duration = s.Elapsed,
                            StorageSize = storage_size,
                        });
                        
                        line = string.Format("{0,-49} - {1,16} / {2,8}", $"[{i + 1}/{tokens.Length}] [{j + 1}/{combinations.Length}] {combinations[j]}", s.Elapsed, BitMethods.HumanizeByteSize(storage_size));
                        Console.WriteLine(line);
                        res.AppendLine(line);
                        
                        destinationPage.Close();
                    }
                }
                tokens[i].CalculateSubStats();
            }

            Console.WriteLine();
            Console.WriteLine("================================");
            Console.WriteLine();
            res.AppendLine();
            res.AppendLine("================================");
            res.AppendLine();

            // dump statistics in a more readable way
            for(int i = 0; i < tokens.Length; i++) {
                line = $"{tokens[i].Column.Definition.Name}";
                Console.WriteLine();
                Console.WriteLine(line);
                res.AppendLine();
                res.AppendLine(line);
                var xValues = (EncodingType[])Enum.GetValues(typeof(EncodingType));
                var yValues = (CompressionAlgorithm[])Enum.GetValues(typeof(CompressionAlgorithm));
                
                var matrix = new string[xValues.Length, yValues.Length];
                for(int j = 0; j < tokens[i].Writes.Count; j++) {
                    var item = tokens[i].Writes[j];
                    matrix[(int)item.ColumnDef.Encoding, (int)item.ColumnDef.Compression.Algorithm] = item.PrintStats();
                }

                var used_columns = new HashSet<EncodingType>(tokens[i].Writes.Select(o => o.ColumnDef.Encoding));
                var first_column_width = yValues.Max(o => Math.Min(o.ToString().Length, 5)) + 1;
                var column_widths = (79 - first_column_width) / used_columns.Count;

                var header = new StringBuilder();
                header.Append(' ', first_column_width);
                for(int x = 0; x < xValues.Length; x++) {
                    if(used_columns.Contains(xValues[x]))
                        header.Append(string.Format($"{{0,{column_widths}}}", xValues[x]));
                }
                line = new string(' ', first_column_width) + new string('-', column_widths * used_columns.Count);
                Console.WriteLine(header);
                Console.WriteLine(line);
                res.AppendLine(header.ToString());
                res.AppendLine(line);

                var flags = new[] {
                    new { Enum = BenchmarkDisplay.BytesPerSecond, Row = 1 },
                    new { Enum = BenchmarkDisplay.RowsPerSecond, Row = 0 },
                    new { Enum = BenchmarkDisplay.CompressedSize, Row = 2 },
                    new { Enum = BenchmarkDisplay.AvgBytesPerItem, Row = 3 },
                };
                foreach(var flag in flags) {
                    if(!display.HasFlag(flag.Enum))
                        continue;
                    for(int y = 0; y < yValues.Length; y++) {
                        // zlib is too god awful
                        if(yValues[y] == CompressionAlgorithm.zlib)
                            continue;
                        var line1 = new StringBuilder();
                        line1.Append(string.Format($"{{0,-{first_column_width}}}", yValues[y].ToString().Substring(0, Math.Min(yValues[y].ToString().Length, 5))));
                        for(int x = 0; x < xValues.Length; x++)
                            if(used_columns.Contains(xValues[x]))
                                line1.Append(string.Format($"{{0,{column_widths}}}", matrix[x, y] == null ? "" : matrix[x, y].Split('\n')[flag.Row]));
                        Console.WriteLine(line1);
                        res.AppendLine(line1.ToString());
                    }
                }
            }
            return res.ToString();
        }
        [Flags]
        public enum BenchmarkDisplay {
            Default = CompressedSize | BytesPerSecond,
            CompressedSize = 1 << 0,
            AvgBytesPerItem = 1 << 1,
            RowsPerSecond = 1 << 2,
            BytesPerSecond = 1 << 3,
            All = CompressedSize | AvgBytesPerItem | RowsPerSecond | BytesPerSecond,
        }
        private class BenchmarkData_Column {
            public Column Column;
            public Array Data;
            public int RowCount;
            public long UncompressedDataSize;
            public TimeSpan OriginalReadDuration; // time to read Data/RowCount entries
            public List<BenchmarkData_Column_Write> Writes = new List<BenchmarkData_Column_Write>();

            public void CalculateSubStats() {
                var comparand = this.Writes.First(o => o.ColumnDef.Encoding == EncodingType.None && o.ColumnDef.Compression.Algorithm == CompressionAlgorithm.NoCompress);

                foreach(var item in this.Writes) {
                    item.SpeedGain = item == comparand ? 0 : -(1 - (comparand.Duration.Ticks / (double)item.Duration.Ticks));
                    item.SizeGain = item == comparand ? 0 : -(1 - ((double)item.StorageSize / comparand.StorageSize));
                }
            }
        }
        private class BenchmarkData_Column_Write {
            public BenchmarkData_Column Owner;
            public ColumnDefinition ColumnDef;
            public TimeSpan Duration;
            public long StorageSize;
            public double SpeedGain;
            public double SizeGain;

            public string PrintStats() {
                var multiplier = (double)TimeSpan.FromSeconds(1).Ticks / this.Duration.Ticks;

                return string.Format("{0}/s{1,7}\n{2}/s{1,7}\n{3}{4,7}\n{5}{4,7}",
                    BitMethods.HumanizeQuantity((long)(this.Owner.RowCount * multiplier)),
                    PrintPercentage(this.SpeedGain),
                    BitMethods.HumanizeByteSize((long)(this.Owner.UncompressedDataSize * multiplier)),
                    BitMethods.HumanizeByteSize(this.StorageSize),
                    PrintPercentage(this.SizeGain),
                    this.StorageSize / (double)this.Owner.RowCount);
            }
            private static string PrintPercentage(double value) {
                if(value == 0)
                    return string.Empty;
                return value.ToString($"{(value > 0 ? "+" : string.Empty)}0.0%");
            }
        }
        /// <summary>
        ///     Prints as much of a preview of the data as possible.
        ///     ex: '0 3 6'
        ///         '1 4 7'
        ///         '2 5 8'
        /// </summary>
        private static string PrintPreviewMatrix(IEnumerable<string> data, int lines, int charactersPerLine) {
            int columns = 0;
            var widths = new List<int>();

            var res = new List<string>[lines];
            for(int y = 0; y < lines; y++)
                res[y] = new List<string>();

            bool done = false;
            var raw = data.GetEnumerator();
            var remainingCharacters = charactersPerLine;
            bool firstColumn = true;

            while(!done && remainingCharacters > 0) {
                int width = 0;
                for(int y = 0; y < lines; y++) {
                    if(!raw.MoveNext()) {
                        done = true;
                        break;
                    }
                    width = Math.Max(width, raw.Current?.Length ?? 0);
                    res[y].Add(raw.Current);
                }

                remainingCharacters -= width;
                if(!firstColumn)
                    remainingCharacters--;
                firstColumn = false;
                widths.Add(width);
                if(remainingCharacters > 0 || columns == 0)
                    columns++;
            }

            var sb = new StringBuilder(lines * charactersPerLine + lines);
            for(int y = 0; y < lines; y++) {
                for(int x = 0; x < columns; x++) {
                    if(x != 0)
                        sb.Append(' ');
                    sb.Append((res[y][x]?.ToString() ?? string.Empty).PadLeft(widths[x]));
                }
                sb.Append('\n');
            }
            return sb.ToString();
        }
        #endregion

        #region static Page.ChangeArchiveCompression()
        public static void ChangeArchiveCompression(this Page source, string destinationArchiveFile, CompressionSetting compression = null) {
            const int BUFFER_SIZE = 65536;
            var buffer = new byte[BUFFER_SIZE];

            var destSerie = DataSerieDefinition.ChangeCompression(source.SerieDefinition, compression);

            using(var destinationPage = Page.CreateNew(destSerie, destinationArchiveFile)) {
                int channelIndex = -1;
                var channelCount = source.Channels.List.Count();
                long decompressed = 0;
                long compressed = 0;
                foreach(var sourceChannel in source.Channels.List) {
                    channelIndex++;
                    if(sourceChannel.Length == 0)
                        continue;
                    var sourceColumn = GetColumn(source, sourceChannel);
                    var sourceCompressedStream = sourceColumn.Definition.ReadStreamWrapper(sourceChannel);

                    var destChannel = destinationPage.Channels.GetOrCreateChannel(sourceChannel.ID);
                    var destColumn = GetColumn(destinationPage, destChannel);
                    var destCompressedStream = destColumn.Definition.WriteStreamWrapper(destChannel);

                    //sourceCompressedStream.CopyTo(destCompressedStream);
                    int read;
                    long total = 0;
                    while((read = sourceCompressedStream.Read(buffer, 0, BUFFER_SIZE)) > 0) {
                        destCompressedStream.Write(buffer, 0, read);
                        
                        total += read;
                        decompressed += read;
                        Console.WriteLine($"[{channelIndex + 1}/{channelCount}] {BitMethods.HumanizeByteSize(total)} -> {BitMethods.HumanizeByteSize(destChannel.Length)}   (total: {BitMethods.HumanizeByteSize(decompressed)} -> {BitMethods.HumanizeByteSize(compressed+destChannel.Length)}  {Math.Round(((compressed+destChannel.Length)/(double)decompressed)*100d, 1)}%)");
                    }

                    destCompressedStream.Flush();
                    compressed += destChannel.Length;
                }
                destinationPage.RowCount = source.RowCount;
                destinationPage.SaveRowCount();
                destinationPage.Close();
            }
        }
        public static void ChangeArchiveCompression(string sourceArchiveFile, string destinationArchiveFile, CompressionSetting compression = null) {
            using(var sourcePage = Page.Load(sourceArchiveFile)) {
                ChangeArchiveCompression(sourcePage, destinationArchiveFile, compression);
            }
        }
        #endregion
        #region static Page.SaveToCSV()
        public static void SaveToCSV(this Page source, string destinationCSVFile) {
            using(var csv = new CsvStreamWriter(new FileStream(destinationCSVFile, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))) {
                const int ROW_COUNT = 4096 / 8;

                // need the rows in format that works for the csv (object[rows][columns])
                int columns = source.Columns.Count;
                var rows = new object[ROW_COUNT][];
                for(int i = 0; i < ROW_COUNT; i++)
                    rows[i] = new object[columns];

                // this is where we'll read columnar data
                var buffer = new Array[columns];
                for(int i = 0; i < columns; i++)
                    buffer[i] = Array.CreateInstance(source.Columns[i].Definition.Type, ROW_COUNT);

                long total = 0;

                var remaining = source.RowCount;
                while(remaining > 0) {
                    int process_rows = (int)Math.Min(ROW_COUNT, remaining);
                    for(int i = 0; i < columns; i++) {
                        var writeBuffer = buffer[i];
                        var writeBufferIList = (System.Collections.IList)writeBuffer;
                        int read = source.Columns[i].ReadOnly.Read(writeBuffer, 0, process_rows);

                        if(read != process_rows)
                            System.Diagnostics.Debugger.Break();

                        // then move this back into rows
                        for(int j = 0; j < process_rows; j++)
                            rows[j][i] = writeBufferIList[j];
                    }

                    for(int i = 0; i < process_rows; i++) {
                        if(++total % 1000 == 0)
                            Console.WriteLine(total);

                        csv.WriteLine(rows[i]);
                    }

                    remaining -= (ulong)process_rows;
                }
            }
        }
        public static void SaveToCSV(string sourceArchiveFile, string destinationCSVFile) {
            using(var page = Page.Load(sourceArchiveFile)) {
                SaveToCSV(page, destinationCSVFile);
            }
        }
        #endregion
        #region static Page.BuildSyntheticBenchmarkData()
        public static void BuildSyntheticBenchmarkData(string archiveFile, int rowCount = 1000000) {
            Func<double, DateTime> d_to_dt1 = o => new DateTime((int)(2000 + o * 100), 1, 1);
            Func<double, DateTime> d_to_dt2 = o => new DateTime(new DateTime(2000, 1, 1).Ticks + (long)(o * TimeSpan.TicksPerSecond));

            var _cases = new[]{
                new { Name = "int8_FiniteSet(100) int8",         Values = (IEnumerable)TestCases.FiniteSet(0.05, 100).Select(o => (sbyte)(o - 50))},
                new { Name = "int16_FiniteSet(100) int16",       Values = (IEnumerable)TestCases.FiniteSet(0.05, 100).Select(o => (short)(o - 50))},
                new { Name = "int32_FiniteSet(100) int32",       Values = (IEnumerable)TestCases.FiniteSet(0.05, 100).Select(o => (int)(o - 50))},
                new { Name = "int64_FiniteSet(100) int64",       Values = (IEnumerable)TestCases.FiniteSet(0.05, 100).Select(o => (long)(o - 50))},
                new { Name = "uint8_FiniteSet(100) uint8",       Values = (IEnumerable)TestCases.FiniteSet(0.05, 100).Select(o => (byte)o)},
                new { Name = "uint16_FiniteSet(100) uint16",     Values = (IEnumerable)TestCases.FiniteSet(0.05, 100).Select(o => (ushort)o)},
                new { Name = "uint32_FiniteSet(100) uint32",     Values = (IEnumerable)TestCases.FiniteSet(0.05, 100).Select(o => (uint)o)},
                new { Name = "uint64_FiniteSet(100) uint64",     Values = (IEnumerable)TestCases.FiniteSet(0.05, 100).Select(o => (ulong)o)},
                new { Name = "float_FiniteSet(100) float",       Values = (IEnumerable)TestCases.FiniteSet(0.05, 100).Select(o => (float)o)},
                new { Name = "double_FiniteSet(100) double",     Values = (IEnumerable)TestCases.FiniteSet(0.05, 100).Select(o => (double)o)},
                new { Name = "decimal_FiniteSet(100) decimal",   Values = (IEnumerable)TestCases.FiniteSet(0.05, 100).Select(o => (decimal)o)},

                new { Name = "d_Constant double",                Values = (IEnumerable)TestCases.Const(42)},
                new { Name = "d_Random double",                  Values = (IEnumerable)TestCases.Random()},
                new { Name = "d_RandomWalk double",              Values = (IEnumerable)TestCases.RandomWalk(0, 0.01)},
                new { Name = "d_RandomWalk_(x10) double",        Values = (IEnumerable)TestCases.RandomWalk(0, 0.1)},
                new { Name = "d_FiniteSet_(2) double",           Values = (IEnumerable)TestCases.FiniteSet(0.05, 2)},
                new { Name = "d_FiniteSet_(10) double",          Values = (IEnumerable)TestCases.FiniteSet(0.05, 10)},
                new { Name = "d_FiniteSet_(100) double",         Values = (IEnumerable)TestCases.FiniteSet(0.05, 100)},
                new { Name = "d_Pattern_(2) double",             Values = (IEnumerable)TestCases.RepeatingPattern(2)},
                new { Name = "d_Pattern_(10) double",            Values = (IEnumerable)TestCases.RepeatingPattern(10)},
                new { Name = "d_Pattern_(100) double",           Values = (IEnumerable)TestCases.RepeatingPattern(100)},
                new { Name = "d_SteadyGrowth double",            Values = (IEnumerable)TestCases.SteadyGrowing(0.1, 1)},
                new { Name = "d_SteadyGrowth_(int) double",      Values = (IEnumerable)TestCases.SteadyGrowing(1, 1)},
                new { Name = "d_SlowRandomGrowth double",        Values = (IEnumerable)TestCases.RandomlyGrowing(0.05, 0.1)},
                new { Name = "d_FastRandomGrowth double",        Values = (IEnumerable)TestCases.RandomlyGrowing(0.5, 0.1)},
                new { Name = "d_SlowRandomGrowth_(int) double",  Values = (IEnumerable)TestCases.RandomlyGrowing(0.05, 10)}, // 5% chance of doing another step
                new { Name = "d_FastRandomGrowth_(int) double",  Values = (IEnumerable)TestCases.RandomlyGrowing(0.5, 10)},
                
                //new { Name = "dt_now datetime",                Values = (IEnumerable)Enumerable.Repeat(0, rowCount).Select(o => DateTime.UtcNow) },
                //new { Name = "dt_Random datetime",             Values = (IEnumerable)TestCases.Random().Select(d_to_dt1) }, // crashes with DeltaDelta
                new { Name = "dt_SteadyGrowth datetime",         Values = (IEnumerable)TestCases.SteadyGrowing(1, 1).Select(d_to_dt2) },
                new { Name = "dt_SlowRandomGrowth datetime",     Values = (IEnumerable)TestCases.RandomlyGrowing(0.05, 1).Select(d_to_dt2) }, // 5% chance of doing another step
                new { Name = "dt_FastRandomGrowth datetime",     Values = (IEnumerable)TestCases.RandomlyGrowing(0.5, 1).Select(d_to_dt2) },

                new { Name = "uint32_Constant uint32",           Values = (IEnumerable)TestCases.Const(42).Select(o => (uint)o)},
                new { Name = "uint32_Random uint32",             Values = (IEnumerable)TestCases.Random().Select(o => (uint)(o * uint.MaxValue))},
                new { Name = "uint32_RandomWalk uint32",         Values = (IEnumerable)TestCases.RandomWalk(0, 1).Select(o => (uint)(o + 10000))},
                new { Name = "uint32_RandomWalk_(x10) uint32",   Values = (IEnumerable)TestCases.RandomWalk(0, 10).Select(o => (uint)(o + 10000))},
                new { Name = "uint32_FiniteSet_(2) uint32",      Values = (IEnumerable)TestCases.FiniteSet(0.05, 2).Select(o => (uint)o)},
                new { Name = "uint32_FiniteSet_(10) uint32",     Values = (IEnumerable)TestCases.FiniteSet(0.05, 10).Select(o => (uint)o)},
                new { Name = "uint32_FiniteSet_(100) uint32",    Values = (IEnumerable)TestCases.FiniteSet(0.05, 100).Select(o => (uint)o)},
                new { Name = "uint32_Pattern_(2) uint32",        Values = (IEnumerable)TestCases.RepeatingPattern(2).Select(o => (uint)(o * uint.MaxValue))},
                new { Name = "uint32_Pattern_(10) uint32",       Values = (IEnumerable)TestCases.RepeatingPattern(10).Select(o => (uint)(o * uint.MaxValue))},
                new { Name = "uint32_Pattern_(100) uint32",      Values = (IEnumerable)TestCases.RepeatingPattern(100).Select(o => (uint)(o * uint.MaxValue))},
                new { Name = "uint32_SteadyGrowth uint32",       Values = (IEnumerable)TestCases.SteadyGrowing(1, 1).Select(o => (uint)o)},
                new { Name = "uint32_SlowRandomGrowth uint32",   Values = (IEnumerable)TestCases.RandomlyGrowing(0.05, 10).Select(o => (uint)o)}, // 5% chance of doing another step
                new { Name = "uint32_FastRandomGrowth uint32",   Values = (IEnumerable)TestCases.RandomlyGrowing(0.5, 10).Select(o => (uint)o)},

                new { Name = "uint64_Constant uint64",           Values = (IEnumerable)TestCases.Const(42).Select(o => (ulong)o)},
                new { Name = "uint64_Random uint64",             Values = (IEnumerable)TestCases.Random().Select(o => (ulong)(o * ulong.MaxValue))},
                new { Name = "uint64_RandomWalk uint64",         Values = (IEnumerable)TestCases.RandomWalk(0, 1).Select(o => (ulong)(o + 10000))},
                new { Name = "uint64_RandomWalk_(x10) uint64",   Values = (IEnumerable)TestCases.RandomWalk(0, 10).Select(o => (ulong)(o + 10000))},
                new { Name = "uint64_FiniteSet_(2) uint64",      Values = (IEnumerable)TestCases.FiniteSet(0.05, 2).Select(o => (ulong)o)},
                new { Name = "uint64_FiniteSet_(10) uint64",     Values = (IEnumerable)TestCases.FiniteSet(0.05, 10).Select(o => (ulong)o)},
                new { Name = "uint64_FiniteSet_(100) uint64",    Values = (IEnumerable)TestCases.FiniteSet(0.05, 100).Select(o => (ulong)o)},
                new { Name = "uint64_Pattern_(2) uint64",        Values = (IEnumerable)TestCases.RepeatingPattern(2).Select(o => (ulong)(o * ulong.MaxValue))},
                new { Name = "uint64_Pattern_(10) uint64",       Values = (IEnumerable)TestCases.RepeatingPattern(10).Select(o => (ulong)(o * ulong.MaxValue))},
                new { Name = "uint64_Pattern_(100) uint64",      Values = (IEnumerable)TestCases.RepeatingPattern(100).Select(o => (ulong)(o * ulong.MaxValue))},
                new { Name = "uint64_SteadyGrowth uint64",       Values = (IEnumerable)TestCases.SteadyGrowing(1, 1).Select(o => (ulong)o)},
                new { Name = "uint64_SlowRandomGrowth uint64",   Values = (IEnumerable)TestCases.RandomlyGrowing(0.05, 10).Select(o => (ulong)o)}, // 5% chance of doing another step
                new { Name = "uint64_FastRandomGrowth uint64",   Values = (IEnumerable)TestCases.RandomlyGrowing(0.5, 10).Select(o => (ulong)o)},
            };

            var all = _cases
                .Select(o => {
                    var array = (IList)Array.CreateInstance(o.Values.Cast<object>().First().GetType(), rowCount);
                    var _enumerator = o.Values.GetEnumerator();
                    for(int i = 0; i < rowCount; i++) {
                        _enumerator.MoveNext();
                        array[i] = _enumerator.Current;
                    }
                    return new { o.Name, Values = array };
                }).ToArray();

            var column_definitions = string.Join("\n", all.Select(o => o.Name));
            var serie = new DataSerieDefinition($"SyntheticBenchmarkData\n{column_definitions}");

            using(var page = Page.CreateNew(serie, archiveFile)) {
                for(int j = 0; j < all.Length; j++) {
                    page.Columns[j].WriteOnly.Write((Array)all[j].Values, 0, rowCount);
                    Console.WriteLine($"[{j + 1}/{all.Length}] {all[j].Name} {rowCount}");
                }

                page.RowCount = (ulong)rowCount;
                page.Save();
                page.Close();
            }
        }
        #region private static class TestCases
        private static class TestCases {
            public static IEnumerable<double> Const(double value) {
                while(true)
                    yield return value;
            }
            public static IEnumerable<double> Random() {
                var rnd = new Random();
                
                while(true) {
                    var res = rnd.NextDouble();
                    yield return res;
                }
            }
            /// <param name="mu">mean</param>
            /// <param name="sigma">standard deviation</param>
            public static IEnumerable<double> RandomWalk(double mu = 0, double sigma = 1) {
                double value = 0;

                foreach(var item in NextGaussian(new Random(), mu, sigma)) {
                    value += item;
                    yield return value;
                }
            }
            /// <summary>
            /// Obtains normally (Gaussian) distributed random numbers, using the Box-Muller
            /// transformation. This transformation takes two uniformly distributed deviates
            /// within the unit circle, and transforms them into two independently
            /// distributed normal deviates.
            /// </summary>
            /// <param name="mu">The mean of the distribution.</param>
            /// <param name="sigma">The standard deviation of the distribution.</param>
            public static IEnumerable<double> NextGaussian(Random random, double mu = 0, double sigma = 1) {
                if(sigma <= 0)
                    throw new ArgumentOutOfRangeException(nameof(sigma), "Must be > 0.");

                while(true) {
                    double v1;
                    double v2;
                    double rSquared;

                    do {
                        v1 = random.NextDouble() * 2 - 1;
                        v2 = random.NextDouble() * 2 - 1;
                        rSquared = v1 * v1 + v2 * v2;
                        // ensure within the unit circle
                    } while(rSquared >= 1 || rSquared == 0);

                    // calculate polar tranformation for each deviate
                    var polar = Math.Sqrt(-2 * Math.Log(rSquared) / rSquared);

                    yield return v1 * polar * sigma + mu;
                    yield return v2 * polar * sigma + mu;
                }
            }
            public static IEnumerable<double> RandomlyGrowing(double growthrate, double step) {
                double value = 0;
                var random = new Random();

                while(true) {
                    do {
                        value += step;
                    } while(random.NextDouble() < growthrate);

                    yield return value;
                }
            }
            public static IEnumerable<double> SteadyGrowing(double growthrate, int steps) {
                if(steps <= 0)
                    throw new ArgumentException(nameof(steps));
                double value = 0;
                var random = new Random();

                while(true) {
                    for(int i = 0; i < steps; i++)
                        yield return value;

                    value += growthrate;
                }
            }
            /// <param name="n">Pattern length.</param>
            public static IEnumerable<double> RepeatingPattern(int n) {
                if(n <= 0)
                    throw new ArgumentException(nameof(n));

                var random = new Random();
                var pattern = new double[n];
                for(int i = 0; i < n; i++)
                    pattern[i] = random.NextDouble();

                while(true) {
                    foreach(var item in pattern)
                        yield return item;
                }
            }
            /// <summary>
            ///     Returns infinite random sequence of flags (1, 2, 3, ...).
            /// </summary>
            /// <param name="probability_flag_switch">Between 0-1.</param>
            /// <param name="n">Number of flags.</param>
            public static IEnumerable<double> FiniteSet(double probability_flag_switch, int n) {
                if(n <= 0)
                    throw new ArgumentException(nameof(n));

                var random = new Random();
                var current = random.Next(0, n);

                while(true) {
                    if(random.NextDouble() < probability_flag_switch)
                        current = random.Next(0, n);

                    yield return current;
                }
            }
        }
        #endregion
        #endregion

        #region static Page.DebugDump()
        public static string DebugDump(this Page source) {
            var sb = new StringBuilder();

            int serie_size = 0;
            var buffer = new byte[BitMethods.ENCODESTRING_BUFFER_SIZE]; // new byte[Encoding.UTF8.GetMaxByteCount(source.SerieDefinition.ToString().Length)]
            BitMethods.EncodeString(buffer, ref serie_size, null, Encoding.UTF8, source.SerieDefinition.ToString());

            sb.AppendLine($"Page header (stream position: {source.HeaderPosition})");
            sb.AppendLine($"------------------------");
            sb.AppendLine($"magic_signature (uint32): 0x{Page.MAGIC_SIGNATURE.ToString("X8")}");
            sb.AppendLine($"serie_unique_id (uint32): {source.SerieDefinition.UniqueID}");
            sb.AppendLine($"page_version    (uint8):  {Page.PAGE_VERSION}");
            sb.AppendLine($"row_count       (uint64): {source.RowCount}");
            sb.AppendLine($"serie           (string, size={serie_size}): \"{source.SerieDefinition.ToString()}\"");
            sb.AppendLine($"raw data        (multi-channel stream): [further down]");
            sb.AppendLine();
            sb.AppendLine($"Serie");
            sb.AppendLine($"------------------------");
            sb.AppendLine($"{source.SerieDefinition.ToString(false)}");
            foreach(var c in source.SerieDefinition.Columns) {
                sb.AppendLine($"  -> {c.ToString()}");
                foreach(var channel in GetChannels(source, source.Columns.First(o => o.Definition == c)))
                    sb.AppendLine($"     {channel.DebugDump()}");
            }
            sb.AppendLine();
            sb.AppendLine($"raw data");
            sb.Append(source.Channels.DebugDump());

            return sb.ToString();
        }
        #endregion
        #region static Page.DebugDumpContent()
        public static void DebugDumpContent(this Page source, string directory) {
            if(!Directory.Exists(directory))
                Directory.CreateDirectory(directory);

            int column_index = 0;
            foreach(var c in source.Columns) {
                foreach(var channel in GetChannels(source, c)) {
                    var filename = Path.Combine(directory, string.Format("column-[{0}] {1} channel-{2}.bin", column_index, c.Definition.Name, channel.ID));
                    using(var fs = new FileStream(filename, FileMode.Create, FileAccess.Write, FileShare.ReadWrite)) {
                        channel.Position = 0;
                        channel.CopyTo(fs);
                    }
                }
                column_index++;
            }
        }
        #endregion
    }
}
