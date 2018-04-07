using System;
using System.IO;
using System.Text;
using System.Collections.Generic;
using System.Linq;

using Newtonsoft.Json.Linq;


namespace TimeSeriesDB
{
    using DataStreams.Readers;
    using DataStreams.Writers;
    using Internal;
    using IO;
    
    public static class PageExamples {
        #region static StoreFiles()
        public static void StoreFiles(string archiveFile, string path, string searchPattern = "*.*", SearchOption searchOptions = SearchOption.AllDirectories) {
            var serie = new DataSerieDefinition(@"files
FileName string
Content byte[]");

            using(var page = Page.CreateNew(serie, archiveFile)){
                var files = Directory.GetFiles(path, searchPattern, searchOptions);

                var adapter = new FileAdapter() { Page = page, };
                var buffer = new File[4096 / 8];
                int writeIndex = 0;
                var directory = Path.GetFullPath(path + "/");

                for(int i = 0; i < files.Length; i++) {
                    if(i % 100 == 0)
                        Console.WriteLine(i);

                    buffer[writeIndex++] = new File() {
                        FileName = files[i].Substring(directory.Length).Replace('\\', '/'),
                        Content = System.IO.File.ReadAllBytes(files[i]),
                    };
                    if(writeIndex == buffer.Length) {
                        writeIndex = 0;
                        adapter.Put(buffer, 0, buffer.Length);
                    }
                }
                if(writeIndex > 0)
                    adapter.Put(buffer, 0, writeIndex);
                adapter.Commit();

                page.Save();
                page.Close();
            }
        }
        #endregion
        #region static RestoreFiles()
        public static void RestoreFiles(string archiveFile, string destinationPath) {
            var directory = Path.GetFullPath(destinationPath + "/");
            using(var page = Page.Load(archiveFile)) {
                var adapter = new FileAdapter() { Page = page, };
                var buffer = new File[4096 / 8];
                int read;
                long total = 0;

                while((read = adapter.Read(buffer, 0, buffer.Length)) > 0) {
                    for(int i = 0; i < read; i++) {
                        if(total++ % 100 == 0)
                            Console.WriteLine(total - 1);

                        var filename = Path.GetFullPath(Path.Combine(directory, buffer[i].FileName));

                        //BuildDirectoryPath(filename);
                        //System.IO.File.WriteAllBytes(filename, buffer[i].Content);
                    }
                }
            }
        }
        #endregion

        #region static ParseJsonTicker()
        public static void ParseJsonTicker(string archiveFile, string path, string searchPattern = "*.*", SearchOption searchOptions = SearchOption.AllDirectories, string regex = null) {
            var files = Directory.GetFiles(path, searchPattern, searchOptions);
            if(regex != null) {
                var r = new System.Text.RegularExpressions.Regex(regex);
                files = files.Where(o => r.IsMatch(o)).ToArray();
            }
            ParseJsonTicker(
                archiveFile, 
                "tickers_test", 
                files);
        }
        /// <param name="serieDefMinusColumns">ex: 'tickers coin_symbol=BTC coin_id=1 interval=5min/15min/1h/all'</param>
        public static void ParseJsonTicker(string archiveFile, string serieDefMinusColumns, string[] jsonFiles) {
            var serie = new DataSerieDefinition(serieDefMinusColumns + @"
CoinID uint
Symbol string
Timestamp datetime
MarketCapByAvailableSupply double
PriceBTC double
PricePlatform double
PriceUSD double
VolumeUSD double");

            var errors = new List<string>();
            ulong totalRows = 0;

            using(var page = Page.CreateNew(serie, archiveFile)) {
                for(int i = 0; i < jsonFiles.Length; i++){
                    var file = jsonFiles[i];
                    JToken json = null;
                    try {
                        json = JToken.Parse(System.IO.File.ReadAllText(file));
                    } catch {
                        errors.Add(file);
                        continue;
                    }

                    var coinName        = (string)json["coin"]["Name"]["Name"];
                    var coinSymbol      = (string)json["coin"]["Name"]["Symbol"];
                    var coinID          = (uint)json["coin"]["ID"];
                    var tickerCount     = (long)json["ticker_count"];
                    var avgInterval     = (TimeSpan)json["avg_interval"];
                    var requestedStart  = (DateTime)json["requested"]["start"];
                    var requestedEnd    = (DateTime)json["requested"]["end"];
                    //var actualStart     = (DateTime)json["actual"]["start"]; // can return null
                    //var actualEnd       = (DateTime)json["actual"]["end"]; // can return null
                    var uri             = (string)json["uri"];
                    var willNeedRequery = (bool)json["will_need_requery"];
                    var format          = (string)json["format"];

                    if(willNeedRequery)
                        continue;

                    bool includes_price_platform = false;
                    switch(format) {
                        case "timestamp (unix format milliseconds since 1970),market_cap_by_available_supply,price_btc,price_platform,price_usd,volume_usd":
                            includes_price_platform = true;
                            break;
                        case "timestamp (unix format milliseconds since 1970),market_cap_by_available_supply,price_btc,price_usd,volume_usd":
                            break;
                        default:
                            throw new NotImplementedException();
                    }

                    const int BUFFER_SIZE = 512;
                    var coin_id                        = new uint[BUFFER_SIZE];
                    var coin_symbol                    = new string[BUFFER_SIZE];
                    var timestamp                      = new DateTime[BUFFER_SIZE];
                    var market_cap_by_available_supply = new double[BUFFER_SIZE];
                    var price_btc                      = new double[BUFFER_SIZE];
                    var price_platform                 = new double[BUFFER_SIZE];
                    var price_usd                      = new double[BUFFER_SIZE];
                    var volume_usd                     = new double[BUFFER_SIZE];
                    int index = 0;

                    void dump_rows() {
                        page.Columns["CoinID"].WriteOnly.Write(coin_id, 0, index);
                        page.Columns["Symbol"].WriteOnly.Write(coin_symbol, 0, index);
                        page.Columns["Timestamp"].WriteOnly.Write(timestamp, 0, index);
                        page.Columns["MarketCapByAvailableSupply"].WriteOnly.Write(market_cap_by_available_supply, 0, index);
                        page.Columns["PriceBTC"].WriteOnly.Write(price_btc, 0, index);
                        page.Columns["PricePlatform"].WriteOnly.Write(price_platform, 0, index);
                        page.Columns["PriceUSD"].WriteOnly.Write(price_usd, 0, index);
                        page.Columns["VolumeUSD"].WriteOnly.Write(volume_usd, 0, index);
                        index = 0;
                    }

                    foreach(var row in json["Data"]) {
                        coin_id[index]                        = coinID;
                        coin_symbol[index]                    = coinSymbol;
                        timestamp[index]                      = ReadUnixDate((long)row[0]);
                        market_cap_by_available_supply[index] = (double)row[1];
                        price_btc[index]                      = (double)row[2];
                        price_platform[index]                 = includes_price_platform ? (double)row[3] : 0;
                        price_usd[index]                      = (double)row[includes_price_platform ? 4 : 3];
                        volume_usd[index]                     = (double)row[includes_price_platform ? 5 : 4];

                        if(++index == BUFFER_SIZE)
                            dump_rows();

                        if(++totalRows % 1000 == 0)
                            Console.WriteLine($"[{i + 1}/{jsonFiles.Length}] {totalRows}");
                    }
                    if(index > 0)
                        dump_rows();
                }

                page.RowCount = totalRows;
                page.Save();
                page.Close();
            }

            if(errors.Count > 0)
                System.Diagnostics.Debugger.Break();
        }
        private static readonly DateTime m_unixDateStart = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
        private static DateTime ReadUnixDate(long value) {
            return DateTime.SpecifyKind(m_unixDateStart + TimeSpan.FromMilliseconds(value), DateTimeKind.Utc);
        }
        #endregion

        #region private static BuildDirectoryPath()
        private static void BuildDirectoryPath(string path) {
            var directory = Path.GetDirectoryName(path);
            var info = new DirectoryInfo(directory);
            if(info.Exists)
                return;

            var folders = new List<DirectoryInfo>();;
            while(info != null) {
                if(info.Exists)
                    break;
                folders.Add(info);
                info = info.Parent;
            }
            while(folders.Count > 0) {
                folders[folders.Count - 1].Create();
                folders.RemoveAt(folders.Count - 1);
            }
        }
        #endregion

        #region private class FileAdapter
        private sealed class File {
            public string FileName;
            public byte[] Content;
        }
        private sealed class FileAdapter : PageRowAdapter<File> {
            private readonly string[] m_filenameBuffer = new string[BUFFER_SIZE];
            private readonly byte[][] m_contentBuffer = new byte[BUFFER_SIZE][];

            /// <summary>
            ///     Read the buffer from Page.Columns.
            ///     Count will never exceed BUFFER_SIZE.
            /// </summary>
            protected override int InternalReadRows(File[] buffer, int offset, int count) {
                var filename = this.Page.Columns["FileName"].ReadOnly as DataStreamReader_String;
                var content = this.Page.Columns["Content"].ReadOnly as DataStreamReader_ByteArray;

                var read = filename.Read(m_filenameBuffer, 0, count);
                content.Read(m_contentBuffer, 0, read);

                for(int i = 0; i < read; i++) {
                    buffer[offset++] = new File() {
                        FileName = m_filenameBuffer[i],
                        Content = m_contentBuffer[i],
                    };
                }

                return read;
            }
            /// <summary>
            ///     Write the buffer into the Page.Columns.
            ///     Count will never exceed BUFFER_SIZE.
            /// </summary>
            protected override void InternalWriteRows(File[] buffer, int offset, int count) {
                var filename = this.Page.Columns["FileName"].WriteOnly as DataStreamWriter_String;
                var content = this.Page.Columns["Content"].WriteOnly as DataStreamWriter_ByteArray;

                for(int i = 0; i < count; i++) {
                    var value = buffer[offset++];
                    m_filenameBuffer[i] = value.FileName;
                    m_contentBuffer[i] = value.Content;
                }

                filename.Write(m_filenameBuffer, 0, count);
                content.Write(m_contentBuffer, 0, count);
            }
        }
        #endregion
    }
}
