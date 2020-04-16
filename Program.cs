using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Globalization;

using TimeSeriesDB.IO;
using TimeSeriesDB.Internal;

namespace TimeSeriesDB {
    class Program {

        private static void a() {
            ////var csvwr = new CsvStreamWriter(new System.IO.FileStream("d:\\test.csv", System.IO.FileMode.Append, System.IO.FileAccess.Write));
            //var csvr = new CsvStreamReader(new System.IO.FileStream("d:\\test.csv", System.IO.FileMode.Open, System.IO.FileAccess.Read));
            //var csvr = new CsvStreamReader("C:\\Users\\Marton\\Desktop\\Dropbox\\AdaptiveRadixTree\\qq.txt");

            /*var r = new CsvStreamReader(System.IO.File.OpenRead("d:\\double_0001.csv"));
            DateTime[] x1 = new DateTime[100000];
            double[] x2 = new double[100000];
            int index = 0;
            while(index < 100000) {
                r.SkipColumns(1);
                x1[index] = r.ReadDateTime().Value;
                x2[index++] = r.ReadDouble().Value;
                r.SkipLine();
            }*/


            var nowtt = DateTime.UtcNow;
            //
            //for(int i = 0; i < 1; i++) {
            //    //csvwr.Write(-nowtt.TimeOfDay );
            //    var x = csvr.ReadString();
            //    //csvwr.Write("this is a fucking test");
            //    //csvwr.Write(new byte[] {(byte) i});
            //    //csvwr.WriteComma();
            //    //csvwr.WriteLine();
            //}
            ////csvwr.Close();

            //PageExtensions.BuildSyntheticBenchmarkData("d:\\benchmarkdata2.zstd", 1000000);
            //PageExtensions.SaveToCSV("d:\\benchmarkdata.zstd", "d:\\benchmarkdata.csv");
            //PageExamples.StoreFiles("d:\\coins3_new.zstd", "d:\\coins_history\\");
            //PageExamples.RestoreFiles("d:\\coins2_new.zstd", "d:\\coins_history2\\");
            //PageExtensions.ChangeArchiveCompression("d:\\coins2_new.zstd", "d:\\coins4.zstd", new CompressionSetting(CompressionAlgorithm.zstd, "3"));

            //var w = new DataStreams.Writers.DataStreamWriter_UInt64_LSB();
            //var ms = new DynamicMemoryStream();
            //w.Init(new[] { ms });
            ////var xtem = RandomWalk(0, 1).Take(1000000).Select(o => (uint)o).ToArray();
            ////var xtem = "4294967295,0,0,0,0,1,1".Split(',').Select(o => uint.Parse(o)).ToArray();
            //var xtem = Enumerable.Range(0, 1000000).Take(1000000).Select(o => (ulong)0x55).ToArray();
            //int y = 0;
            //for(int i = 0; i < 500; i++) {
            //    Array.Clear(xtem, y, i);
            //    y += i + 1;
            //}
            //var xtem2 = new ulong[1000000];
            //w.Write(xtem, 0, xtem.Length);
            //w.Commit();
            //w.Flush();
            //ms.Position = 0;
            //
            //var r = new DataStreams.Readers.DataStreamReader_UInt64_LSB();
            //r.Init(new[] { ms });
            //var sdfsdf = r.Read(xtem2, 0, xtem.Length);
            //for(int i=0; i<xtem.Length; i++)
            //    if(xtem[i] != xtem2[i])
            //        "".ToString();

            // todo: read benchmarks
            // dtoa atod
            // csv benchmarks
            // page append mode (re-open page and add stuff)
            // generic/dynamic page adapter
            // benchmark generic/dynamic page adapter vs hand-coded
            // page statistics (first/last)
            // pagemanager + filestorageadapter/postgresqlstorageadapter
            // dfcm on int
            //IResumableDataStream
            // encodingtypes.RewindToLastCheckpoint()
            // collect eth tip reddit
            // test xor/delta/deltadelta encoders with 4k chunks, shouldnt work since code assumes we read write 2 buffer
            // and all reading classes
            // streamreaderbase requires read benchmarks
            // replace CountLeadingZero/CountTrailingZero/CountZeroes with CPU intrinsics (BSR/BSF/builtin_ctz/builtin_clz) for major speedup


            //PageExamples.ParseJsonTicker("d:\\coins_test 5min.zstd", "D:\\coins_history\\", "*.json", System.IO.SearchOption.AllDirectories, "\\\\5min\\\\");
            //PageExtensions.SaveToCSV(Page.Load("d:\\coins_test.zstd"), "d:\\coins_test.csv");
            var xx = Extensions.Benchmark(Page.Load("d:\\benchmarkdata2.zstd"), Extensions.BenchmarkDisplay.All);
            System.IO.File.WriteAllText("d:\\bench2.txt", xx);
            //var xx = PageExtensions.Benchmark(Page.Load("d:\\coins_test 5min.zstd"), PageExtensions.BenchmarkDisplay.RowsPerSecond);
            //var xx = PageExtensions.Benchmark(Page.Load("d:\\aaaaaa.dat"), PageExtensions.BenchmarkDisplay1.RowsPerSecond, PageExtensions.BenchmarkDisplay2.AvgBytesPerItem);


            /*var r = new CsvStreamReader(System.IO.File.OpenRead("d:\\double_0001.csv"));
            r.SkipColumns = new bool[] { true };
            DateTime[] x1 = new DateTime[100000];
            double[] x2 = new double[100000];
            int index = 0;
            while(index < 100000) {
                if(!r.Read())
                    break;
                x1[index] = ((DateTime?)r[1]).Value;
                x2[index++] = ((double?)r[2]).Value;
            }*/

            /*var serie = new SerieDefinition(@"SyntheticBenchmarkData
ts datetime
value double");
            using(var page = Page.CreateNew(serie, "d:\\aaaaaa.dat")) {
                page.Columns[0].WriteOnly.Write(x1, 0, 100000);
                page.Columns[1].WriteOnly.Write(x2, 0, 100000);

                page.RowCount = (ulong)index;
                page.Save();
                page.Close();
            }*/
            /*
            var copy = new ulong[count];
            values.CopyTo(copy, 0);
            var countbac = count;

            var finalprev = m_prev;
            m_prev = 0;
            offset = 0;
            count = countbac;
            var xx = new ulong[1000000];
            while(count-- > 0) {
                xx[offset] = m_prev;
                this.InternalGetNext(ref copy[offset++]);
            }
            if(m_prev != finalprev) {
                Console.WriteLine(m_prev);
                Console.WriteLine(finalprev);
                System.Diagnostics.Debugger.Break();
            }
            for(int i = 0; i < countbac; i++) {
                if(values[i] != copy[i]) {
                    Console.WriteLine(i);
                    Console.WriteLine(values[i]);
                    Console.WriteLine(copy[i]);
                    System.Diagnostics.Debugger.Break();
                }
            }
            */

            /*var r = new Random();
            var xx = new long[4096 * 10000];
            for(int i = 0; i < xx.Length; i++)
                xx[i] = i;// r.Next();
            var e = new DataStreams.Writers.WithEncoders.DeltaDelta_DataStreamWriter_Int64_LSB();
            var e2 = new DataStreams.Writers.WithEncoders.DeltaDelta_DataStreamWriter_Int64_LSB2();
            var sou = new DynamicMemoryStream();
            var sou2 = new DynamicMemoryStream();
            e.Init(new[] { sou });
            e2.Init(new[] { sou2 });
            var remaining = xx.Length;
            var pos = 0;
            while(remaining > 0) {
                //e.Write(xx, pos, 4096);
                e2.Write(xx, pos, 4096);
                pos += 4096;
                remaining -= 4096;
            }
            e.Commit();
            e2.Commit();
            e.Flush();
            e2.Flush();
            sou.Position = 0;
            sou2.Position = 0;
            //var dsf = sou.ToArray().SequenceEqual(sou2.ToArray());
            var s = BitMethods.StreamCompareIndex(sou, sou2);
            Console.WriteLine(s);*/


            Console.WriteLine(DateTime.UtcNow - nowtt);
            Console.WriteLine("done");
            Console.ReadLine();
        }

        static void Main(string[] args) {
            a();
        }
    }
}
