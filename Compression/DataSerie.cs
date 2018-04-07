using System;
using System.Text;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;


namespace TimeSeriesDB
{
    /// <summary>
    ///     Represents a serie of data, spanning multiple pages.
    /// </summary>
    public sealed class DataSerie {
        private const int MAX_ROWS_PER_PAGE = 25000; // aka stripe

        #region constructors
        public DataSerie(DataSerieDefinition definition) : base() {
            this.Definition = definition;
            this.Pages = new List<PageInfo>();
            throw new NotImplementedException();
        }
        #endregion

        public readonly DataSerieDefinition Definition;
        public readonly List<PageInfo> Pages;
        /// <summary>
        ///     Assumes MAX_ROWS_PER_PAGE * Pages.Count for non-loaded pages.
        /// </summary>
        public long EstimatedTotalRowCount;

        // make an ISerieAdapter and dump all those calls in there
        public IEnumerable<PageInfo> GetPages() {
            yield return null;
        }
        public Page LoadPage(PageInfo page) {
            return null;
        }
        public Page CreatePage() {
            return null;
        }
        public Page ResumePage(PageInfo page) {
            // resumes the page if it has less than MAX_ROWS_PER_PAGE, otherwise loads in read-only mode.
            return null;
        }

        public class PageInfo {
            public Page Page { get; private set; }
            public string File { get; private set; } // maybe not part of the interface, but rather on specific implementations

            #region constructors
            public PageInfo() {
            }
            #endregion
        }
    }
}
