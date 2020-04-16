using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace DispatchExpert.Model.Dispatch.NewDispatchEngine {
    public static class IEnumerableExtensions {
        #region static IEnumerable<T>.IndexOf()
        public static int IndexOf<T>(this IEnumerable<T> source, T item) {
            if(source == null)
                throw new ArgumentNullException(nameof(source));

            if(source is IList list)
                return list.IndexOf(item);

            var comparer = EqualityComparer<T>.Default;
            int index = 0;
            foreach(var x in source) {
                if(comparer.Equals(x, item))
                    return index;
                index++;
            }
            return -1;
        }
        public static int IndexOf<T>(this IEnumerable<T> source, int startIndex, T item) {
            if(source == null)
                throw new ArgumentNullException(nameof(source));
            if(startIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(startIndex));

            var comparer = EqualityComparer<T>.Default;

            if(source is IList<T> ilist) {
                var max = ilist.Count;
                for(int i = startIndex; i < max; i++) {
                    if(comparer.Equals(ilist[i], item))
                        return i;
                }
                return -1;
            }

            int index = 0;
            foreach(var x in source) {
                if(index >= startIndex && comparer.Equals(x, item))
                    return index;
                index++;
            }
            return -1;
        }
        public static int IndexOf<T>(this IEnumerable<T> source, int startIndex, int count, T item) {
            if(source == null)
                throw new ArgumentNullException(nameof(source));
            if(startIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(startIndex));
            if(count <= 0)
                return -1;

            var max = startIndex + count;
            var comparer = EqualityComparer<T>.Default;

            if(source is IList<T> ilist) {
                for(int i = startIndex; i < max; i++) {
                    if(comparer.Equals(ilist[i], item))
                        return i;
                }
                return -1;
            }

            int index = 0;
            foreach(var x in source) {
                if(index >= startIndex) {
                    if(comparer.Equals(x, item))
                        return index;
                    if(index >= max)
                        break;
                }
                index++;
            }
            return -1;
        }
        public static int IndexOf<T>(this IEnumerable<T> source, Predicate<T> match) {
            if(source == null)
                throw new ArgumentNullException(nameof(source));

            if(source is List<T> list)
                return list.FindIndex(match);
            if(source is T[] array)
                return Array.FindIndex(array, match);
            if(source is IList<T> ilist) {
                var max = ilist.Count;
                for(int i = 0; i < max; i++) {
                    if(match(ilist[i]))
                        return i;
                }
                return -1;
            }

            int index = 0;
            foreach(var item in source) {
                if(match(item))
                    return index;
                index++;
            }
            return -1;
        }
        public static int IndexOf<T>(this IEnumerable<T> source, int startIndex, Predicate<T> match) {
            if(source == null)
                throw new ArgumentNullException(nameof(source));
            if(startIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(startIndex));

            if(source is List<T> list)
                return list.FindIndex(startIndex, match);
            if(source is T[] array)
                return Array.FindIndex(array, startIndex, match);
            if(source is IList<T> ilist) {
                var max = ilist.Count;
                for(int i = startIndex; i < max; i++) {
                    if(match(ilist[i]))
                        return i;
                }
                return -1;
            }

            int index = 0;
            foreach(var item in source) {
                if(index >= startIndex && match(item))
                    return index;
                index++;
            }
            return -1;
        }
        public static int IndexOf<T>(this IEnumerable<T> source, int startIndex, int count, Predicate<T> match) {
            if(source == null)
                throw new ArgumentNullException(nameof(source));
            if(startIndex < 0)
                throw new ArgumentOutOfRangeException(nameof(startIndex));
            if(count <= 0)
                return -1;

            if(source is List<T> list)
                return list.FindIndex(startIndex, count, match);
            if(source is T[] array)
                return Array.FindIndex(array, startIndex, count, match);

            var max = startIndex + count;

            if(source is IList<T> ilist) {
                for(int i = startIndex; i < max; i++) {
                    if(match(ilist[i]))
                        return i;
                }
                return -1;
            }

            int index = 0;
            foreach(var item in source) {
                if(index >= startIndex) {
                    if(match(item))
                        return index;
                    if(index >= max)
                        break;
                }
                index++;
            }
            return -1;
        }
        #endregion
        #region static IEnumerable<T>.LastIndexOf()
        public static int LastIndexOf<T>(this IEnumerable<T> source, T item) {
            if(source == null)
                throw new ArgumentNullException(nameof(source));

            var comparer = EqualityComparer<T>.Default;

            if(source is IList<T> list) {
                for(int i = list.Count - 1; i >= 0; i--) {
                    if(comparer.Equals(list[i], item))
                        return i;
                }
                return -1;
            }

            int result = -1;
            int index = 0;
            foreach(var x in source) {
                if(comparer.Equals(x, item))
                    result = index;
                index++;
            }
            return result;
        }
        #endregion
        #region static IEnumerable<T>.GroupByAdjacent()
        /// <summary>
        ///     Same as GroupBy(), but will only group with the previous group if appropriate.
        /// </summary>
        public static IEnumerable<Grouping<TKey, T>> GroupByAdjacent<T, TKey>(this IEnumerable<T> source, Func<T, TKey> keySelector) {
            return GroupByAdjacent(source, keySelector, EqualityComparer<TKey>.Default);
        }
        /// <summary>
        ///     Same as GroupBy(), but will only group with the previous group if appropriate.
        /// </summary>
        public static IEnumerable<Grouping<TKey, T>> GroupByAdjacent<T, TKey>(this IEnumerable<T> source, Func<T, TKey> keySelector, EqualityComparer<TKey> comparer) {
            Grouping<TKey, T> group = null;

            foreach(var item in source) {
                var key = keySelector(item);

                if(group == null)
                    group = new Grouping<TKey, T>(key);
                else if(!comparer.Equals(key, group.Key)) {
                    yield return group;
                    group = new Grouping<TKey, T>(key);
                }

                group.Add(item);
            }

            if(group != null)
                yield return group;
        }
        /// <summary>
        ///     Same as GroupBy(), but will only group with the previous group if appropriate.
        /// </summary>
        public static IEnumerable<Grouping<TKey, TElement>> GroupByAdjacent<T, TKey, TElement>(this IEnumerable<T> source, Func<T, TKey> keySelector, Func<T, TElement> elementSelector) {
            return GroupByAdjacent(source, keySelector, elementSelector, EqualityComparer<TKey>.Default);
        }
        /// <summary>
        ///     Same as GroupBy(), but will only group with the previous group if appropriate.
        /// </summary>
        public static IEnumerable<Grouping<TKey, TElement>> GroupByAdjacent<T, TKey, TElement>(this IEnumerable<T> source, Func<T, TKey> keySelector, Func<T, TElement> elementSelector, EqualityComparer<TKey> comparer) {
            Grouping<TKey, TElement> group = null;

            foreach(var item in source) {
                var key = keySelector(item);

                if(group == null)
                    group = new Grouping<TKey, TElement>(key);
                else if(!comparer.Equals(key, group.Key)) {
                    yield return group;
                    group = new Grouping<TKey, TElement>(key);
                }

                group.Add(elementSelector(item));
            }

            if(group != null)
                yield return group;
        }
        /// <summary>
        ///     Same as GroupBy(), but will only group with the previous group if appropriate.
        /// </summary>
        public static IEnumerable<TResult> GroupByAdjacent<T, TKey, TResult>(this IEnumerable<T> source, Func<T, TKey> keySelector, Func<TKey, IEnumerable<T>, TResult> resultSelector) {
            return GroupByAdjacent(source, keySelector, resultSelector, EqualityComparer<TKey>.Default);
        }
        /// <summary>
        ///     Same as GroupBy(), but will only group with the previous group if appropriate.
        /// </summary>
        public static IEnumerable<TResult> GroupByAdjacent<T, TKey, TResult>(this IEnumerable<T> source, Func<T, TKey> keySelector, Func<TKey, IEnumerable<T>, TResult> resultSelector, EqualityComparer<TKey> comparer) {
            Grouping<TKey, T> group = null;

            foreach(var item in source) {
                var key = keySelector(item);

                if(group == null)
                    group = new Grouping<TKey, T>(key);
                else if(!comparer.Equals(key, group.Key)) {
                    yield return resultSelector(group.Key, group);
                    group = new Grouping<TKey, T>(key);
                }

                group.Add(item);
            }

            if(group != null)
                yield return resultSelector(group.Key, group);
        }
        /// <summary>
        ///     Same as GroupBy(), but will only group with the previous group if appropriate.
        /// </summary>
        public static IEnumerable<TResult> GroupByAdjacent<T, TKey, TElement, TResult>(this IEnumerable<T> source, Func<T, TKey> keySelector, Func<T, TElement> elementSelector, Func<TKey, IEnumerable<TElement>, TResult> resultSelector) {
            return GroupByAdjacent(source, keySelector, elementSelector, resultSelector, EqualityComparer<TKey>.Default);
        }
        /// <summary>
        ///     Same as GroupBy(), but will only group with the previous group if appropriate.
        /// </summary>
        public static IEnumerable<TResult> GroupByAdjacent<T, TKey, TElement, TResult>(this IEnumerable<T> source, Func<T, TKey> keySelector, Func<T, TElement> elementSelector, Func<TKey, IEnumerable<TElement>, TResult> resultSelector, EqualityComparer<TKey> comparer) {
            Grouping<TKey, TElement> group = null;

            foreach(var item in source) {
                var key = keySelector(item);

                if(group == null)
                    group = new Grouping<TKey, TElement>(key);
                else if(!comparer.Equals(key, group.Key)) {
                    yield return resultSelector(group.Key, group);
                    group = new Grouping<TKey, TElement>(key);
                }

                group.Add(elementSelector(item));
            }

            if(group != null)
                yield return resultSelector(group.Key, group);
        }
        public class Grouping<TKey, TElement> : IGrouping<TKey, TElement>, IEnumerable<TElement>, IEnumerable, IList<TElement>, ICollection<TElement> {
            private readonly List<TElement> m_elements;
            private readonly TKey m_key;

            public TKey Key => m_key;
            public int Count => m_elements.Count;

            public Grouping(TKey key, List<TElement> elements = null) {
                m_key = key;
                m_elements = elements ?? new List<TElement>();
            }

            bool ICollection<TElement>.IsReadOnly => true;

            public TElement this[int index] {
                get => m_elements[index];
                set => throw new NotSupportedException();
            }

            internal void Add(TElement element) {
                m_elements.Add(element);
            }
            internal void Insert(int index, TElement element) {
                m_elements.Insert(index, element);
            }
            internal bool Remove(TElement element) {
                return m_elements.Remove(element);
            }
            internal void RemoveAt(int index) {
                m_elements.RemoveAt(index);
            }

            public IEnumerator<TElement> GetEnumerator() {
                return m_elements.GetEnumerator();
            }
            IEnumerator IEnumerable.GetEnumerator() {
                return this.GetEnumerator();
            }
            void ICollection<TElement>.Add(TElement item) {
                throw new NotSupportedException();
            }
            void ICollection<TElement>.Clear() {
                throw new NotSupportedException();
            }
            public bool Contains(TElement item) {
                return m_elements.Contains(item);
            }
            public void CopyTo(TElement[] array, int arrayIndex) {
                m_elements.CopyTo(array, arrayIndex);
            }
            bool ICollection<TElement>.Remove(TElement item) {
                throw new NotSupportedException();
            }
            public int IndexOf(TElement item) {
                return m_elements.IndexOf(item);
            }
            void IList<TElement>.Insert(int index, TElement item) {
                throw new NotSupportedException();
            }
            void IList<TElement>.RemoveAt(int index) {
                throw new NotSupportedException();
            }
        }
        #endregion
        #region static IEnumerable<T>.GroupByAdjacentWhile()
        /// <summary>
        ///     Same as GroupBy(), but will only group with the previous group if appropriate.
        /// </summary>
        public static IEnumerable<List<T>> GroupByAdjacentWhile<T>(this IEnumerable<T> source, Func<List<T>, T, bool> accumulate) {
            List<T> group = null;

            foreach(var item in source) {
                if(group == null)
                    group = new List<T>();
                else if(!accumulate(group, item)) {
                    yield return group;
                    group = new List<T>();
                }

                group.Add(item);
            }

            if(group != null)
                yield return group;
        }
        /// <summary>
        ///     Same as GroupBy(), but will only group with the previous group if appropriate.
        /// </summary>
        public static IEnumerable<Grouping<TKey, T>> GroupByAdjacentWhile<T, TKey>(this IEnumerable<T> source, Func<T, TKey> keySelector, Func<Grouping<TKey, T>, T, bool> accumulate) {
            if(accumulate == null)
                return GroupByAdjacent(source, keySelector);
            return GroupByAdjacentWhile(source, keySelector, accumulate, EqualityComparer<TKey>.Default);
        }
        /// <summary>
        ///     Same as GroupBy(), but will only group with the previous group if appropriate.
        /// </summary>
        public static IEnumerable<Grouping<TKey, T>> GroupByAdjacentWhile<T, TKey>(this IEnumerable<T> source, Func<T, TKey> keySelector, Func<Grouping<TKey, T>, T, bool> accumulate, EqualityComparer<TKey> comparer) {
            Grouping<TKey, T> group = null;

            foreach(var item in source) {
                var key = keySelector(item);

                if(group == null)
                    group = new Grouping<TKey, T>(key);
                else if(!comparer.Equals(key, group.Key) || !accumulate(group, item)) {
                    yield return group;
                    group = new Grouping<TKey, T>(key);
                }

                group.Add(item);
            }

            if(group != null)
                yield return group;
        }
        /// <summary>
        ///     Same as GroupBy(), but will only group with the previous group if appropriate.
        /// </summary>
        public static IEnumerable<Grouping<TKey, TElement>> GroupByAdjacentWhile<T, TKey, TElement>(this IEnumerable<T> source, Func<T, TKey> keySelector, Func<T, TElement> elementSelector, Func<Grouping<TKey, TElement>, TElement, bool> accumulate) {
            if(accumulate == null)
                return GroupByAdjacent(source, keySelector, elementSelector);
            return GroupByAdjacentWhile(source, keySelector, elementSelector, accumulate, EqualityComparer<TKey>.Default);
        }
        /// <summary>
        ///     Same as GroupBy(), but will only group with the previous group if appropriate.
        /// </summary>
        public static IEnumerable<Grouping<TKey, TElement>> GroupByAdjacentWhile<T, TKey, TElement>(this IEnumerable<T> source, Func<T, TKey> keySelector, Func<T, TElement> elementSelector, Func<Grouping<TKey, TElement>, TElement, bool> accumulate, EqualityComparer<TKey> comparer) {
            Grouping<TKey, TElement> group = null;

            foreach(var item in source) {
                var key = keySelector(item);
                var element = elementSelector(item);

                if(group == null)
                    group = new Grouping<TKey, TElement>(key);
                else if(!comparer.Equals(key, group.Key) || !accumulate(group, element)) {
                    yield return group;
                    group = new Grouping<TKey, TElement>(key);
                }

                group.Add(element);
            }

            if(group != null)
                yield return group;
        }
        #endregion
        #region static IEnumerable<T>.Split()
        /// <summary>
        ///     Same as string.Split(), but for enumerables.
        ///     ex: [abcde].Split(c => c == 'b') = {[a], [cde]}
        /// </summary>
        public static IEnumerable<List<T>> Split<T>(this IEnumerable<T> source, Predicate<T> separator, StringSplitOptions options = StringSplitOptions.None) {
            if(options == StringSplitOptions.None) {
                var res = new List<T>();
                foreach(var item in source) {
                    if(separator(item)) {
                        yield return res;
                        res = new List<T>();
                    } else
                        res.Add(item);
                }
                yield return res;
            } else if(options == StringSplitOptions.RemoveEmptyEntries) {
                List<T> res = null;
                foreach(var item in source) {
                    if(separator(item)) {
                        if(res != null) {
                            yield return res;
                            res = null;
                        }
                    } else {
                        if(res == null)
                            res = new List<T>();
                        res.Add(item);
                    }
                }
                if(res != null)
                    yield return res;
            } else
                throw new NotSupportedException("Unsupported StringSplitOptions");
        }
        /// <summary>
        ///     Same as string.Split(), but for enumerables.
        ///     ex: [abcde].Split(c => c == 'b') = {[a], [cde]}
        /// </summary>
        public static IEnumerable<List<T>> Split<T>(this IEnumerable<T> source, Func<T, int, bool> separator, StringSplitOptions options = StringSplitOptions.None) {
            int index = 0;

            if(options == StringSplitOptions.None) {
                var res = new List<T>();
                foreach(var item in source) {
                    if(separator(item, index++)) {
                        yield return res;
                        res = new List<T>();
                    } else
                        res.Add(item);
                }
                yield return res;
            } else if(options == StringSplitOptions.RemoveEmptyEntries) {
                List<T> res = null;
                foreach(var item in source) {
                    if(separator(item, index++)) {
                        if(res != null) {
                            yield return res;
                            res = null;
                        }
                    } else {
                        if(res == null)
                            res = new List<T>();
                        res.Add(item);
                    }
                }
                if(res != null)
                    yield return res;
            } else
                throw new NotSupportedException("Unsupported StringSplitOptions");
        }
        #endregion
        #region static IEnumerable<T>.SlidingWindow()
        /// <summary>
        ///     Returns a sliding window enumerator.
        ///     Only returns results when the full windowSize is read (by default).
        ///     ex: [abcde].SlidingWindow(2,1) = {[ab], [bc], [cd], [de]}
        ///         [abcde].SlidingWindow(2,2) = {[ab], [cd]}
        ///         [abcde].SlidingWindow(6,1) = {}
        /// </summary>
        public static IEnumerable<T[]> SlidingWindow<T>(this IEnumerable<T> source, int windowSize, int increment, SlidingWindowOptions options = SlidingWindowOptions.None) {
            if(windowSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(windowSize));
            if(increment <= 0)
                throw new ArgumentOutOfRangeException(nameof(increment));

            int writeIndex = 0;
            var res = new T[windowSize];
            var enumerator = source.GetEnumerator();

            if(!options.HasFlag(SlidingWindowOptions.AllowPartialWindow)) {
                if(increment < windowSize) {
                    if(options.HasFlag(SlidingWindowOptions.DontReuseInstance)) {
                        while(true) {
                            while(writeIndex < windowSize && enumerator.MoveNext())
                                res[writeIndex++] = enumerator.Current;

                            if(writeIndex < windowSize)
                                yield break;

                            var _new = new T[windowSize];
                            Array.Copy(res, increment, _new, 0, windowSize - increment); //Buffer.BlockCopy()

                            yield return res;

                            res = _new;
                            writeIndex = windowSize - increment;
                        }
                    } else {
                        while(true) {
                            while(writeIndex < windowSize && enumerator.MoveNext())
                                res[writeIndex++] = enumerator.Current;

                            if(writeIndex < windowSize)
                                yield break;
                            yield return res;

                            Array.Copy(res, increment, res, 0, windowSize - increment); //Buffer.BlockCopy()
                            writeIndex = windowSize - increment;
                        }
                    }
                } else {
                    int skipCount = increment - windowSize;
                    while(true) {
                        while(writeIndex < windowSize && enumerator.MoveNext())
                            res[writeIndex++] = enumerator.Current;

                        if(writeIndex < windowSize)
                            yield break;
                        yield return res;

                        for(int i = 0; i < skipCount; i++)
                            if(!enumerator.MoveNext())
                                yield break;

                        writeIndex = 0;

                        if(options.HasFlag(SlidingWindowOptions.DontReuseInstance))
                            res = new T[windowSize];
                    }
                }
            } else { // allow partial windows
                if(increment < windowSize) {
                    if(options.HasFlag(SlidingWindowOptions.DontReuseInstance)) {
                        while(true) {
                            while(writeIndex < windowSize && enumerator.MoveNext())
                                res[writeIndex++] = enumerator.Current;

                            if(writeIndex == 0)
                                yield break;

                            var _new = new T[windowSize];

                            var copy = writeIndex - increment;
                            if(copy > 0) {
                                Array.Copy(res, increment, _new, 0, copy); //Buffer.BlockCopy()
                                writeIndex = copy;
                            } else
                                writeIndex = 0;

                            yield return res;

                            res = _new;
                        }
                    } else {
                        int prevWriteIndex = 0;
                        while(true) {
                            while(writeIndex < windowSize && enumerator.MoveNext())
                                res[writeIndex++] = enumerator.Current;

                            if(writeIndex == 0)
                                yield break;

                            var remainder = prevWriteIndex - writeIndex;
                            if(remainder > 0)
                                Array.Clear(res, writeIndex, remainder);

                            yield return res;

                            prevWriteIndex = writeIndex;

                            var copy = writeIndex - increment;
                            if(copy > 0) {
                                Array.Copy(res, increment, res, 0, copy); //Buffer.BlockCopy()
                                writeIndex = copy;
                            } else
                                writeIndex = 0;
                        }
                    }
                } else {
                    int skipCount = increment - windowSize;
                    if(options.HasFlag(SlidingWindowOptions.DontReuseInstance)) {
                        while(true) {
                            while(writeIndex < windowSize && enumerator.MoveNext())
                                res[writeIndex++] = enumerator.Current;

                            if(writeIndex == 0)
                                yield break;

                            yield return res;

                            for(int i = 0; i < skipCount; i++)
                                if(!enumerator.MoveNext())
                                    yield break;

                            writeIndex = 0;
                            res = new T[windowSize];
                        }
                    } else {
                        int prevWriteIndex = 0;
                        while(true) {
                            while(writeIndex < windowSize && enumerator.MoveNext())
                                res[writeIndex++] = enumerator.Current;

                            if(writeIndex == 0)
                                yield break;

                            var remainder = prevWriteIndex - writeIndex;
                            if(remainder > 0)
                                Array.Clear(res, writeIndex, remainder);

                            yield return res;

                            for(int i = 0; i < skipCount; i++)
                                if(!enumerator.MoveNext())
                                    yield break;

                            prevWriteIndex = writeIndex;
                            writeIndex = 0;
                        }
                    }
                }
            }
        }
        [Flags]
        public enum SlidingWindowOptions {
            None = 0,
            /// <summary>
            ///     The returned array is a new instance for every returned item.
            ///     If this is not set, then the same instance is returned every time.
            /// </summary>
            DontReuseInstance = 1,
            /// <summary>
            ///     Allows returning sliding windows that are partially filled.
            ///     ex: [1,2,null,null]
            /// </summary>
            AllowPartialWindow = 2,
        }
        #endregion
        #region static IEnumerable<T>.SelectMany()
        /// <summary>
        ///     Shortcut for this.SelectMany(_ => _);
        /// </summary>
        public static IEnumerable<T> SelectMany<T>(this IEnumerable<IEnumerable<T>> source) {
            //return source.SelectMany(_ => _);

            // runs faster without any lambdas
            foreach(var item in source) {
                foreach(var subitem in item)
                    yield return subitem;
            }
        }
        #endregion
        #region static IEnumerable<T>.OrderBy()
        /// <summary>
        ///     Shortcut for this.OrderBy(_ => _);
        /// </summary>
        public static IEnumerable<T> OrderBy<T>(this IEnumerable<T> source) {
            return source.OrderBy(_ => _);
        }
        #endregion
        #region static IEnumerable.IsEmpty()
        public static bool IsEmpty(this IEnumerable source) {
            //return !source.Any();

            if(source == null)
                throw new ArgumentNullException(nameof(source));

            // 'manual' using() to avoid reading any value
            IEnumerator enumerator = null;
            try {
                enumerator = source.GetEnumerator();

                if(enumerator.MoveNext())
                    return false;
            } finally {
                if(enumerator is IDisposable disposable)
                    disposable.Dispose();
            }

            return true;
        }
        #endregion
        #region static IEnumerable<T>.None()
        /// <summary>
        ///     More readable "! list.Any(condition)"
        ///     Returns true if list is empty.
        /// </summary>
        public static bool None<T>(this IEnumerable<T> source, Func<T, bool> predicate) {
            return !source.Any(predicate);
        }
        #endregion
        #region static IEnumerable<T>.Shuffle()
        public static List<T> Shuffle<T>(this IEnumerable<T> source, Random random = null) {
            var copy = source.ToList();
            var count = copy.Count;
            var max = count - 1;

            if(random == null)
                random = new Random();

            for(int i = 0; i < max; i++) {
                var rng = random.Next(i, count);
                var swap = copy[i];
                copy[i] = copy[rng];
                copy[rng] = swap;
            }
            return copy;
        }
        #endregion
        #region static IEnumerable<T>.ToBuffer()
        /// <summary>
        ///     Same as Enumerable.ToArray(), but without requiring the final Array.Copy().
        ///     Essentially this is the same as Enumerable.ToList(), but giving direct access to the underlying array. 
        /// </summary>
        public static Buffer<T> ToBuffer<T>(this IEnumerable<T> source) {
            return new Buffer<T>(source);
        }
        #endregion
        #region static IEnumerable<T>.ToReadOnlyBuffer()
        /// <summary>
        ///     Same as Enumerable.ToArray(), but without requiring the final Array.Copy().
        ///     Essentially this is the same as Enumerable.ToList(), but giving direct access to the underlying array. 
        /// </summary>
        public static ReadOnlyBuffer<T> ToReadOnlyBuffer<T>(this IEnumerable<T> source) {
            return new ReadOnlyBuffer<T>(source);
        }
        #endregion
        #region static IEnumerable<T>.ReverseFast()
        /// <summary>
        ///     A more efficient Enumerable.Reverse() implementation that does not buffer and directly reads the source collection if possible.
        ///     If the source collection is modified while enumerating, the code only does a "for(i=source.Count-1; i--) yield source[i]", so it might work.
        /// </summary>
        public static IEnumerable<T> ReverseFast<T>(this IEnumerable<T> source) {
            if(source is T[] array) {
                for(int i = array.Length - 1; i >= 0; i--)
                    yield return array[i]; // this runs faster than using IList<T>
            } else if(source is IList<T> list) {
                for(int i = list.Count - 1; i >= 0; i--)
                    yield return list[i];
            } else if(source is IReadOnlyList<T> readonly_list) {
                for(int i = readonly_list.Count - 1; i >= 0; i--)
                    yield return readonly_list[i];
            } else {
                var buffer = source.ToReadOnlyBuffer();
                for(int i = buffer.Count - 1; i >= 0; i--)
                    yield return buffer.Items[i];
            }
        }
        #endregion

        #region static Array<T>.Move()
        /// <summary>
        ///     Moves the items [fromIndex + count] to [toIndex + count] position.
        ///     ex: move([0,1,2,3,4], 1, 0, 4) = [1,2,3,4,0]
        /// </summary>
        public static void Move<T>(this T[] array, int fromIndex, int toIndex, int count) {
            const int MAX_BUFFER = 512;

            if(fromIndex == toIndex)
                return;

            var remaining = count;
            var buffer = new T[Math.Min(count, MAX_BUFFER)];

            while(remaining > 0) {
                var size = Math.Min(remaining, MAX_BUFFER);

                Array.Copy(array, toIndex, buffer, 0, size);
                Array.Copy(array, fromIndex, array, toIndex, size);

                fromIndex += size;
                toIndex   += size;
                remaining -= size;
            }
        }
        #endregion

        #region static GeneratePermutations()
        /// <summary>
        ///     Generates all the permutations possible for the given source such that all possible orderings are returned.
        ///     Use result.ToArray() if you want to store the results, as the same instance is returned every time.
        ///     Outputs 'n!' (factorial) results.
        ///     ex: [A,B,C] -> {[A,B,C], [A,B,C], [B,A,C], [B,C,A], [C,A,B], [C,B,A]}
        /// </summary>
        /// <param name="filter">Filters sub results, short-circuiting the needless processings.</param>
        /// <param name="selector">Generates sub-items for one dimension/level/bucket. default = remaining.Where(o => o.Count > 0)</param>
        public static IEnumerable<T[]> GeneratePermutations<T>(IEnumerable<T> source, PermutationEnumerator<T>.SubResultFilter filter = null, PermutationEnumerator<T>.SubResultSelector selector = null) {
            return new PermutationEnumerator<T>(source, filter, selector).List();
        }
        public sealed class PermutationEnumerator<T> {
            private readonly List<T> m_source;
            private readonly T[] m_current;
            private readonly int m_count;
            private int m_currentIndex = 0;
            private readonly SubResultFilter m_filter;
            private readonly SubResultSelector m_selector;
            private readonly ItemToken[] m_remaining;

            /// <param name="filter">Filters sub results, short-circuiting the needless processings.</param>
            /// <param name="selector">Generates sub-items for one dimension/level/bucket. default = remaining.Where(o => o.Count > 0)</param>
            public PermutationEnumerator(IEnumerable<T> source, SubResultFilter filter = null, SubResultSelector selector = null) {
                m_source = source as List<T> ?? source.ToList();
                m_count = m_source.Count;
                m_current = new T[m_count];
                m_filter = filter;
                m_selector = selector;

                m_remaining = m_source
                    .GroupBy(o => o)
                    .Select(o => new ItemToken() { Item = o.Key, Count = o.Count() })
                    .ToArray();
            }
            public IEnumerable<T[]> List() {
                m_currentIndex = 0;
                if(m_count == 0)
                    return Enumerable.Empty<T[]>();

                // implement 4 times because permutations can generate a ton of results
                // so any speed boost will matter

                if(m_filter == null) {
                    return m_selector == null ?
                        this.ListImplementation_NoFilterNoSelector() :
                        this.ListImplementation_NoFilterSelector();
                } else {
                    return m_selector == null ?
                        this.ListImplementation_FilterNoSelector() :
                        // would be weird/inefficient to both have a selector and a filter (and kind of redundant), but who knows
                        this.ListImplementation_FilterSelector();
                }
            }
            private IEnumerable<T[]> ListImplementation_NoFilterNoSelector() {
                foreach(var item in m_remaining.Where(o => o.Count > 0).ToList()) {
                    m_current[m_currentIndex++] = item.Item;
                    item.Count--;

                    if(m_currentIndex < m_count) {
                        // recurse until last layer
                        foreach(var x in this.ListImplementation_NoFilterNoSelector())
                            yield return x;
                    } else
                        // last layer
                        yield return m_current;

                    m_currentIndex--;
                    item.Count++;
                }
            }
            private IEnumerable<T[]> ListImplementation_FilterNoSelector() {
                foreach(var item in m_remaining.Where(o => o.Count > 0).ToList()) {
                    m_current[m_currentIndex++] = item.Item;
                    item.Count--;

                    if(m_filter(m_current, m_currentIndex)) {
                        if(m_currentIndex < m_count) {
                            // recurse until last layer
                            foreach(var x in this.ListImplementation_FilterNoSelector())
                                yield return x;
                        } else
                            // last layer
                            yield return m_current;
                    }

                    m_currentIndex--;
                    item.Count++;
                }
            }
            private IEnumerable<T[]> ListImplementation_NoFilterSelector() {
                foreach(var item in m_selector(m_remaining, m_current, m_currentIndex).ToList()) {
                    m_current[m_currentIndex++] = item.Item;
                    item.Count--;

                    if(m_currentIndex < m_count) {
                        // recurse until last layer
                        foreach(var x in this.ListImplementation_NoFilterSelector())
                            yield return x;
                    } else
                        // last layer
                        yield return m_current;

                    m_currentIndex--;
                    item.Count++;
                }
            }
            private IEnumerable<T[]> ListImplementation_FilterSelector() {
                foreach(var item in m_selector(m_remaining, m_current, m_currentIndex).ToList()) {
                    m_current[m_currentIndex++] = item.Item;
                    item.Count--;

                    if(m_filter(m_current, m_currentIndex)) {
                        if(m_currentIndex < m_count) {
                            // recurse until last layer
                            foreach(var x in this.ListImplementation_FilterSelector())
                                yield return x;
                        } else
                            // last layer
                            yield return m_current;
                    }

                    m_currentIndex--;
                    item.Count++;
                }
            }

            public class ItemToken {
                public T Item;
                public int Count;
                public override string ToString() {
                    return string.Format("[{0}] {1}", this.Count, this.Item);
                }
            }

            public delegate bool SubResultFilter(T[] result, int len);
            public delegate IEnumerable<ItemToken> SubResultSelector(ItemToken[] remaining, T[] result, int len);
        }
        #endregion
        #region static GenerateCombinations()
        /// <summary>
        ///     Generates all the combinations possible for the given source.
        ///     This will maintain the ordering.
        ///     For performance reasons, the same instance will be returned continually, so clone it if needed.
        ///     Outputs "item_counts[0] * item_counts[1] * ..." results (ie: multiplicative product).
        ///     
        ///     ex: this([3,2]) = { [0,0], [0,1], [1,0], [1,1], [2,0], [2,1] }
        ///     ex: this({[A,B], [C,D]}) -> {[A,C], [A,D], [B,C], [B,D]}
        /// </summary>
        public static IEnumerable<int[]> GenerateCombinations(IEnumerable<int> item_counts) {
            bool found;
            int[] counts = item_counts.ToArray();
            int[] path = new int[counts.Length];
            int max = counts.Length - 1;

            yield return path;

            for(int i = 0; i < counts.Length; i++)
                counts[i]--;

            do {
                found = false;
                int dimension = max;
                while(dimension >= 0) {
                    if(path[dimension] < counts[dimension]) {
                        path[dimension]++;
                        while(dimension < max)
                            path[++dimension] = 0;
                        yield return path;
                        found = true;
                        break;
                    } else
                        dimension--;
                }
            } while(found);
        }
        /// <summary>
        ///     Generates all the combinations possible for the given source.
        ///     This will maintain the ordering.
        ///     For performance reasons, the same instance will be returned continually, so clone it if needed.
        ///     Outputs "item_counts[0] * item_counts[1] * ..." results (ie: multiplicative product).
        ///     
        ///     ex: this({[A,B], [C,D]}) -> {[A,C], [A,D], [B,C], [B,D]}
        /// </summary>
        public static IEnumerable<T[]> GenerateCombinations<T>(IEnumerable<T[]> source) {
            bool found;
            T[][] values = source.ToArray();
            int[] counts = new int[values.Length];
            T[] path = new T[values.Length];
            int[] pathIndex = new int[counts.Length];
            int max = values.Length - 1;

            for(int i = 0; i < values.Length; i++) {
                var v = values[i];
                path[i] = v[0];
                counts[i] = v.Length - 1;
            }

            yield return path;

            do {
                found = false;
                int dimension = max;
                while(dimension >= 0) {
                    if(pathIndex[dimension] < counts[dimension]) {
                        var index = pathIndex[dimension] + 1;
                        pathIndex[dimension] = index;
                        path[dimension] = values[dimension][index];
                        while(dimension < max) {
                            pathIndex[++dimension] = 0;
                            path[dimension] = values[dimension][0];
                        }
                        yield return path;
                        found = true;
                        break;
                    } else
                        dimension--;
                }
            } while(found);
        }
        /// <summary>
        ///     Generates all the combinations possible for the given source.
        ///     This will maintain the ordering.
        ///     For performance reasons, the same instance will be returned continually, so clone it if needed.
        ///     Outputs "item_counts[0] * item_counts[1] * ..." results (ie: multiplicative product).
        ///     
        ///     ex: {[A,B], [C,D]} -> {[A,C], [A,D], [B,C], [B,D]}
        /// </summary>
        /// <param name="filter">Filters sub results, short-circuiting the needless processings.</param>
        /// <param name="selector">Generates sub-items for one dimension/level/bucket.</param>
        public static IEnumerable<T[]> GenerateCombinations<T>(IEnumerable<T[]> source, CombinationEnumerator<T>.SubResultFilter filter = null, CombinationEnumerator<T>.SubResultSelector selector = null) {
            return new CombinationEnumerator<T>(source, filter, selector).List();
        }
        public sealed class CombinationEnumerator<T> {
            private readonly T[][] m_sourceBackup;
            private readonly int m_dimensions;
            private readonly T[] m_result;
            private readonly SubResultFilter m_filter;
            private readonly SubResultSelector m_selector;

            public CombinationEnumerator(IEnumerable<T[]> source, SubResultFilter filter = null, SubResultSelector selector = null) {
                m_sourceBackup = source as T[][] ?? source.ToArray();
                m_dimensions = m_sourceBackup.Length;
                m_result = new T[m_sourceBackup.Length];
                m_filter = filter;
                m_selector = selector;
            }
            public IEnumerable<T[]> List() {
                if(m_sourceBackup.All(o => o == null || o.Length == 0))
                    return Enumerable.Empty<T[]>();

                // implement 4 times because combinations can generate a ton of results
                // so any speed boost will matter

                if(m_filter == null) {
                    return m_selector == null ?
                        this.ListImplementation_NoFilterNoSelector() :
                        this.ListImplementation_NoFilterSelector(0);
                } else {
                    return m_selector == null ?
                        this.ListImplementation_FilterNoSelector(0) :
                        // would be weird/inefficient to both have a selector and a filter (and kind of redundant), but who knows
                        this.ListImplementation_FilterSelector(0);
                }
            }
            private IEnumerable<T[]> ListImplementation_NoFilterNoSelector() {
                return GenerateCombinations(m_sourceBackup);
            }
            private IEnumerable<T[]> ListImplementation_FilterNoSelector(int depth) {
                var dimension = m_sourceBackup[depth];
                var dimension_size = dimension == null ? 0 : dimension.Length;

                if(dimension_size == 0) {
                    if(depth + 1 < m_dimensions) {
                        // recurse until last layer
                        foreach(var item in this.ListImplementation_FilterNoSelector(depth + 1))
                            yield return item;
                    } else
                        // last layer
                        yield return m_result;
                } else {
                    for(int i = 0; i < dimension_size; i++) {
                        m_result[depth] = dimension[i];

                        if(!m_filter(m_result, depth + 1))
                            continue;

                        if(depth + 1 < m_dimensions) {
                            // recurse until last layer
                            foreach(var item in this.ListImplementation_FilterNoSelector(depth + 1))
                                yield return item;
                        } else
                            // last layer
                            yield return m_result;
                    }
                }
            }
            private IEnumerable<T[]> ListImplementation_NoFilterSelector(int depth) {
                var dimension = m_sourceBackup[depth];

                if(dimension == null || dimension.Length == 0) {
                    if(depth + 1 < m_dimensions) {
                        // recurse until last layer
                        foreach(var item in this.ListImplementation_NoFilterSelector(depth + 1))
                            yield return item;
                    } else
                        // last layer
                        yield return m_result;
                } else {
                    foreach(var current in m_selector(dimension, m_result, depth)) {
                        m_result[depth] = current;

                        if(depth + 1 < m_dimensions) {
                            // recurse until last layer
                            foreach(var item in this.ListImplementation_NoFilterSelector(depth + 1))
                                yield return item;
                        } else
                            // last layer
                            yield return m_result;
                    }
                }
            }
            private IEnumerable<T[]> ListImplementation_FilterSelector(int depth) {
                var dimension = m_sourceBackup[depth];

                if(dimension == null || dimension.Length == 0) {
                    if(depth + 1 < m_dimensions) {
                        // recurse until last layer
                        foreach(var item in this.ListImplementation_FilterSelector(depth + 1))
                            yield return item;
                    } else
                        // last layer
                        yield return m_result;
                } else {
                    foreach(var current in m_selector(dimension, m_result, depth)) {
                        m_result[depth] = current;

                        if(!m_filter(m_result, depth + 1))
                            continue;

                        if(depth + 1 < m_dimensions) {
                            // recurse until last layer
                            foreach(var item in this.ListImplementation_FilterSelector(depth + 1))
                                yield return item;
                        } else
                            // last layer
                            yield return m_result;
                    }
                }
            }

            /// <param name="len">The number of result[] that are written to. The current item is included in result[].</param>
            public delegate bool SubResultFilter(T[] result, int len);
            /// <param name="len">The number of result[] that are written to. The current item is included in result[].</param>
            public delegate IEnumerable<T> SubResultSelector(IEnumerable<T> current_dimension, T[] result, int len);
        }
        #endregion
    }

    /// <summary>
    ///     Same as Enumerable.ToArray(), but without requiring the final Array.Copy().
    ///     Essentially this is the same as Enumerable.ToList(), but giving direct access to the underlying array.
    /// </summary>
    public readonly struct ReadOnlyBuffer<T> : IReadOnlyList<T>, IReadOnlyCollection<T>, IList<T>, ICollection<T>, IEnumerable<T> { // could have a potential speedup by using 'ref'
        public readonly T[] Items;
        public readonly int Count;
        #region constructors
        public ReadOnlyBuffer(IEnumerable<T> source) {
            const int DEFAULT_ARRAY_START_SIZE = 4;

            T[] array = null;
            int index;
            if(source is ICollection<T> collection) {
                index = collection.Count;
                if(index > 0) {
                    array = new T[index];
                    collection.CopyTo(array, 0);
                }
            } else {
                index = 0;
                foreach(var current in source) {
                    if(array == null)
                        array = new T[DEFAULT_ARRAY_START_SIZE];
                    else if(array.Length == index) {
                        var temp = new T[checked(index * 2)];
                        Array.Copy(array, 0, temp, 0, index);
                        array = temp;
                    }
                    array[index++] = current;
                }
            }
            this.Items = array;
            this.Count = index;
        }
        #endregion
        #region ToArray()
        public T[] ToArray() {
            var count = this.Count;
            if(count == 0)
                return new T[0];

            var items = this.Items;
            if(items.Length == count)
                return this.Items;

            var res = new T[count];
            Array.Copy(items, 0, res, 0, count);
            return res;
        }
        #endregion
        #region CopyTo()
        public void CopyTo(T[] array, int startIndex) {
            Array.Copy(this.Items, 0, array, startIndex, this.Count);
        }
        #endregion
        #region Contains()
        public bool Contains(T item) {
            return this.IndexOf(item) >= 0;
        }
        #endregion
        #region IndexOf()
        public int IndexOf(T item) {
            return Array.IndexOf(this.Items, item, 0, this.Count);
        }
        public int IndexOf(T item, int startIndex) {
            if(startIndex > this.Count)
                throw new ArgumentOutOfRangeException(nameof(startIndex));

            return Array.IndexOf(this.Items, item, startIndex, this.Count - startIndex);
        }
        public int IndexOf(T item, int startIndex, int count) {
            if(startIndex > this.Count)
                throw new ArgumentOutOfRangeException(nameof(startIndex));
            if(count < 0 || startIndex > this.Count - count)
                throw new ArgumentOutOfRangeException(nameof(count));

            return Array.IndexOf(this.Items, item, startIndex, count);
        }
        #endregion
        #region GetEnumerator()
        /// <summary>
        ///     This is included for convenience, but you probably meant to use "Enumerable.ToList()" rather than "new ReadOnlyBuffer()" if you call this.
        ///     If you want speed, access this.Items directly.
        /// </summary>
        public IEnumerator<T> GetEnumerator() {
            for(int i = 0; i < this.Count; i++)
                yield return this.Items[i];
        }
        /// <summary>
        ///     This is included for convenience, but you probably meant to use "Enumerable.ToList()" rather than "new ReadOnlyBuffer()" if you call this.
        ///     If you want speed, access this.Items directly.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() {
            for(int i = 0; i < this.Count; i++)
                yield return this.Items[i];
        }
        #endregion
        #region interfaces implementations
        T IReadOnlyList<T>.this[int index] {
            get {
                if(index >= this.Count)
                    throw new IndexOutOfRangeException();
                return this.Items[index];
            }
        }
        T IList<T>.this[int index] {
            get {
                if(index >= this.Count)
                    throw new IndexOutOfRangeException();
                return this.Items[index];
            }
            set {
                throw new NotSupportedException();
                // technically this makes it not { IReadOnlyList<T>, IReadOnlyCollection<T> }
                //if(index >= this.Count)
                //    throw new IndexOutOfRangeException();
                //this.Items[index] = value;
            }
        }
        int IReadOnlyCollection<T>.Count => this.Count;
        // IList<T> + ICollection<T> are implemented because a lot of extensions will only check for those
        int ICollection<T>.Count => this.Count;
        bool ICollection<T>.IsReadOnly => true;
        void ICollection<T>.Add(T item) => throw new NotSupportedException();
        bool ICollection<T>.Remove(T item) => throw new NotSupportedException();
        void IList<T>.Insert(int index, T item) => throw new NotSupportedException();
        void IList<T>.RemoveAt(int index) => throw new NotSupportedException();
        void ICollection<T>.Clear() => throw new NotSupportedException();
        #endregion
    }
    /// <summary>
    ///     An efficient equivalent to List&lt;T&gt;.
    ///     All methods are implemented with a performance-first bias, 
    /// </summary>
    public struct Buffer<T> : IList<T>, ICollection<T>, IEnumerable<T> { // Array<T> if class  (maybe add: IList, ICollection)
        private const int DEFAULT_ARRAY_START_SIZE = 4;

        public T[] Items { get; private set; }
        public int Count { get; private set; }
        #region Capacity
        public int Capacity {
            get => this.Items.Length;
            set {
                var items = this.Items;
                if(value < items.Length)
                    throw new ArgumentOutOfRangeException();
                if(value == items.Length)
                    return;
                if(value > 0) {
                    var array = new T[value];
                    var count = this.Count;
                    if(count > 0)
                        Array.Copy(items, 0, array, 0, count);
                    this.Items = array;
                } else
                    this.Items = new T[0];
            }
        }
        #endregion
        #region constructors
        //public Buffer() : base() { // enable if changed into a class
        //    this.Items = new T[DEFAULT_ARRAY_START_SIZE];
        //    this.Count = 0;
        //}
        public Buffer(IEnumerable<T> source) {
            T[] array = null;
            int index;
            if(source is ICollection<T> collection) {
                index = collection.Count;
                if(index > 0) {
                    array = new T[index];
                    collection.CopyTo(array, 0);
                }
            } else {
                index = 0;
                foreach(var current in source) {
                    if(array == null)
                        array = new T[DEFAULT_ARRAY_START_SIZE];
                    else if(array.Length == index) {
                        var temp = new T[checked(index * 2)];
                        Array.Copy(array, 0, temp, 0, index);
                        array = temp;
                    }
                    array[index++] = current;
                }
            }
            this.Items = array;
            this.Count = index;
        }
        #endregion
        #region this[]
        /// <summary>
        ///     Implemented for convenience sake.
        ///     Use this.Items[] directly if you dont need to check for boundaries.
        /// </summary>
        public T this[int index] {
            get {
                if(index >= this.Count)
                    throw new IndexOutOfRangeException();
                return this.Items[index];
            }
            set {
                if(index >= this.Count)
                    throw new IndexOutOfRangeException();
                this.Items[index] = value;
            }
        }
        #endregion
        #region Add()
        public void Add(T item) {
            var count = this.Count;
            if(count == this.Capacity) {
                this.EnsureCapacity(count + 1);
                count = this.Count; // the resize can be slow, so re-read afterwards
            }
            this.Count = count + 1;
            this.Items[count] = item;
        }
        #endregion
        #region AddRange()
        public void AddRange(IEnumerable<T> items) {
            this.InsertRange(this.Count, items);
        }
        #endregion
        #region Insert()
        public void Insert(int index, T item) {
            if(index > this.Count)
                throw new ArgumentOutOfRangeException(nameof(index));
            if(this.Count == this.Capacity)
                this.EnsureCapacity(this.Count + 1);

            var count = this.Count;
            var items = this.Items;

            if(index < count)
                Array.Copy(items, index, items, index + 1, count - index);

            items[index] = item;
            this.Count = count + 1;
        }
        #endregion
        #region InsertRange()
        public void InsertRange(int index, IEnumerable<T> items) {
            if(items == null)
                throw new ArgumentNullException(nameof(items));
            if(index > this.Count)
                throw new ArgumentOutOfRangeException(nameof(index));

            if(items is ICollection<T> items2) {
                int added = items2.Count;
                if(added > 0) {
                    this.EnsureCapacity(this.Count + added);
                    if(index < this.Count)
                        Array.Copy(this.Items, index, this.Items, index + added, this.Count - index);

                    if(object.ReferenceEquals(this, items2)) {
                        Array.Copy(this.Items, 0, this.Items, index, index);
                        Array.Copy(this.Items, index + added, this.Items, index * 2, this.Count - index);
                    } else {
                        var array = new T[added];
                        items2.CopyTo(array, 0);
                        Array.Copy(array, 0, this.Items, index, added);
                    }
                    this.Count += added;
                }
            } else {
                // add at the end, then move (thus avoiding memory allocation)
                var count           = this.Count;
                var capacity        = this.Capacity;
                var writeIndexStart = count;

                foreach(var item in items) {
                    if(count == capacity)
                        this.EnsureCapacity(count + 1);
                    this.Count = count + 1;
                    this.Items[count++] = item;
                }

                this.Items.Move(writeIndexStart, index, count - writeIndexStart);
            }
        }
        #endregion
        #region Remove()
        public bool Remove(T item) {
            int index = this.IndexOf(item);
            if(index >= 0) {
                this.RemoveAt(index);
                return true;
            }
            return false;
        }
        #endregion
        #region RemoveAt()
        public void RemoveAt(int index) {
            var count = this.Count;
            if(index >= count)
                throw new ArgumentOutOfRangeException(nameof(index));

            this.Count = --count;
            if(index < count)
                Array.Copy(this.Items, index + 1, this.Items, index, count - index);

            this.Items[count] = default;
        }
        #endregion
        #region RemoveRange()
        public void RemoveRange(int index, int count) {
            if(index < 0)
                throw new ArgumentOutOfRangeException(nameof(index));
            if(count < 0 || this.Count - index < count)
                throw new ArgumentOutOfRangeException(nameof(count));

            if(count > 0) {
                this.Count -= count;
                if(index < this.Count)
                    Array.Copy(this.Items, index + count, this.Items, index, this.Count - index);

                Array.Clear(this.Items, this.Count, count);
            }
        }
        #endregion
        #region Clear()
        public void Clear() {
            if(this.Count > 0) {
                Array.Clear(this.Items, 0, this.Count);
                this.Count = 0;
            }
        }
        #endregion
        #region Contains()
        public bool Contains(T item) {
            return this.IndexOf(item) >= 0;
        }
        #endregion
        #region IndexOf()
        public int IndexOf(T item) {
            return Array.IndexOf(this.Items, item, 0, this.Count);
        }
        public int IndexOf(T item, int startIndex) {
            if(startIndex >= this.Count)
                throw new ArgumentOutOfRangeException(nameof(startIndex));

            return Array.IndexOf(this.Items, item, startIndex, this.Count - startIndex);
        }
        public int IndexOf(T item, int startIndex, int count) {
            if(startIndex >= this.Count)
                throw new ArgumentOutOfRangeException(nameof(startIndex));
            if(count < 0 || startIndex > this.Count - count)
                throw new ArgumentOutOfRangeException(nameof(count));

            return Array.IndexOf(this.Items, item, startIndex, count);
        }
        #endregion
        #region LastIndexOf()
        public int LastIndexOf(T item) {
            if(this.Count == 0)
                return -1;
            return Array.LastIndexOf(this.Items, item, this.Count - 1, this.Count);
        }
        /// <param name="startIndex">The starting index of the backward search</param>
        public int LastIndexOf(T item, int startIndex) {
            if(this.Count == 0)
                return -1;
            if(startIndex < 0 || startIndex >= this.Count)
                throw new ArgumentOutOfRangeException(nameof(startIndex));

            return Array.LastIndexOf(this.Items, item, startIndex, startIndex + 1);
        }
        /// <param name="startIndex">The starting index of the backward search</param>
        public int LastIndexOf(T item, int startIndex, int count) {
            if(this.Count == 0)
                return -1;
            if(startIndex < 0 || startIndex >= this.Count)
                throw new ArgumentOutOfRangeException(nameof(startIndex));
            if(count < 0 || count > startIndex + 1)
                throw new ArgumentOutOfRangeException(nameof(count));

            return Array.LastIndexOf(this.Items, item, startIndex, count);
        }
        #endregion
        #region ToArray()
        /// <summary>
        ///     Returns a properly-sized array.
        ///     This will return the internal array instance if the size matches rather than a new copy.
        /// </summary>
        public T[] ToArray() {
            var count = this.Count;
            if(count == 0)
                return new T[0];

            var items = this.Items;
            if(items.Length == count)
                return this.Items;

            var res = new T[count];
            Array.Copy(items, 0, res, 0, count);
            return res;
        }
        #endregion
        #region CopyTo()
        public void CopyTo(T[] array, int startIndex) {
            Array.Copy(this.Items, 0, array, startIndex, this.Count);
        }
        #endregion
        #region GetEnumerator()
        /// <summary>
        ///     This is included for convenience, but you probably meant to use "Enumerable.ToList()" rather than "new Buffer()" if you call this.
        ///     If you want speed, access this.Items directly.
        /// </summary>
        public IEnumerator<T> GetEnumerator() {
            for(int i = 0; i < this.Count; i++)
                yield return this.Items[i];
        }
        /// <summary>
        ///     This is included for convenience, but you probably meant to use "Enumerable.ToList()" rather than "new Buffer()" if you call this.
        ///     If you want speed, access this.Items directly.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() {
            for(int i = 0; i < this.Count; i++)
                yield return this.Items[i];
        }
        #endregion
        #region interfaces implementations
        bool ICollection<T>.IsReadOnly => false;
        #endregion
        #region private EnsureCapacity()
        /// <summary>
        ///     Ensures theres at least enough capacity for min items.
        /// </summary>
        private void EnsureCapacity(int min) {
            var capacity = this.Capacity;
            if(min > capacity) {
                capacity = capacity == 0 ? DEFAULT_ARRAY_START_SIZE : capacity * 2;
                // this is the max possible with the .NET 2GB memory limit per variable.
                // this value is slightly lower than int.MaxValue (2147483647)
                if(capacity > 2146435071)
                    capacity = 2146435071;
                if(capacity < min)
                    capacity = min;
                this.Capacity = capacity;
            }
        }
        #endregion
    }
}
