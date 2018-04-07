using System;
using System.Collections;
using System.Collections.Generic;



namespace TimeSeriesDB.Internal
{
    /// <summary>
    ///     Represents a Dictionary whose entries have an index and an optional key.
    /// </summary>
    public sealed class KeyOrIndexCollection<TKey, TValue> : IEnumerable<TValue> { // IDictionary<TKey, TValue>, IDictionary
        private readonly List<KeyValuePair<TKey, TValue>> m_items;
        private readonly Dictionary<TKey, TValue> m_itemsDict; // excludes default(TKey) entries
        private int m_defaultKeyCount = 0;

        public int Count { get; private set; }

        #region constructors
        public KeyOrIndexCollection() {
            if(typeof(TKey) == typeof(int))
                throw new NotSupportedException("The key cannot be an integer, because it would be indistinguishable from indices. You may use longs or strings instead.");

            m_items = new List<KeyValuePair<TKey, TValue>>();
            m_itemsDict = new Dictionary<TKey, TValue>();
        }
        public KeyOrIndexCollection(int capacity) {
            if(typeof(TKey) == typeof(int))
                throw new NotSupportedException("The key cannot be an integer, because it would be indistinguishable from indices. You may use longs or strings instead.");

            m_items = new List<KeyValuePair<TKey, TValue>>(capacity);
            m_itemsDict = new Dictionary<TKey, TValue>(capacity);
        }
        #endregion

        public TValue this[int index] {
            get {
                return m_items[index].Value;
            }
            // set{} cant be used because no key specified
        }
        public TValue this[TKey key] {
            get {
                if(!object.Equals(key, default(TKey)))
                    return m_itemsDict[key];
                else if(m_defaultKeyCount != 0) {
                    var index = this.FindKeyIndex(key);
                    if(index >= 0)
                        return m_items[index].Value;
                }

                throw new KeyNotFoundException();
            }
            set {
                if(!object.Equals(key, default(TKey))) {
                    if(m_itemsDict.ContainsKey(key)) {
                        // update
                        m_itemsDict[key] = value;
                        m_items[this.FindKeyIndex(key)] = new KeyValuePair<TKey, TValue>(key, value);
                    } else
                        // add
                        this.Add(key, value);
                    return;
                } else if(m_defaultKeyCount != 0) {
                    var index = this.FindKeyIndex(key);
                    if(index >= 0) {
                        m_items[index] = new KeyValuePair<TKey, TValue>(key, value);
                        return;
                    }
                }

                //this.Add(key, value);
                this.Count++;
                m_defaultKeyCount++;
                m_items.Add(new KeyValuePair<TKey, TValue>(key, value));
            }
        }

        public void Add(TValue value) {
            this.Add(default, value);
        }
        public void Add(TKey key, TValue value) {
            if(!object.Equals(key, default(TKey)))
                m_itemsDict.Add(key, value);
            else
                m_defaultKeyCount++;

            m_items.Add(new KeyValuePair<TKey, TValue>(key, value));
            this.Count++;
        }

        public void Clear() { 
            m_items.Clear();
            m_itemsDict.Clear();
            m_defaultKeyCount = 0;
            this.Count = 0;
        }
        public bool ContainsKey(TKey key) { 
            if(!object.Equals(key, default(TKey)))
                return m_itemsDict.ContainsKey(key);
            else
                return m_defaultKeyCount != 0;
        }
        public bool ContainsValue(TValue value) { 
            return this.FindValueIndex(value) >= 0;
        }
        public bool Remove(int index) { 
            if(index < 0 || index >= this.Count)
                return false;

            var keyvalue = m_items[index];

            this.Count--;
            if(!object.Equals(keyvalue.Key, default(TKey)))
                m_itemsDict.Remove(keyvalue.Key);
            else
                m_defaultKeyCount--;

            m_items.RemoveAt(index);

            return true;
        }
        public bool Remove(TKey key) {
            return this.Remove(this.FindKeyIndex(key));
        }
        public bool TryGetValue(TKey key, out TValue value) {
            var index = this.FindKeyIndex(key);

            if(index >= 0) {
                value = m_items[index].Value;
                return true;
            } else {
                value = default;
                return false;
            }
        }

        #region GetEnumerator()
        public IEnumerator<TValue> GetEnumerator() {
            foreach(var item in m_items)
                yield return item.Value;
        }
        IEnumerator IEnumerable.GetEnumerator() {
            return this.GetEnumerator();
        }
        #endregion

        private int FindKeyIndex(TKey key) {
            var max = m_items.Count;
            for(int i = 0; i < max; i++) {
                if(object.Equals(m_items[i].Key, key))
                    return i;
            }
            return -1;
        }
        private int FindValueIndex(TValue value) {
            var max = m_items.Count;
            for(int i = 0; i < max; i++) {
                if(object.Equals(m_items[i].Value, value))
                    return i;
            }
            return -1;
        }
    }
}
