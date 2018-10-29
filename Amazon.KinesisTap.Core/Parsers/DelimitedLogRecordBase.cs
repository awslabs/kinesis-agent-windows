using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace Amazon.KinesisTap.Core
{
    public abstract class DelimitedLogRecordBase : IReadOnlyDictionary<string, string>
    {
        protected string[] _data;
        protected DelimitedLogContext _context;
        protected DateTimeKind _timeZoneKind;

        protected DelimitedLogRecordBase(string[] data, DelimitedLogContext context)
        {
            _data = data;
            _context = context;
        }

        public abstract DateTime TimeStamp { get; }

        #region IReadOnlyDictionary
        public string this[string key] => _data[_context.Mapping[key]];


        public IEnumerable<string> Keys => _context.Mapping.Keys;

        public IEnumerable<string> Values => _data;

        public int Count => _context.Mapping.Count;

        public bool ContainsKey(string key)
        {
            return _context.Mapping.ContainsKey(key);
        }

        public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
        {
            return _context.Mapping.Keys.Select(k => new KeyValuePair<string, string>(k, this[k])).GetEnumerator();
        }

        public bool TryGetValue(string key, out string value)
        {
            if (_context.Mapping.TryGetValue(key, out int index))
            {
                value = _data[index];
                return true;
            }
            else
            {
                value = null;
                return false;
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
        #endregion
    }
}
