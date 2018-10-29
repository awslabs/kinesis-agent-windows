using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public class FactoryCatalog<T> : IFactoryCatalog<T>
    {
        protected IDictionary<string, IFactory<T>> _catalog = new Dictionary<string, IFactory<T>>(StringComparer.OrdinalIgnoreCase);

        public IFactory<T> GetFactory(string entry)
        {
            if (_catalog.TryGetValue(entry, out var factory))
            {
                return factory;
            }
            else
            {
                return null;
            }
        }

        public void RegisterFactory(string entry, IFactory<T> factory)
        {
            _catalog[entry] = factory;
        }
    }
}
