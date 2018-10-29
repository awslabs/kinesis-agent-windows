using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public class EnvelopeComparer<T> : IEqualityComparer<Envelope<T>>
    {
        private IEqualityComparer<T> _dataComparer;

        public EnvelopeComparer(IEqualityComparer<T> dataComparer)
        {
            _dataComparer = dataComparer;
        }

        public bool Equals(Envelope<T> x, Envelope<T> y)
        {
            return x.Timestamp == y.Timestamp &&
                _dataComparer.Equals(x.Data, y.Data);
        }

        public int GetHashCode(Envelope<T> obj)
        {
            return obj.GetHashCode();
        }
    }
}
