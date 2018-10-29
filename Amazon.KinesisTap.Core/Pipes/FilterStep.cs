using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Implement a filter step
    /// </summary>
    /// <typeparam name="T">Type of the data to filter</typeparam>
    public class FilterStep<T> : Step<T, T>
    {
        private readonly Func<T, bool> _filter;

        public FilterStep(Func<T, bool> filter)
        {
            Guard.ArgumentNotNull(filter, "filter");
            _filter = filter;
        }

        public override void OnNext(T value)
        {
            if (_next != null && _filter(value))
                _next.OnNext(value);
        }
    }
}
