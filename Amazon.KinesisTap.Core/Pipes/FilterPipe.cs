using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Impplement the filter pipe semantics. The type does not change. Need to implement the filter method
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class FilterPipe<T> : Pipe<T, T>
    {
        protected FilterPipe(IPlugInContext context) : base(context)
        {
        }

        public override void OnNext(IEnvelope<T> value)
        {
            if (Filter(value))
            {
                _subject.OnNext(value);
            }
        }

        /// <summary>
        /// Override this method if need to do something in Start
        /// </summary>
        public override void Start()
        {
        }

        /// <summary>
        /// Override this method if need to do something in Stop
        /// </summary>
        public override void Stop()
        {
        }

        /// <summary>
        /// Event will propagate through the pipe only if the filter turns true
        /// </summary>
        /// <param name="value">The event to filter</param>
        /// <returns></returns>
        protected abstract bool Filter(IEnvelope<T> value);
    }    
}
