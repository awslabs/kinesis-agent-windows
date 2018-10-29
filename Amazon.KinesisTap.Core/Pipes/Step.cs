using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Base class for a step
    /// </summary>
    /// <typeparam name="TIn"></typeparam>
    /// <typeparam name="TOut"></typeparam>
    public abstract class Step<TIn, TOut> : IStep<TIn>
    {
        protected IStep<TOut> _next;

        public virtual IStep Next
        {
            get => _next;

            set
            {
                try
                {
                    _next = (IStep<TOut>)value;
                }
                catch
                {
                    throw new ArgumentException($"Property only accepts IStep<{typeof(TOut)}>");
                }
            }
        }

        public abstract void OnNext(TIn value);
    }
}
