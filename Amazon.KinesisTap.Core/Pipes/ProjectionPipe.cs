using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Implement the basic semantics of conversion. The event is converted from TIn to TOut.
    /// Need to implement the Project method for the type conversion
    /// </summary>
    /// <typeparam name="TIn"></typeparam>
    /// <typeparam name="TOut"></typeparam>
    public abstract class ProjectionPipe<TIn, TOut> : Pipe<TIn, TOut>
    {
        protected ProjectionPipe(IPlugInContext context) : base(context)
        {
        }

        public override void OnNext(IEnvelope<TIn> value)
        {
            _subject.OnNext(Project(value));
        }

        protected abstract IEnvelope<TOut> Project(IEnvelope<TIn> value);
    }
}
