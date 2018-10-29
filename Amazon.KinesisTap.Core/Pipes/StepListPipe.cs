using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core.Pipes
{
    /// <summary>
    /// A complex pipe that can contain a list of steps
    /// </summary>
    /// <typeparam name="TIn"></typeparam>
    /// <typeparam name="TOut"></typeparam>
    public class StepListPipe<TIn, TOut> : Pipe<TIn, TOut>
    {
        private readonly StepList<IEnvelope<TIn>> _stepList;

        protected StepListPipe(IPlugInContext context, StepList<IEnvelope<TIn>> stepList) : base(context)
        {
            _stepList = stepList;
            _stepList.Tail.Next = new TerminalStep<IEnvelope<TOut>>((value) => _subject.OnNext((IEnvelope<TOut>)value));
        }

        public override void OnNext(IEnvelope<TIn> value)
        {
            _stepList.Head.OnNext(value);
        }

        public override void Start()
        {
        }

        public override void Stop()
        {
        }
    }
}
