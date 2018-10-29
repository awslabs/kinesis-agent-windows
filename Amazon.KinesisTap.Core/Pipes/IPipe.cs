using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    //A pipe is both source and sink so it can subscribe to a souce and be subscribed by a sink
    public interface IPipe : IEventSource, IEventSink
    {
    }

    public interface IPipe<in TIn, out TOut> : IPipe, IEventSink<TIn>, IEventSource<TOut>
    {
    }
}
