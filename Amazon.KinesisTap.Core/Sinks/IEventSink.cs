using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// The interface for the EventSink. It extends IObserver and thus allow others to push data to it
    /// </summary>
    public interface IEventSink : IObserver<IEnvelope>, ISink
    {
    }

    public interface IEventSink<in TIn> : IObserver<IEnvelope<TIn>>, ISink
    {
    }
}
