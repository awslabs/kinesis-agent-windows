using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Interface for Event source. It extends IObservable so that it can be subscribed.
    /// </summary>
    public interface IEventSource : ISource, IObservable<IEnvelope>
    {
    }

    public interface IEventSource<out T> : IEventSource, IObservable<IEnvelope<T>>
    {
    }
}
