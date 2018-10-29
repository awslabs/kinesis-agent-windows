using System;
using Amazon.KinesisTap.Core;
using System.Reactive.Subjects;

namespace Amazon.KinesisTap.Core.Test
{
    internal class MockEventSource<T> : EventSource<T>
    {
        private ISubject<IEnvelope<T>> _subject = new Subject<IEnvelope<T>>();

        public MockEventSource(IPlugInContext context) : base(context)
        {

        }

        public override void Start()
        {
        }

        public override void Stop()
        {
        }

        public override IDisposable Subscribe(IObserver<IEnvelope<T>> observer)
        {
            return _subject.Subscribe(observer);
        }

        public void MockEvent(T data)
        {
            _subject.OnNext(new Envelope<T>(data));
        }

        public void MockEvent(T data, DateTime timestamp)
        {
            _subject.OnNext(new Envelope<T>(data, timestamp));
        }

        public void MockLogEvent(T data, DateTime timestamp, string rawRecord, string filePath, long position, long lineNumber)
        {
            _subject.OnNext(new LogEnvelope<T>(data, timestamp, rawRecord, filePath, position, lineNumber));
        }
    }
}
