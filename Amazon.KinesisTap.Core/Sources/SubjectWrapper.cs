using System;
using System.Collections.Generic;
using System.Reactive.Subjects;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public class SubjectWrapper<T> : ISubject<T>
    {
        private readonly ISubject<T> _subject;
        private readonly Action<IObserver<T>> _onSubscribe;

        public SubjectWrapper(Action<IObserver<T>> onSubscribe)
        {
            _subject = new Subject<T>();
            _onSubscribe = onSubscribe;
        }

        public void OnCompleted()
        {
            _subject.OnCompleted();
        }

        public void OnError(Exception error)
        {
            _subject.OnError(error);
        }

        public void OnNext(T value)
        {
            _subject.OnNext(value);
        }

        public IDisposable Subscribe(IObserver<T> observer)
        {
            var disposable = _subject.Subscribe(observer);
            try
            {
                _onSubscribe(observer);
            }
            catch { }
            return disposable;
        }
    }
}
