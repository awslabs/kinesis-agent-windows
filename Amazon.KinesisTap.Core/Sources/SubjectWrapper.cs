/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
using System;
using System.Reactive.Subjects;

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
