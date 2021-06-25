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
