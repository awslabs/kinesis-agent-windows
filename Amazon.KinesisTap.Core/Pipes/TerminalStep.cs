using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Last step of a list. Used to attach and handler to hancle the output
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class TerminalStep<T> : Step<T, T>
    {
        private readonly Action<T> _handler;

        /// <summary>
        /// Handler to handle the output
        /// </summary>
        /// <param name="handler"></param>
        public TerminalStep(Action<T> handler)
        {
            _handler = handler;
        }

        public override IStep Next { get => throw new NotImplementedException("No more next"); set => throw new NotImplementedException("No more next"); }

        public override void OnNext(T value)
        {
            _handler(value);
        }
    }
}
