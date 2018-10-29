using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// A Step is like a pipe but it is light weight
    /// A Step participates in a single linked-list so we execute step by step
    /// </summary>
    public interface IStep
    {
        /// <summary>
        /// The next step
        /// </summary>
        IStep Next { get; set; }
    }

    public interface IStep<in T> : IStep
    {
        //Called by the previous step to handle a value
        void OnNext(T value);
    }
}
