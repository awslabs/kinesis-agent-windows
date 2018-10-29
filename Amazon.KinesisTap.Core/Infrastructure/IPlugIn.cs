using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    public interface IPlugIn
    {
        string Id { get; set; }

        void Start();

        /// <summary>
        /// An opportunity to cleanup such as flushing the buffer and stop the timer if any
        /// </summary>
        void Stop();
    }
}
