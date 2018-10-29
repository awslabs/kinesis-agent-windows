using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Marker interface for sink. All sinks should extend this interface
    /// </summary>
    public interface ISink : IPlugIn
    {
    }
}
