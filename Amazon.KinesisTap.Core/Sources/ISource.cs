using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Marker interface for source. All sources should extend this interface
    /// </summary>
    public interface ISource : IPlugIn
    {
    };
}
