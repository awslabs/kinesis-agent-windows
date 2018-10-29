using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// This interface is used to define a source that be queried (instead of push events) 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IDataSource<out T> : ISource
    {
        /// <summary>
        /// Query the source
        /// </summary>
        /// <param name="query">Query spec.</param>
        /// <returns>Query results.</returns>
        IEnvelope<T> Query(string query);
    }
}
