using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    //Wrap around the underlying data to provide additional meta data
    public interface IEnvelope
    {
        DateTime Timestamp { get; }

        string GetMessage(string format);

        /// <summary>
        /// Resolve local variable. Local variable are defined as the variable only depends on the envelope. 
        /// They don't depend on the environment (environment variables, ec2 meta data)
        /// </summary>
        /// <param name="variable">Name of the variable to resolve. Assume {} are already stripped off and starts with $.</param>
        /// <returns>Return value from evaludating the variable.</returns>
        object ResolveLocalVariable(string variable);
    }

    public interface IEnvelope<out TData> : IEnvelope
    {
        TData Data { get; }
    }
}
