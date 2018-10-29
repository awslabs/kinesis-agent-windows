using System;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.Core.Pipes
{
    //Factory for the Sharepoint Uls Log source
    public class PipeFactory : IFactory<IPipe>
    {
        public const string REGEX_FILTER_PIPE = "regexfilterpipe";

        public void RegisterFactory(IFactoryCatalog<IPipe> catalog)
        {
            catalog.RegisterFactory(REGEX_FILTER_PIPE, this);
        }

        public IPipe CreateInstance(string entry, IPlugInContext context)
        {
            IConfiguration config = context.Configuration;
            ILogger logger = context.Logger;

            switch (entry.ToLower())
            {
                case REGEX_FILTER_PIPE:
                    Type sourceType = (Type)context.ContextData[PluginContext.SOURCE_TYPE];
                    Type sinkType = (Type)context.ContextData[PluginContext.SINK_TYPE];
                    Type sourceDataType = sourceType.GenericTypeArguments[0];
                    Type regexFilterPipeType = typeof(RegexFilterPipe<>).MakeGenericType(sourceDataType);
                    return (IPipe)Activator.CreateInstance(regexFilterPipeType, context);
                default:
                    throw new ArgumentException($"Source {entry} not recognized.");
            }
        }
    }
}
