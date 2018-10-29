using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;

namespace Amazon.KinesisTap.ExchangeSource
{
    public class ExchangeSourceFactory : IFactory<ISource>
    {
        public void RegisterFactory(IFactoryCatalog<ISource> catalog)
        {
            catalog.RegisterFactory("ExchangeLogSource", this);
        }

        public ISource CreateInstance(string entry, IPlugInContext context)
        {
            IConfiguration config = context.Configuration;
            ILogger logger = context.Logger;

            switch (entry.ToLower())
            {
                case "exchangelogsource":
                    ExchangeLogParser exchangeLogParser = new ExchangeLogParser();
                    exchangeLogParser.TimeStampField = config["TimeStampField"];
                    return DirectorySourceFactory.CreateEventSource(
                        context,
                        exchangeLogParser,
                        DirectorySourceFactory.CreateDelimitedLogSourceInfo);
                default:
                    throw new ArgumentException($"Source {entry} not recognized.");
            }
        }
    }
}
