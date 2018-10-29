using System;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.Uls
{
    //Factory for the Sharepoint Uls Log source
    public class UlsSourceFactory : IFactory<ISource>
    {
        const string ULSSOURCE = "ulssource";

        public void RegisterFactory(IFactoryCatalog<ISource> catalog)
        {
            catalog.RegisterFactory(ULSSOURCE, this);
        }

        public ISource CreateInstance(string entry, IPlugInContext context)
        {
            IConfiguration config = context.Configuration;
            ILogger logger = context.Logger;

            switch (entry.ToLower())
            {
                case ULSSOURCE:
                    UlsLogParser ulsParser = new UlsLogParser();
                    return DirectorySourceFactory.CreateEventSource(
                        context,
                        ulsParser,
                        DirectorySourceFactory.CreateDelimitedLogSourceInfo);
                default:
                    throw new ArgumentException($"Source {entry} not recognized.");
            }
        }
    }
}
