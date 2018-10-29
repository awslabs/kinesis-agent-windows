using System;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Amazon.KinesisTap.Core;

namespace Amazon.KinesisTap.AutoUpdate
{
    public class AutoUpdateFactory : IFactory<IGenericPlugin>
    {
        const string PACKAGE_UPDATE = "packageupdate";
        const string CONFIG_UPDATE = "configupdate";

        public void RegisterFactory(IFactoryCatalog<IGenericPlugin> catalog)
        {
            catalog.RegisterFactory(PACKAGE_UPDATE, this);
            catalog.RegisterFactory(CONFIG_UPDATE, this);
        }

        public IGenericPlugin CreateInstance(string entry, IPlugInContext context)
        {
            IConfiguration config = context.Configuration;
            ILogger logger = context.Logger;

            switch (entry.ToLower())
            {
                case PACKAGE_UPDATE:
                    return new PackageUpdater(context);
                case CONFIG_UPDATE:
                    return new ConfigurationFileUpdater(context);
                default:
                    throw new ArgumentException($"Source {entry} not recognized.");
            }
        }
    }
}
