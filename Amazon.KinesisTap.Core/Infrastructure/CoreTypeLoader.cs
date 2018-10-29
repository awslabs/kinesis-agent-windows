//This class requires .net standard 1.5. We are trying to move the requirement down to .net standard 1.3
/*
using System;
using System.Collections.Generic;
using System.Composition.Convention;
using System.Composition.Hosting;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;

namespace Amazon.KinesisTap.Core
{
    public class CoreTypeLoader : ITypeLoader
    {
        private IList<Assembly> _assemblies;

        public CoreTypeLoader()
        {
            _assemblies = Directory
                .GetFiles(AppContext.BaseDirectory, "*.dll", SearchOption.TopDirectoryOnly)
                .Select(AssemblyLoadContext.GetAssemblyName)
                .Select(AssemblyLoadContext.Default.LoadFromAssemblyName)
                .ToList();
        }

        public IEnumerable<T> LoadTypes<T>()
        {
            var conventions = new ConventionBuilder();
            conventions
                .ForTypesDerivedFrom<T>()
                .Export<T>()
                .Shared();

            var configuration = new ContainerConfiguration()
                .WithAssemblies(_assemblies, conventions);

            using (var container = configuration.CreateContainer())
            {
                var plugins = container.GetExports<T>();
                return plugins;
            }
        }
    }
}
*/