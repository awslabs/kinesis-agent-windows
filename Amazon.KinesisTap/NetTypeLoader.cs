using Amazon.KinesisTap.Core;
using System;
using System.Collections.Generic;
using System.Composition.Convention;
using System.Composition.Hosting;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Amazon.KinesisTap
{
    public class NetTypeLoader : ITypeLoader
    {
        private readonly IList<Assembly> _assemblies;

        public NetTypeLoader()
        {
            _assemblies = Directory
                .GetFiles(AppContext.BaseDirectory, "*.dll", SearchOption.TopDirectoryOnly)
                .Select(Assembly.LoadFrom)
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
