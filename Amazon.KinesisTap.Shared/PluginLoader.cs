/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
using Amazon.KinesisTap.Core;
using System;
using System.Collections.Generic;
using System.Composition.Convention;
using System.Composition.Hosting;
using System.IO;
using System.Linq;
using System.Reflection;

namespace Amazon.KinesisTap.Shared
{
    public class PluginLoader : ITypeLoader
    {
        private readonly IList<Assembly> _assemblies;

        public PluginLoader()
            : this(Directory.GetFiles(AppContext.BaseDirectory, "*.dll", SearchOption.TopDirectoryOnly))
        {
        }

        public PluginLoader(IEnumerable<string> assemblies)
        {
#if NET46
            _assemblies = assemblies
                .Select(Assembly.LoadFrom)
                .ToList();
#else
            _assemblies = assemblies
                .Select(p => Assembly.Load(new AssemblyName(Path.GetFileNameWithoutExtension(p))))
                //.Select(p => AssemblyLoadContext.Default.LoadFromAssemblyPath(p))
                .ToList();
#endif
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
