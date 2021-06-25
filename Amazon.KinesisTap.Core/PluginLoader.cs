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
    public class PluginLoader : ITypeLoader
    {
        private readonly List<Assembly> _assemblies = new();
        private static readonly string[] _builtInLibraryAssemblies =
        {
            "Amazon.KinesisTap.DiagnosticTool.Core.dll",
            "Amazon.KinesisTap.Hosting.dll",
            "Amazon.KinesisTap.Shared.dll",
            "Amazon.KinesisTap.AEM.Model.dll"
        };

        public PluginLoader()
            : this(Directory.GetFiles(AppContext.BaseDirectory, "*KinesisTap.*.dll", SearchOption.TopDirectoryOnly))
        {
        }

        public PluginLoader(IEnumerable<string> assemblies)
        {
            foreach (var file in assemblies)
            {
                var fileName = Path.GetFileName(file);
                if (_builtInLibraryAssemblies.Any(l => l == fileName))
                {
                    continue;
                }

                try
                {
                    var assembly = AssemblyLoadContext.Default.LoadFromAssemblyPath(file);
                    _assemblies.Add(assembly);
                }
                catch { }
            }
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

            using var container = configuration.CreateContainer();
            var plugins = container.GetExports<T>();
            return plugins;
        }
    }
}
