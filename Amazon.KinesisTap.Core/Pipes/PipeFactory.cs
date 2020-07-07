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
using Amazon.KinesisTap.Core.EMF;

namespace Amazon.KinesisTap.Core.Pipes
{
    //Factory for the Sharepoint Uls Log source
    public class PipeFactory : IFactory<IPipe>
    {
        public const string REGEX_FILTER_PIPE = "regexfilterpipe";
        public const string EMF_PIPE = "emfpipe";

        public void RegisterFactory(IFactoryCatalog<IPipe> catalog)
        {
            catalog.RegisterFactory(REGEX_FILTER_PIPE, this);
            catalog.RegisterFactory(EMF_PIPE, this);
        }

        public IPipe CreateInstance(string entry, IPlugInContext context)
        {
            Type sourceOutputType = (Type)context.ContextData[PluginContext.SOURCE_OUTPUT_TYPE];

            switch (entry.ToLower())
            {
                case REGEX_FILTER_PIPE:
                    Type regexFilterPipeType = typeof(RegexFilterPipe<>).MakeGenericType(sourceOutputType);
                    return (IPipe)Activator.CreateInstance(regexFilterPipeType, context);
                case EMF_PIPE:
                    Type emfPipeType = typeof(EMFPipe<>).MakeGenericType(sourceOutputType);
                    return (IPipe)Activator.CreateInstance(emfPipeType, context);
                default:
                    throw new ArgumentException($"Source {entry} not recognized.");
            }
        }
    }
}
