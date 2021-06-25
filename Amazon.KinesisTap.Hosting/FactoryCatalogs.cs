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

namespace Amazon.KinesisTap.Hosting
{
    /// <summary>
    /// Use to group plugin factory catalogs together to pass to sessions
    /// </summary>
    public class FactoryCatalogs
    {
        /// <summary>
        /// The source factory catalog
        /// </summary>
        public IFactoryCatalog<ISource> SourceFactoryCatalog { get; set; }

        /// <summary>
        /// The sink factory catalog
        /// </summary>
        public IFactoryCatalog<IEventSink> SinkFactoryCatalog { get; set; }

        /// <summary>
        /// The credential provider factory catalog
        /// </summary>
        public IFactoryCatalog<ICredentialProvider> CredentialProviderFactoryCatalog { get; set; }

        /// <summary>
        /// The generic plugin factory catalog
        /// </summary>
        public IFactoryCatalog<IGenericPlugin> GenericPluginFactoryCatalog { get; set; }

        /// <summary>
        /// The pipe factory catalog
        /// </summary>
        public IFactoryCatalog<IPipe> PipeFactoryCatalog { get; set; }

        /// <summary>
        /// The record parser factory catalog
        /// </summary>
        public IFactoryCatalog<IRecordParser> RecordParserCatalog { get; set; }
    }
}
