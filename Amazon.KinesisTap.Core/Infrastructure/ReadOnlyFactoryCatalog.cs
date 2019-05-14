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
using System.Text;

namespace Amazon.KinesisTap.Core
{
    //Read-only wrapper for a factory catalog
    public class ReadOnlyFactoryCatalog<T> : IFactoryCatalog<T>
    {
        private IFactoryCatalog<T> _factoryCatalog;

        public ReadOnlyFactoryCatalog(IFactoryCatalog<T> factoryCatalog)
        {
            Guard.ArgumentNotNull(factoryCatalog, "factoryCatalog");
            _factoryCatalog = factoryCatalog;
        }

        /// <summary>
        /// Get the factory from catalog
        /// </summary>
        /// <param name="entry">The name of the factory to get</param>
        /// <returns>A factory</returns>
        public IFactory<T> GetFactory(string entry)
        {
            return _factoryCatalog.GetFactory(entry);
        }

        /// <summary>
        /// Always throw Not implemented exception because it is read-only so one cannot add an entry
        /// </summary>
        /// <param name="entry"></param>
        /// <param name="factory"></param>
        public void RegisterFactory(string entry, IFactory<T> factory)
        {
            throw new NotImplementedException(); //Prevent plug-ins from tempering the catalog
        }
    }
}
