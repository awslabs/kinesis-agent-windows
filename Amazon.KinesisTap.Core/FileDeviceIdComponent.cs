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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Security.Cryptography;

namespace Amazon.KinesisTap.Core
{
    class FileDeviceIdComponent
    {
        /// <summary>
        /// The paths of file we should look at.
        /// </summary>
        private readonly string[] _paths;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileDeviceIdComponent"/> class.
        /// </summary>
        /// <param name="path">The path of the file holding the component ID.</param>
        public FileDeviceIdComponent(string path)
            : this(new string[] { path }) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileDeviceIdComponent"/> class.
        /// </summary>
        /// <param name="paths">The paths of the files holding the component ID.</param>
        public FileDeviceIdComponent(IEnumerable<string> paths)
        {
            _paths = paths.ToArray();
        }

        /// <summary>
        /// Gets the component value.
        /// </summary>
        /// <returns>The component value.</returns>
        public string GetValue()
        {
            foreach (var path in _paths)
            {
                if (!File.Exists(path))
                {
                    continue;
                }

                try
                {
                    var contents = default(string);

                    using (var file = File.OpenText(path))
                    {
                        contents = file.ReadToEnd();
                    }

                    contents = contents.Trim();

                    return contents;
                }
                catch (UnauthorizedAccessException)
                {
                }
            }

            return string.Empty;
        }
    }
}
