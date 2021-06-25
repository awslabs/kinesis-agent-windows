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
using System.IO;
using Amazon.KinesisTap.Core.Test;
using Xunit.Abstractions;

namespace Amazon.KinesisTap.Filesystem.Test
{
    public abstract class AsyncDirectorySourceTestBase : IDisposable
    {
        protected readonly string _testDir = Path.Combine(TestUtility.GetTestHome(), Guid.NewGuid().ToString());
        protected readonly ITestOutputHelper _output;
        protected readonly string _sourceId = $"source_{Guid.NewGuid()}";
        private bool _disposed;

        public AsyncDirectorySourceTestBase(ITestOutputHelper output)
        {
            _output = output;
            if (Directory.Exists(_testDir))
            {
                Directory.Delete(_testDir, true);
            }
            Directory.CreateDirectory(_testDir);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                if (Directory.Exists(_testDir))
                {
                    Directory.Delete(_testDir, true);
                }
            }

            _disposed = true;
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
