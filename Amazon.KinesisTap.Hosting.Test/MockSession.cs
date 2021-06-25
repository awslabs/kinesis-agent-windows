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
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Hosting.Test
{
    /// <summary>
    /// A mock LogManager that does nothing, other than storing states
    /// </summary>
    public class MockSession : ISession
    {
        private readonly bool _loadSuccess;
        private Task _execution;
        private CancellationTokenSource _stopTokenSource;
        private readonly int _stopDelayMs;

        public MockSession(string name, bool loadSuccess, bool validated) : this(name, loadSuccess, validated, 0) { }

        public MockSession(string name, bool loadSuccess, bool validated, int stopDelayMs)
        {
            Name = name;
            IsValidated = validated;
            _stopDelayMs = stopDelayMs;
            _loadSuccess = loadSuccess;
        }

        public string Name { get; }

        public bool Disposed { get; private set; }

        public DateTime StartTime { get; } = DateTime.Now;

        public bool IsValidated { get; }

        public string DisplayName => Name;

        public bool IsDefault => Name is null;

        public void Dispose()
        {
            Disposed = true;
        }

        public Task StartAsync(CancellationToken stopToken)
        {
            if (!_loadSuccess)
            {
                throw new Exception();
            }

            _stopTokenSource = CancellationTokenSource.CreateLinkedTokenSource(stopToken);

            _execution = ExecutionTask(_stopTokenSource.Token);

            return Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken gracefulStopToken)
        {
            _stopTokenSource?.Cancel();
            if (_execution is not null)
            {
                await _execution;
            }
            if (_stopDelayMs > 0)
            {
                await Task.Delay(_stopDelayMs, gracefulStopToken);
            }
        }

        private async Task ExecutionTask(CancellationToken stopToken)
        {
            while (!stopToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(1000, stopToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
    }
}
