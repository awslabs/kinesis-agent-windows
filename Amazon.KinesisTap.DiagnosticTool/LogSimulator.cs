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
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.DiagnosticTool
{
    public abstract class LogSimulator : IDisposable
    {
        protected Timer _timer;
        protected int _interval;
        private int _size;
        private int _batchSize;
        private static Random random = new Random();
        private const int MIN_SIZE = 86;
        private const int MIN_BATCH_SIZE = 1;

        internal LogSimulator(int interval, int size, int batchSize)
        {
            _interval = interval;
            _size = size;
            _batchSize = batchSize;
            _timer = new Timer(OnTimer, null, Timeout.Infinite, Timeout.Infinite);
        }

        public void Start()
        {
            _timer.Change(0, _interval);
        }

        public void Stop()
        {
            _timer.Change(Timeout.Infinite, Timeout.Infinite);
        }

        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789,. ";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        protected int Size
        {
            get { return _size; }
            set { _size = value < MIN_SIZE ? MIN_SIZE : value; }
        }

        protected int BatchSize
        {
            get { return _batchSize; }
            set { _batchSize = value < MIN_BATCH_SIZE ? MIN_BATCH_SIZE : value; }
        }

        protected void OnTimer(object stateInfo)
        {
            for (int i = 0; i < this.BatchSize; i++)
            {
                WriteLog($"{RandomString(_size - MIN_SIZE)}");
            }
        }

        protected abstract void WriteLog(string v);

        protected void ParseOptionValues(string[] args)
        {
            var options = args.Where(s => s.StartsWith("-"));
            foreach (string option in options)
            {
                if (option.StartsWith("-t"))
                {
                    if (int.TryParse(option.Substring(2), out int interval))
                    {
                        _interval = interval;
                    }
                }
                else if (option.StartsWith("-s"))
                {
                    if (int.TryParse(option.Substring(2), out int size))
                    {
                        _size = size;
                    }
                }
                else if (option.StartsWith("-b"))
                {
                    if (int.TryParse(option.Substring(2), out int batzhSize))
                    {
                        _batchSize = batzhSize;
                    }
                }
            }
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _timer.Dispose();
                }

                disposedValue = true;
            }
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
        }
        #endregion
    }
}
