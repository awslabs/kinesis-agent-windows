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
using System.Threading;

namespace Amazon.KinesisTap.DiagnosticTool.Core
{
    /// <summary>
    /// The class for Directory watcher
    /// </summary>
    public class DirectoryWatcher : IDisposable
    {
        private readonly string _directory;
        private readonly string _filer;
        readonly FileSystemWatcher _watcher;
        readonly TextWriter _writer;
        readonly Timer _timer;

        /// <summary>
        /// Directory Watcher constructor
        /// </summary>
        /// <param name="directory"></param>
        /// <param name="filter"></param>
        /// <param name="writer"></param>
        public DirectoryWatcher(string directory, string filter, TextWriter writer)
        {
            _directory = directory;
            _filer = filter;
            _writer = writer;

            _timer = new Timer(OnTimer, null, 1000, 1000);

            _watcher = new FileSystemWatcher();
            _watcher.Path = directory;
            _watcher.Filter = filter;
            _watcher.NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.FileName | NotifyFilters.Size;

            _watcher.Changed += new FileSystemEventHandler(OnChanged);
            _watcher.Created += new FileSystemEventHandler(OnChanged);
            _watcher.Deleted += new FileSystemEventHandler(OnChanged);
            _watcher.Renamed += new RenamedEventHandler(OnRenamed);

            _watcher.EnableRaisingEvents = true;
        }

        /// <summary>
        /// Print out message if there is remaning in the current directory
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void OnRenamed(object sender, RenamedEventArgs e)
        {
            _writer.WriteLine("File: {0} renamed to {1}", e.OldFullPath, e.FullPath);
        }

        /// <summary>
        /// Print out message if there is any file changed in the current directory
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void OnChanged(object sender, FileSystemEventArgs e)
        {
            _writer.WriteLine($"File: {e.FullPath} ChangeType: {e.ChangeType}");
        }

        protected void OnTimer(object stateInfo)
        {
            var files = Directory.GetFiles(_directory, _filer);
            foreach(var file in files)
            {
                var fi = new FileInfo(file);
                var l = fi.Length;
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            _watcher.Dispose();
            _timer.Dispose();
        }

        public void Dispose()
        {
            Dispose(true);
        }
    }
}
