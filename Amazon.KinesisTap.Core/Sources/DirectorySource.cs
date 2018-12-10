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
using System.IO;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using System.Linq;
using System.Reactive.Linq;
using System.Threading;
using System.Reactive.Subjects;
using Newtonsoft.Json;
using System.Collections.Concurrent;
using Amazon.KinesisTap.Core.Metrics;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Watch a direction for log files
    /// </summary>
    public class DirectorySource<TData, TContext> : EventSource<TData>, IDisposable, IBookmarkable where TContext : LogContext
    {
        private readonly FileSystemWatcher _watcher;
        private readonly string _directory;
        private readonly string _filter;
        private readonly int _interval;
        private readonly int _skipLines;
        private Timer _timer;
        private ISubject<IEnvelope<TData>> _recordSubject = new Subject<IEnvelope<TData>>();
        private bool _hasBookmark;

        protected bool _started;
        protected IRecordParser<TData, TContext> _recordParser;
        protected ISet<string> _buffer = new HashSet<string>();
        protected IDictionary<string, TContext> _logFiles = new ConcurrentDictionary<string, TContext>();
        protected Func<string, long, TContext> _logSourceInfoFactory;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="directory">Path of the directory to monitor</param>
        /// <param name="filter">File name filter</param>
        /// <param name="logger">Logger</param>
        public DirectorySource(
            string directory, 
            string filter, 
            int interval,
            IPlugInContext context, 
            IRecordParser<TData, TContext> recordParser,
            Func<string, long, TContext> logSourceInfoFactory
        ) : base(context)
        {
            Guard.ArgumentNotNullOrEmpty(directory, nameof(directory));
            _directory = directory;
            _filter = filter ?? "" ;
            _interval = interval;
            _recordParser = recordParser;
            _logSourceInfoFactory = logSourceInfoFactory;
            if (_config != null)
            {
                _skipLines = Utility.ParseInteger(_config["SkipLines"], 0);
            }

            _timer = new Timer(OnTimer, null, Timeout.Infinite, Timeout.Infinite);

            _watcher = new FileSystemWatcher();
            _watcher.Path = directory;
            _watcher.Filter = filter;
            _watcher.NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.FileName | NotifyFilters.Size;

            _watcher.Changed += new FileSystemEventHandler(this.OnChanged);
            _watcher.Created += new FileSystemEventHandler(this.OnChanged);
            _watcher.Deleted += new FileSystemEventHandler(this.OnChanged);
            _watcher.Renamed += new RenamedEventHandler(this.OnRenamed);
        }


        #region public methods
        public override void Start()
        {
            if (this.InitialPosition != InitialPositionEnum.EOS && File.Exists(GetBookmarkFilePath()))
            {
                _hasBookmark = true;
                LoadSavedBookmark();
            }
            ReadBookmarkFromLogFiles();
            _started = true;
            _timer.Change(_interval, Timeout.Infinite);
            _watcher.EnableRaisingEvents = true;

            _metrics?.InitializeCounters(this.Id, MetricsConstants.CATEGORY_SOURCE, CounterTypeEnum.Increment, new Dictionary<string, MetricValue>()
                {
                    { MetricsConstants.DIRECTORY_SOURCE_RECORDS_READ, MetricValue.ZeroCount },
                    { MetricsConstants.DIRECTORY_SOURCE_BYTES_READ, MetricValue.ZeroBytes }

                });

            _logger?.LogInformation($"DirectorySource id {this.Id} watching directory {_directory} with filter {_filter} started.");
        }

        public override void Stop()
        {
            _watcher.EnableRaisingEvents = false;
            _timer.Change(Timeout.Infinite, Timeout.Infinite);
            _started = false;
            SaveBookmark();

            _logger?.LogInformation($"DirectorySource id {this.Id} watching directory {_directory} with filter {_filter} stopped.");
        }

        public void Dispose()
        {
            Dispose(true);
        }

        public int NumberOfConsecutiveIOExceptionsToLogError { get; set; }

        public void LoadSavedBookmark()
        {
            using (var fs = new FileStream(GetBookmarkFilePath(), FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var sr = new StreamReader(fs))
            {
                while(!sr.EndOfStream)
                {
                    string line = sr.ReadLine();
                    if (!string.IsNullOrWhiteSpace(line))
                    {
                        string[] parts = line.Split(',');
                        _logFiles[Path.GetFileName(parts[0])] = _logSourceInfoFactory(parts[0], long.Parse(parts[1]));
                    }
                }
            }
        }

        public void SaveBookmark()
        {
            string bookmarkDir = Path.GetDirectoryName(GetBookmarkFilePath());
            if (!Directory.Exists(bookmarkDir))
            {
                Directory.CreateDirectory(bookmarkDir);
            }
            if (InitialPosition != InitialPositionEnum.EOS)
            {
                using (var fs = File.OpenWrite(GetBookmarkFilePath()))
                using (var sw = new StreamWriter(fs))
                {
                    foreach (var logFile in _logFiles.Values)
                    {
                        sw.WriteLine($"{logFile.FilePath},{logFile.Position}");
                    }
                }
            }
        }

        public override IDisposable Subscribe(IObserver<IEnvelope<TData>> observer)
        {
            return this._recordSubject.Subscribe(observer);
        }
        #endregion

        #region protected methods
        protected virtual void OnChanged(object source, FileSystemEventArgs e)
        {
            try
            {
                //Sometimes we receive event where e.name is null so we should just skip it
                string filename = e.Name;
                if (string.IsNullOrEmpty(filename))
                {
                    return;
                }

                //The entries in _buffer should be deleted before _logfiles and added after _logfiles
                switch (e.ChangeType)
                {
                    case WatcherChangeTypes.Deleted:
                        RemoveFromBuffer(filename);
                        _logFiles.Remove(filename);
                        break;
                    case WatcherChangeTypes.Created:
                        _logFiles[filename] = _logSourceInfoFactory(e.FullPath, 0);
                        AddToBuffer(filename);
                        break;
                    case WatcherChangeTypes.Changed:
                        AddToBuffer(filename);
                        break;
                }
                _logger?.LogDebug($"ThreadId{Thread.CurrentThread.ManagedThreadId} File: {e.FullPath} ChangeType: {e.ChangeType}");
            }
            catch(Exception ex)
            {
                _logger?.LogError(ex.ToString());
            }
        }

        protected virtual void OnRenamed(object source, RenamedEventArgs e)
        {
            try
            {
                //Sometimes we receive event where e.name is null so we should just skip it
                if (string.IsNullOrEmpty(e.Name) || string.IsNullOrEmpty(e.OldName))
                {
                    return;
                }

                //File name rotation
                RemoveFromBuffer(e.OldName);
                if (_logFiles.ContainsKey(e.OldName))
                {
                    _logFiles[e.Name] = _logSourceInfoFactory(e.FullPath, _logFiles[e.OldName].Position);
                    _logFiles.Remove(e.OldName);
                }
                else
                {
                    _logFiles.Add(e.Name, _logSourceInfoFactory(e.FullPath, 0));
                }
                AddToBuffer(e.Name);
                _logger?.LogInformation("File: {0} renamed to {1}", e.OldFullPath, e.FullPath);
            }
            catch(Exception ex)
            {
                _logger?.LogError(ex.ToString());
            }
        }

        protected void OnTimer(object stateInfo)
        {
            _timer.Change(Timeout.Infinite, Timeout.Infinite);
            try
            {
                //First to check whether filewatch events were missed
                long bytesToRead = 0;
                long filesToProcess = 0;
                foreach (string fileName in _logFiles.Keys)
                {
                    if (!_started) break;
                    try
                    {
                        TContext fileContext = _logFiles[fileName];
                        FileInfo fi = new FileInfo(fileContext.FilePath);
                        long fileLength = fi.Length;
                        if (fileLength == fileContext.Position) //No change
                        {
                            continue;
                        }
                        else if (fileLength < fileContext.Position) //shrink or truncate
                        {
                            _logger?.LogWarning($"File: {fi.Name} shrinked or truncated from {fileContext.Position} to {fi.Length}");
                            //Other than malicious attack, the most likely scenario is file truncate so we will read from the beginning
                            fileContext.Position = 0;
                        }
                        bytesToRead += fi.Length - fileContext.Position;
                        filesToProcess++;
                        AddToBuffer(fileName);
                    }
                    catch { }
                }

                _metrics?.PublishCounters(this.Id, MetricsConstants.CATEGORY_SOURCE, Metrics.CounterTypeEnum.CurrentValue, new Dictionary<string, MetricValue>()
                {
                    { MetricsConstants.DIRECTORY_SOURCE_BYTES_TO_READ, new MetricValue(bytesToRead, MetricUnit.Bytes) },
                    { MetricsConstants.DIRECTORY_SOURCE_FILES_TO_PROCESS, new MetricValue(filesToProcess) },
                });

                string[] files = null;
                lock (_buffer)
                {
                     files = new string[_buffer.Count];
                    _buffer.CopyTo(files, 0);
                    _buffer.Clear();
                }

                (long recordsRead, long bytesRead) = ParseLogFiles(files);
                SaveBookmark();

               _metrics?.PublishCounters(this.Id, MetricsConstants.CATEGORY_SOURCE, Metrics.CounterTypeEnum.Increment, new Dictionary<string, MetricValue>()
                {
                    { MetricsConstants.DIRECTORY_SOURCE_RECORDS_READ, new MetricValue(recordsRead) },
                    { MetricsConstants.DIRECTORY_SOURCE_BYTES_READ, new MetricValue(bytesRead, MetricUnit.Bytes) },
                });

            }
            catch (Exception ex)
            {
                _logger?.LogError(ex.ToString());
            }
            finally
            {
                if (_started)
                {
                    _timer.Change(_interval, Timeout.Infinite);
                }
            }
        }

        protected virtual (long recordsRead, long bytesRead) ParseLogFile(string fileName, string fullPath)
        {
            long recordsRead = 0;
            long bytesRead = 0;

            if (!_logFiles.TryGetValue(fileName, out TContext sourceInfo))
            {
                sourceInfo = _logSourceInfoFactory(fullPath, 0);
            }
            try
            {
                using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                    //The responsibility of the following line has been moved to parser in case the parser need to get the meta data before the position
                    //fs.Position = sourceInfo.Position; 
                    using (var sr = new StreamReader(fs))
                    {
                        var records = _recordParser.ParseRecords(sr, sourceInfo);
                        foreach (var record in records)
                        {
                            ILogEnvelope envelope = (ILogEnvelope)record;
                            if (record.Timestamp > (this.InitialPositionTimestamp ?? DateTime.MinValue)
                                && envelope.LineNumber > _skipLines)
                            {
                                _recordSubject.OnNext(record);
                                recordsRead++;
                            }
                            if (!_started) break;
                        }

                        //Need to grab the position before disposing the reader because disposing the reader will dispose the stream
                        bytesRead = fs.Position - sourceInfo.Position;
                        sourceInfo.Position = fs.Position;
                        sourceInfo.ConsecutiveIOExceptionCount = 0;
                    }
                }
            }
            catch(IOException ex)
            {
                //Add it back to buffer for processing
                AddToBuffer(fileName);
                sourceInfo.ConsecutiveIOExceptionCount++;
                if (sourceInfo.ConsecutiveIOExceptionCount >= this.NumberOfConsecutiveIOExceptionsToLogError)
                {
                    _logger?.LogError(ex.ToString());
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex.ToString());
            }
            return (recordsRead, bytesRead);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _watcher?.Dispose();
                _timer?.Dispose();
            }
        }
        #endregion

        #region private methods
        private (long recordsRead, long bytesRead) ParseLogFiles(string[] files)
        {
            var toProcess = files
                .Select(fn => (fileName: fn, fullPath: Path.Combine(_directory, fn)))
                .Where(_ => File.Exists(_.fullPath))
                .OrderBy(_ => new FileInfo(_.fullPath).LastWriteTime);

            long totalRecordsRead = 0;
            long totalBytesRead = 0;
            foreach (var (fileName, fullPath) in toProcess)
            {
                if (!_started) break;
                (long recordsRead, long bytesRead) = ParseLogFile(fileName, fullPath);
                totalRecordsRead += recordsRead;
                totalBytesRead += bytesRead;
            }
            return (totalRecordsRead, totalBytesRead);
        }

        private void ReadBookmarkFromLogFiles()
        {
            // If the filename filter is specified as an empty string, return all log files under the target directory

            string[] files = string.IsNullOrEmpty(_filter) ? Directory.GetFiles(_directory) : Directory.GetFiles(_directory, _filter);

            foreach (string filePath in files)
            {
                FileInfo fi = new FileInfo(filePath);
                string fileName = Path.GetFileName(filePath);
                long fileSize = fi.Length;
                switch(this.InitialPosition)
                {
                    case InitialPositionEnum.EOS:
                        _logFiles[fileName] = _logSourceInfoFactory(filePath, fi.Length);
                        break;
                    case InitialPositionEnum.BOS:
                        if (_hasBookmark)
                        {
                            ProcessNewOrExpandedFiles(filePath, fileName, fileSize);
                        }
                        else
                        {
                            //Process all files
                            _logFiles.Add(fileName, _logSourceInfoFactory(filePath, 0));
                            AddToBuffer(fileName);
                        }
                        break;
                    case InitialPositionEnum.Bookmark:
                        if (_hasBookmark)
                        {
                            ProcessNewOrExpandedFiles(filePath, fileName, fileSize);
                        }
                        else
                        {
                            _logFiles[fileName] = _logSourceInfoFactory(filePath, fi.Length);
                        }
                        break;
                    case InitialPositionEnum.Timestamp:
                        if (_hasBookmark)
                        {
                            ProcessNewOrExpandedFiles(filePath, fileName, fileSize);
                        }
                        else
                        {
                            DateTime fileDateTime = File.GetLastWriteTimeUtc(filePath);
                            if (fileDateTime > this.InitialPositionTimestamp)
                            {
                                _logFiles.Add(fileName, _logSourceInfoFactory(filePath, 0));
                                AddToBuffer(fileName);

                            }
                            else
                            {
                                _logFiles[fileName] = _logSourceInfoFactory(filePath, fi.Length);
                            }
                        }
                        break;
                    default:
                        throw new NotImplementedException($"Initial Position {this.InitialPosition} is not supported");
                }
            }
        }

        private void ProcessNewOrExpandedFiles(string filePath, string fileName, long fileSize)
        {
            //Only process new or expanded files
            if (_logFiles.TryGetValue(fileName, out TContext context))
            {
                if (fileSize > context.Position)
                {
                    //Expanded file
                    AddToBuffer(fileName);
                }
            }
            else
            {
                //New file
                _logFiles.Add(fileName, _logSourceInfoFactory(filePath, 0));
                AddToBuffer(fileName);
            }
        }

        private void AddToBuffer(string filename)
        {
            lock (_buffer)
            {
                _buffer.Add(filename);
            }
        }

        private void RemoveFromBuffer(string filename)
        {
            lock (_buffer)
            {
                _buffer.Remove(filename);
            }
        }
        #endregion
    }
}
