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
using System.Text.RegularExpressions;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Watch a direction for log files
    /// </summary>
    public class DirectorySource<TData, TContext> : DependentEventSource<TData>, IBookmarkable where TContext : LogContext
    {
        //Exclude well-known compressed files when using wild-card filter *.*
        private static readonly string[] _excludedExtensions = new string[] { ".zip", ".gz", ".bz2" };

        private FileSystemWatcher _watcher;
        private readonly string _directory;
        private readonly string _filterSpec;
        private readonly string[] _fileFilters;
        private readonly Regex[] _fileFilterRegexs;
        private readonly int _interval;
        private readonly int _skipLines;
        private Timer _timer;
        private ISubject<IEnvelope<TData>> _recordSubject = new Subject<IEnvelope<TData>>();
        private bool _hasBookmark;
        private readonly object _bookmarkFileLock = new object();

        protected bool _started;
        protected IRecordParser<TData, TContext> _recordParser;
        protected ISet<string> _buffer = new HashSet<string>();
        protected IDictionary<string, TContext> _logFiles = new ConcurrentDictionary<string, TContext>();
        protected Func<string, long, TContext> _logSourceInfoFactory;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="directory">Path of the directory to monitor</param>
        /// <param name="filterSpec">File name filter</param>
        /// <param name="logger">Logger</param>
        public DirectorySource(
            string directory, 
            string filterSpec, 
            int interval,
            IPlugInContext context, 
            IRecordParser<TData, TContext> recordParser,
            Func<string, long, TContext> logSourceInfoFactory
        ) : base(new DirectoryDependency(directory), context)
        {
            Guard.ArgumentNotNullOrEmpty(directory, nameof(directory));
            _directory = directory;
            _filterSpec = filterSpec;
            _fileFilters = ParseFilterSpec(filterSpec);
            if (_fileFilters.Length > 1)
            {
                _fileFilterRegexs = _fileFilters.Select(ff => new Regex(Utility.WildcardToRegex(ff, true)))
                    .ToArray();
            }
            _interval = interval;
            _recordParser = recordParser;
            _logSourceInfoFactory = logSourceInfoFactory;
            if (_config != null)
            {
                _skipLines = Utility.ParseInteger(_config["SkipLines"], 0);
            }

            _timer = new Timer(OnTimer, null, Timeout.Infinite, Timeout.Infinite);

        }

        /// <summary>
        /// Parse filter specification and return a list of filters
        /// </summary>
        /// <param name="filterSpec">Filter Specification to parse.</param>
        /// <returns>An array of filters. Should contain at least 1 or it will throw exception.</returns>
        private string[] ParseFilterSpec(string filterSpec)
        {
            string[] filters;
            if (string.IsNullOrWhiteSpace(filterSpec))
            {
                filters = new string[] { "*.*" };
            }
            else
            {
                string[] tempfilters = filterSpec.Split(new char[] { '|' }, StringSplitOptions.RemoveEmptyEntries);
                List<string> acceptedFilters = new List<string>();
                foreach(var filter in tempfilters)
                {
                    if (ShouldExclude(filter))
                    {
                        _logger?.LogWarning($"Extension {Path.GetExtension(filter)} is not supported.");
                    }
                    else
                    {
                        acceptedFilters.Add(filter);
                    }
                }
                if (acceptedFilters.Count == 0)
                {
                    throw new ArgumentException("No acceptable filters.");
                }
                filters = acceptedFilters.ToArray();
            }
            return filters;
        }

        #region public methods
        public override void Start()
        {
            if (_dependency.IsDependencyAvailable())
            {
                if (_watcher == null)
                {
                    InitializeWatcher();
                }
            }
            else
            {
                Reset();
                return;
            }

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

            _logger?.LogInformation($"DirectorySource id {this.Id} watching directory {_directory} with filter {_filterSpec} started.");
        }

        /// <summary>
        /// Once the directory we want to watch exists, it is safe to start the source.
        /// </summary>
        protected override void AfterDependencyAvailable()
        {
            Start();
        }


        public override void Stop()
        {
            if (_watcher != null)
            {
                _watcher.EnableRaisingEvents = false;
            }
            _timer.Change(Timeout.Infinite, Timeout.Infinite);
            _started = false;
            SaveBookmark();

            _logger?.LogInformation($"DirectorySource id {this.Id} watching directory {_directory} with filter {_filterSpec} stopped.");
        }

        /// <summary>
        /// When it is detected that the directory to observe doesn't exist, shut down the existing watcher and then start
        /// polling for the directory to exist.
        /// </summary>
        public override void Reset()
        {
            _logger?.LogInformation($"Resetting DirectorySource id {this.Id}.");
            Stop();
            if (_watcher != null)
            {
                _watcher.Dispose();
                _watcher = null;
            }
            base.Reset();
        }

        public int NumberOfConsecutiveIOExceptionsToLogError { get; set; }

        public void LoadSavedBookmark()
        {
            lock (_bookmarkFileLock)
            {
                using (var fs = new FileStream(GetBookmarkFilePath(), FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (var sr = new StreamReader(fs))
                {
                    while (!sr.EndOfStream)
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
        }

        public void SaveBookmark()
        {
            // We don't gather the contents of the bookmark file outside of the lock because
            // we want to avoid a situation where two threads capture position info at slightly different times, and then they write the file out of sequence 
            // (older collected data after newer collected data) since that would lead to out of date bookmarks recorded in the bookmark file.  In other words
            // the gathering of position data and writing the file needs to be atomic.
            lock (_bookmarkFileLock)
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
                string fileName = e.Name;

                //Sometimes we receive event where e.name is null so we should just skip it
                if (string.IsNullOrEmpty(fileName) || ShouldExclude(fileName) || !ShouldInclude(fileName)) return;

                //The entries in _buffer should be deleted before _logfiles and added after _logfiles
                switch (e.ChangeType)
                {
                    case WatcherChangeTypes.Deleted:
                        RemoveFromBuffer(fileName);
                        _logFiles.Remove(fileName);
                        break;
                    case WatcherChangeTypes.Created:
                        if (!_logFiles.ContainsKey(fileName))
                        {
                            _logFiles[fileName] = _logSourceInfoFactory(e.FullPath, 0);
                            AddToBuffer(fileName);
                        }
                        break;
                    case WatcherChangeTypes.Changed:
                        AddToBuffer(fileName);
                        break;
                }
                _logger?.LogDebug($"ThreadId{Thread.CurrentThread.ManagedThreadId} File: {e.FullPath} ChangeType: {e.ChangeType}");
            }
            catch(Exception ex)
            {
                _logger?.LogError(ex.ToMinimized());
            }
        }

        protected virtual void OnRenamed(object source, RenamedEventArgs e)
        {
            try
            {
                //Sometimes we receive event where e.name is null so we should just skip it
                if (string.IsNullOrEmpty(e.Name) || string.IsNullOrEmpty(e.OldName)
                    || ShouldExclude(e.Name) || ShouldExclude(e.OldName)
                    || (!ShouldInclude(e.Name) && !ShouldInclude(e.OldName)))
                {
                    return;
                }

                //File name rotation
                RemoveFromBuffer(e.OldName);
                if (_logFiles.ContainsKey(e.OldName))
                {
                    var newSourceInfo = _logSourceInfoFactory(e.FullPath, _logFiles[e.OldName].Position);
                    newSourceInfo.LineNumber = _logFiles[e.OldName].LineNumber;
                    _logFiles[e.Name] = newSourceInfo;
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
                _logger?.LogError(ex.ToMinimized());
            }
        }

        protected void OnTimer(object stateInfo)
        {
            _timer.Change(Timeout.Infinite, Timeout.Infinite);
            try
            {
                if (!Directory.Exists(_directory))
                {
                    Reset();
                    return;
                }
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
                            _logger?.LogWarning($"File: {fi.Name} shrunk or truncated from {fileContext.Position} to {fi.Length}");
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
                _logger?.LogError(ex.ToMinimized());
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
                _logFiles.Add(fileName, sourceInfo);
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
                    _logger?.LogError(ex.ToMinimized());
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex.ToMinimized());
            }
            return (recordsRead, bytesRead);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (_watcher != null)
                {
                    _watcher?.Dispose();
                    _watcher = null;
                }
                if (_timer != null)
                {
                    _timer?.Dispose();
                    _timer = null;
                }
                base.Dispose(disposing);
            }
        }
        #endregion

        #region private methods
        /// <summary>
        /// Determine whether should exclude file based on file extension defined in _excludedExtensions
        /// E.g. .zip, .gz
        /// </summary>
        /// <param name="filePath">Path to the file</param>
        /// <returns>Whether the file should be excluded</returns>
        private static bool ShouldExclude(string filePath)
        {
            string extension = Path.GetExtension(filePath).ToLower();
            return _excludedExtensions.Any(ext => ext.Equals(extension));
        }

        /// <summary>
        /// Determine whether should include the file when there are multiple file filters.
        /// If there is only one file filter, the filter is handled by FileSystemWatcher
        /// If there are multiple file filters, such as "*.log|*.txt", this function will determine whether the file should be included
        /// </summary>
        /// <param name="fileName">File Name</param>
        /// <returns>Whether the file should be included</returns>
        private bool ShouldInclude(string fileName)
        {
            if (_fileFilters.Length <= 1) return true;
            foreach(var regex in _fileFilterRegexs)
            {
                if (regex.IsMatch(fileName)) return true;
            }
            return false;
        }

        private void InitializeWatcher()
        {
            //If there are multiple filters, we will filter the files in the event handlers
            _watcher = new FileSystemWatcher
            {
                Path = _directory,
                Filter = _fileFilters.Length == 1 ? _fileFilters[0] : "*.*",
                NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.FileName | NotifyFilters.Size
            };

            _watcher.Changed += new FileSystemEventHandler(this.OnChanged);
            _watcher.Created += new FileSystemEventHandler(this.OnChanged);
            _watcher.Deleted += new FileSystemEventHandler(this.OnChanged);
            _watcher.Renamed += new RenamedEventHandler(this.OnRenamed);
        }

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
            var candidateFiles = _fileFilters.SelectMany(filter => Directory.GetFiles(_directory, filter))
                .Where(file => !ShouldExclude(file));

            if (_fileFilters.Length > 1)
            {
                //If there are multiple filters, they may overlap so we need to dedupe
                candidateFiles = candidateFiles.Distinct(); 
            }
            string[] files = candidateFiles.ToArray();

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

        private void AddToBuffer(string fileName)
        {
            lock (_buffer)
            {
                _buffer.Add(fileName);
            }
        }

        private void RemoveFromBuffer(string fileName)
        {
            lock (_buffer)
            {
                _buffer.Remove(fileName);
            }
        }

        #endregion
    }
}
