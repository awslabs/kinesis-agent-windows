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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Watch a direction for log files
    /// </summary>
    public class DirectorySource<TData, TContext> : DependentEventSource<TData>, IBookmarkable where TContext : LogContext, new()
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
        private readonly string _encoding;
        private Timer _timer;
        private ISubject<IEnvelope<TData>> _recordSubject = new Subject<IEnvelope<TData>>();
        private bool _hasBookmark;
        private readonly object _bookmarkFileLock = new object();
        private readonly bool includeSubdirectories;
        private readonly string[] includeDirectoryFilter;

        private readonly string bookmarkDir;
        private readonly bool bookmarkOnBufferFlush = false;
        private string bookmarkPath;

        protected bool _started;
        protected readonly IRecordParser<TData, TContext> _recordParser;
        protected readonly ISet<string> _buffer = new HashSet<string>();
        internal readonly IDictionary<string, TContext> _logFiles = new ConcurrentDictionary<string, TContext>();

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
            IRecordParser<TData, TContext> recordParser
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
            if (_config != null)
            {
                _skipLines = Utility.ParseInteger(_config["SkipLines"], 0);
                _encoding = _config["Encoding"];
                bool.TryParse(this._config["IncludeSubdirectories"], out bool result);
                this.includeSubdirectories = result ? bool.Parse(this._config["IncludeSubdirectories"]) : false;
                if (this.includeSubdirectories)
                {
                    this.includeDirectoryFilter = _config["IncludeDirectoryFilter"]?.Split(';');
                }
                if (bool.TryParse(_config["BookmarkOnBufferFlush"] ?? "false", out bool bookmarkOnBufferFlush))
                    this.bookmarkOnBufferFlush = bookmarkOnBufferFlush;
            }

            this.bookmarkDir = Path.GetDirectoryName(GetBookmarkFilePath());
            if (!Directory.Exists(this.bookmarkDir))
                Directory.CreateDirectory(this.bookmarkDir);

            _timer = new Timer(OnTimer, null, Timeout.Infinite, Timeout.Infinite);
            DelayBetweenDependencyPoll = TimeSpan.FromSeconds(5);
        }

        #region public methods
        public override void Start()
        {
            this.bookmarkPath = GetBookmarkFilePath();
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

            if (this.InitialPosition != InitialPositionEnum.EOS && File.Exists(this.bookmarkPath))
            {
                _hasBookmark = true;
                try
                {
                    LoadSavedBookmark();
                }
                catch
                {
                    //Error is already logged. Fall back to Bookmark mode for the session
                    //ReadBookmarkFromLogFiles below will recreate bookmark
                    this.InitialPosition = InitialPositionEnum.Bookmark;
                }
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
                _watcher.EnableRaisingEvents = false;
            _timer.Change(Timeout.Infinite, Timeout.Infinite);

            SaveBookmark();
            _started = false;
            if (this.bookmarkOnBufferFlush)
                _logFiles.Clear();

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
                try
                {
                    using (var fs = new FileStream(this.bookmarkPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                    using (var sr = new StreamReader(fs))
                    {
                        while (!sr.EndOfStream)
                        {
                            string line = sr.ReadLine();
                            if (!string.IsNullOrWhiteSpace(line))
                            {
                                try
                                {
                                    string[] parts = line.Split(',');
                                    var logSource = CreateLogSourceInfo(parts[0], long.Parse(parts[1]));
                                    _logFiles[GetRelativeFilePath(parts[0], _directory)] = logSource;
                                    if (this.bookmarkOnBufferFlush)
                                        BookmarkManager.RegisterBookmark(this.GetBookmarkName(logSource.FilePath), logSource.Position, (id) => this.SaveBookmark());
                                }
                                catch (Exception ex)
                                {
                                    //Allow continue processing because it is legitimate for system to remove log files while the agent is stopped
                                    _logger?.LogWarning($"Fail to process bookmark {line}: {ex.ToMinimized()}");
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger?.LogError($"Failed loading bookmark: {ex.ToMinimized()}");
                    throw; //Inform caller the error
                }
            }
        }

        public void SaveBookmark()
        {
            if (this.InitialPosition == InitialPositionEnum.EOS || !this._started) return;

            // We don't gather the contents of the bookmark file outside of the lock because
            // we want to avoid a situation where two threads capture position info at slightly different times, and then they write the file out of sequence 
            // (older collected data after newer collected data) since that would lead to out of date bookmarks recorded in the bookmark file.  In other words
            // the gathering of position data and writing the file needs to be atomic.
            lock (_bookmarkFileLock)
            {
                try
                {
                    using (var fs = new FileStream(this.bookmarkPath, FileMode.Create, FileAccess.Write, FileShare.None))
                    using (var sw = new StreamWriter(fs))
                    {
                        foreach (var logFile in _logFiles.Values)
                        {
                            long position;
                            if (this.bookmarkOnBufferFlush)
                                position = BookmarkManager.GetBookmark(this.GetBookmarkName(logFile.FilePath))?.Position ?? logFile.Position;
                            else
                                position = logFile.Position;

                            sw.WriteLine($"{logFile.FilePath},{position}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger?.LogError($"Failed saving bookmark: {ex.ToMinimized()}");
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
                if (this.includeSubdirectories && this.ShouldSkip(e.FullPath))
                    return;

                string relativeFilePath = e.Name;

                //Sometimes we receive event where e.name is null so we should just skip it
                if (string.IsNullOrEmpty(relativeFilePath) || ShouldExclude(relativeFilePath) || !ShouldInclude(relativeFilePath)) return;

                //The entries in _buffer should be deleted before _logfiles and added after _logfiles
                switch (e.ChangeType)
                {
                    case WatcherChangeTypes.Deleted:
                        if (!File.Exists(e.FullPath)) //macOS sometimes fires this event when a file is created so we need this extra check.
                        {
                            RemoveFromBuffer(relativeFilePath);
                            _logFiles.Remove(relativeFilePath);
                            BookmarkManager.RemoveBookmark(this.GetBookmarkName(e.FullPath));
                        }
                        break;
                    case WatcherChangeTypes.Created:
                        if (!_logFiles.ContainsKey(relativeFilePath))
                        {
                            _logFiles[relativeFilePath] = CreateLogSourceInfo(e.FullPath, 0);
                            if (this.bookmarkOnBufferFlush)
                                BookmarkManager.RegisterBookmark(this.GetBookmarkName(e.FullPath), 0, (pos) => this.SaveBookmark());
                            AddToBuffer(relativeFilePath);
                        }
                        break;
                    case WatcherChangeTypes.Changed:
                        AddToBuffer(relativeFilePath);
                        break;
                }
                _logger?.LogDebug($"ThreadId{Thread.CurrentThread.ManagedThreadId} File: {e.FullPath} ChangeType: {e.ChangeType}");
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex.ToMinimized());
            }
        }

        protected virtual void OnRenamed(object source, RenamedEventArgs e)
        {
            try
            {
                // this is for Subdirectories check only
                if (this.includeSubdirectories && this.ShouldSkip(e.FullPath))
                    return;

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
                    var newSourceInfo = CreateLogSourceInfo(e.FullPath, _logFiles[e.OldName].Position);
                    newSourceInfo.LineNumber = _logFiles[e.OldName].LineNumber;
                    _logFiles[e.Name] = newSourceInfo;
                    _logFiles.Remove(e.OldName);

                    var bookmark = BookmarkManager.GetBookmark(this.GetBookmarkName(e.OldFullPath));
                    if (bookmark != null)
                    {
                        BookmarkManager.RemoveBookmark(bookmark.Id);
                        BookmarkManager.RegisterBookmark(this.GetBookmarkName(e.FullPath), bookmark.Position, (id) => this.SaveBookmark());
                    }
                }
                else
                {
                    var newSource = CreateLogSourceInfo(e.FullPath, 0);
                    _logFiles.Add(e.Name, newSource);
                    BookmarkManager.RegisterBookmark(this.GetBookmarkName(e.FullPath), 0, (id) => this.SaveBookmark());
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex.ToMinimized());
            }
            finally
            {
                AddToBuffer(e.Name);
                _logger?.LogInformation("File: {0} renamed to {1}", e.OldFullPath, e.FullPath);
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

                foreach (string relativeFilePath in _logFiles.Keys)
                {
                    if (!_started) break;
                    try
                    {
                        TContext fileContext = _logFiles[relativeFilePath];
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
                            BookmarkManager.ResetBookmarkPosition(this.GetBookmarkName(fileContext.FilePath), -1);
                        }
                        bytesToRead += fi.Length - fileContext.Position;
                        filesToProcess++;
                        AddToBuffer(relativeFilePath);
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
                if (!this.bookmarkOnBufferFlush) SaveBookmark();

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

        protected virtual (long recordsRead, long bytesRead) ParseLogFile(string relativeFilePath, string fullPath)
        {
            long recordsRead = 0;
            long bytesRead = 0;

            int bookmarkId;
            if (!_logFiles.TryGetValue(relativeFilePath, out TContext sourceInfo))
            {
                sourceInfo = CreateLogSourceInfo(fullPath, 0);
                _logFiles.Add(relativeFilePath, sourceInfo);
                bookmarkId = this.bookmarkOnBufferFlush ? BookmarkManager.RegisterBookmark(this.GetBookmarkName(fullPath), 0, (pos) => this.SaveBookmark()).Id : 0;
            }
            else
            {
                bookmarkId = BookmarkManager.GetBookmarkId(this.GetBookmarkName(fullPath));
            }

            try
            {
                using (var fs = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                using (var sr = CreateStreamReader(fs, _encoding))
                {
                    var records = _recordParser.ParseRecords(sr, sourceInfo);
                    foreach (var record in records)
                    {
                        ILogEnvelope envelope = (ILogEnvelope)record;
                        if (envelope != null
                            && record.Timestamp > (this.InitialPositionTimestamp ?? DateTime.MinValue)
                            && envelope.LineNumber > _skipLines)
                        {
                            record.BookmarkId = bookmarkId;
                            _recordSubject.OnNext(record);
                            recordsRead++;
                        }

                        //Need to grab the position before disposing the reader because disposing the reader will dispose the stream
                        bytesRead = fs.Position - sourceInfo.Position;
                        sourceInfo.Position = fs.Position;

                        if (!_started) break;
                    }

                    sourceInfo.ConsecutiveIOExceptionCount = 0;
                }
            }
            catch (IOException ex)
            {
                //Add it back to buffer for processing
                AddToBuffer(relativeFilePath);
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
        /// Determine whether should skip watching this file
        /// </summary>
        /// <param name="fullPath">fullPath</param>
        /// <returns>Whether the file should be skipped</returns>
        private bool ShouldSkip(string fullPath)
        {
            // When a file is changed, it sends multiple change events for both the file and the directory. 
            // We only need to process the event for the file. 
            // That's why we skip the event for for the directory.
            if (File.GetAttributes(fullPath).HasFlag(FileAttributes.Directory))
                return true;

            return this.ShouldSkipSubDirectory(Path.GetDirectoryName(fullPath));
        }

        /// <summary>
        /// Determine whether should skip the directory based on includeDirectoryFilter
        /// </summary>
        /// <param name="subdirectory">subdirectory</param>
        /// <returns>Whether the subdirectory should be skipped</returns>
        private bool ShouldSkipSubDirectory(string subdirectory)
        {
            try
            {
                if (this.includeDirectoryFilter == null)
                    return false;

                if (subdirectory.Equals(_directory)) return false; // if it's the top level directory, we will always process files directly under it.
                var relativeDirectory = GetRelativeFilePath(subdirectory, _directory);
                if (this.includeDirectoryFilter.Any(i => i.Equals(relativeDirectory)))
                    return false;  // if the includeDirectory is set and the relativeDirectory is specified in includeDirectoryFilter, we will process it.
                else
                    return true;  // if the includeDirectory is set and the relativeDirectory is not specified in includeDirectoryFilter, we will skip it.
            }
            catch (Exception e)
            {
                this._logger.LogWarning($"Error occurred while checking if log source directory should get excluded: '{e}'");
            }

            return false;
        }

        /// <summary>
        /// Determine whether should include the file when there are multiple file filters.
        /// If there is only one file filter, the filter is handled by FileSystemWatcher
        /// If there are multiple file filters, such as "*.log|*.txt", this function will determine whether the file should be included
        /// </summary>
        /// <param name="relativeFilePath">Relative Path</param>
        /// <returns>Whether the file should be included</returns>
        private bool ShouldInclude(string relativeFilePath)
        {
            if (_fileFilters.Length <= 1) return true;
            foreach (var regex in _fileFilterRegexs)
            {
                if (regex.IsMatch(relativeFilePath)) return true;
            }
            return false;
        }

        private void InitializeWatcher()
        {
            //If there are multiple filters, we will filter the files in the event handlers
            _watcher = new FileSystemWatcher
            {
                Path = _directory,
                IncludeSubdirectories = this.includeSubdirectories,
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
                .Select(fn => (relativeFilePath: fn, fullPath: Path.Combine(_directory, fn)))
                .Where(_ => File.Exists(_.fullPath))
                .OrderBy(_ => new FileInfo(_.fullPath).LastWriteTime);

            long totalRecordsRead = 0;
            long totalBytesRead = 0;
            foreach (var (relativeFilePath, fullPath) in toProcess)
            {
                if (!_started) break;
                (long recordsRead, long bytesRead) = ParseLogFile(relativeFilePath, fullPath);
                totalRecordsRead += recordsRead;
                totalBytesRead += bytesRead;
            }
            return (totalRecordsRead, totalBytesRead);
        }

        private void ReadBookmarkFromLogFiles()
        {
            var candidateFiles = _fileFilters.SelectMany(filter => this.GetFiles(_directory, filter))
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
                var relativeFilePath = GetRelativeFilePath(filePath, _directory);
                long fileSize = fi.Length;

                if (_hasBookmark && this.InitialPosition != InitialPositionEnum.EOS)
                {
                    ProcessNewOrExpandedFiles(filePath, relativeFilePath, fileSize);
                    continue;
                }

                switch (this.InitialPosition)
                {
                    case InitialPositionEnum.EOS:
                        _logFiles[relativeFilePath] = CreateLogSourceInfo(filePath, fi.Length);
                        break;
                    case InitialPositionEnum.BOS:
                        //Process all files
                        _logFiles[relativeFilePath] = CreateLogSourceInfo(filePath, 0);
                        if (this.bookmarkOnBufferFlush)
                            BookmarkManager.RegisterBookmark(this.GetBookmarkName(filePath), 0, (pos) => this.SaveBookmark());
                        AddToBuffer(relativeFilePath);
                        break;
                    case InitialPositionEnum.Bookmark:
                        _logFiles[relativeFilePath] = CreateLogSourceInfo(filePath, fi.Length);
                        if (this.bookmarkOnBufferFlush)
                            BookmarkManager.RegisterBookmark(this.GetBookmarkName(filePath), fi.Length, (pos) => this.SaveBookmark());
                        break;
                    case InitialPositionEnum.Timestamp:
                        if (fi.LastWriteTimeUtc > this.InitialPositionTimestamp)
                        {
                            _logFiles[relativeFilePath] = CreateLogSourceInfo(filePath, 0);
                            if (this.bookmarkOnBufferFlush)
                                BookmarkManager.RegisterBookmark(this.GetBookmarkName(filePath), 0, (pos) => this.SaveBookmark());
                            AddToBuffer(relativeFilePath);
                        }
                        else
                        {
                            _logFiles[relativeFilePath] = CreateLogSourceInfo(filePath, fi.Length);
                            if (this.bookmarkOnBufferFlush)
                                BookmarkManager.RegisterBookmark(this.GetBookmarkName(filePath), fi.Length, (pos) => this.SaveBookmark());
                        }
                        break;
                    default:
                        throw new NotImplementedException($"Initial Position {this.InitialPosition} is not supported");
                }
            }
        }

        private string[] GetFiles(string directory, string filter)
        {
            var list = new List<string>();
            this.GetFilesHelper(list, directory, filter);
            return list.ToArray();
        }

        private void GetFilesHelper(List<string> list, string directory, string filter)
        {
            if (!this.includeSubdirectories || !this.ShouldSkipSubDirectory(directory))
                list.AddRange(Directory.GetFiles(directory, filter));

            foreach (var subdir in Directory.GetDirectories(directory))
            {
                GetFilesHelper(list, subdir, filter);
            }
        }

        private void ProcessNewOrExpandedFiles(string filePath, string relativeFilePath, long fileSize)
        {
            //Only process new or expanded files
            if (_logFiles.TryGetValue(relativeFilePath, out TContext context))
            {
                // If there is no registered bookmark and we're bookmarking on buffer flush,
                // that means the file was read and events buffered, but never uploaded by the source.
                var position = this.bookmarkOnBufferFlush
                    ? (BookmarkManager.GetBookmark(this.GetBookmarkName(filePath)) ?? BookmarkManager.RegisterBookmark(this.GetBookmarkName(filePath), 0, (pos) => this.SaveBookmark())).Position
                    : context.Position;

                if (fileSize > position)
                {
                    // The file is expanded compared with position saved in bookmark
                    AddToBuffer(relativeFilePath);
                }
            }
            else
            {
                //New file
                _logFiles.Add(relativeFilePath, CreateLogSourceInfo(filePath, 0));
                if (this.bookmarkOnBufferFlush)
                    BookmarkManager.RegisterBookmark(this.GetBookmarkName(filePath), 0, (pos) => this.SaveBookmark());
                AddToBuffer(relativeFilePath);
            }
        }

        private void AddToBuffer(string relativeFilePath)
        {
            lock (_buffer)
            {
                _buffer.Add(relativeFilePath);
            }
        }

        private void RemoveFromBuffer(string relativeFilePath)
        {
            lock (_buffer)
            {
                _buffer.Remove(relativeFilePath);
            }
        }

        private StreamReader CreateStreamReader(Stream stream, string encoding)
        {
            if (string.IsNullOrWhiteSpace(encoding))
            {
                return new StreamReader(stream);
            }
            else
            {
                return new StreamReader(stream, Encoding.GetEncoding(encoding));
            }
        }

        private TContext CreateLogSourceInfo(string filePath, long position)
        {
            var context = new TContext { FilePath = filePath, Position = position };
            if (position > 0)
            {
                for (int attempt = 1; attempt <= 10; attempt++)
                {
                    try
                    {
                        context.LineNumber = GetLineCount(filePath, position);
                        break;
                    }
                    catch (IOException ex)
                    {
                        // Rethrow immediately if max attempts has been reached
                        if (attempt == 10) throw;

                        // Check if the error was caused by a file lock. If so, attempt retry.
                        if (ex.Message.Contains("it is being used by another process"))
                        {
                            _logger?.LogWarning("{0} (attempt {1})", ex.Message, attempt);
                            Thread.Sleep(TimeSpan.FromSeconds(attempt));
                            continue;
                        }

                        // If the error wasn't relating to a file lock, rethrow.
                        throw;
                    }
                }
            }
            return context;
        }

        /// <summary>
        /// Get Unique Name for Bookmark
        /// </summary>
        /// <param name="filePath">filePath</param>
        /// <returns>Unique Name for Bookmark</returns>
        private string GetBookmarkName(string filePath)
        {
            return this.Id + "+" + filePath;
        }

        private static long GetLineCount(string filePath, long position)
        {
            long lineCount = 0;
            using (var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var sr = new StreamReader(fs))
            {
                while (!sr.EndOfStream && sr.BaseStream.Position < position)
                {
                    sr.ReadLine();
                    lineCount++;
                }
            }
            return lineCount;
        }

        private static string GetRelativeFilePath(string filePath, string directory)
        {
            // Directories must end in a slash
            if (!directory.EndsWith(Path.DirectorySeparatorChar.ToString()))
            {
                directory += Path.DirectorySeparatorChar;
            }
            return filePath.StartsWith(directory) ? filePath.Substring(directory.Length) : filePath;
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
                foreach (var filter in tempfilters)
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
        #endregion
    }
}
