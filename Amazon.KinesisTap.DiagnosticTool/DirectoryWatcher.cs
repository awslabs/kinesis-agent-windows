using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.DiagnosticTool
{
    public class DirectoryWatcher : IDisposable
    {
        private readonly string _directory;
        private readonly string _filer;

        FileSystemWatcher _watcher;
        TextWriter _writer;
        Timer _timer;

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

            _watcher.Changed += new FileSystemEventHandler(this.OnChanged);
            _watcher.Created += new FileSystemEventHandler(this.OnChanged);
            _watcher.Deleted += new FileSystemEventHandler(this.OnChanged);
            _watcher.Renamed += new RenamedEventHandler(this.OnRenamed);

            _watcher.EnableRaisingEvents = true;
        }

        private void OnRenamed(object sender, RenamedEventArgs e)
        {
            _writer.WriteLine("File: {0} renamed to {1}", e.OldFullPath, e.FullPath);
        }

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
