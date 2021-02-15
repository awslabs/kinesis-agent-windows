using System;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Windows.Forms;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Hosting;
using Amazon.KinesisTap.Shared;
using Amazon.KinesisTap.Windows;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.EventLog;
using NLog.Extensions.Logging;

namespace Amazon.KinesisTap
{
    public partial class frmMain : Form
    {
        private KinesisTapServiceManager _serviceManager;
        private readonly ILoggerFactory _serviceLoggerFactory;
        private readonly IParameterStore _parameterStore = new RegistryParameterStore();

        public frmMain()
        {
            InitializeComponent();

            // initialize service-level logging
            if (!EventLog.SourceExists(KinesisTapServiceManager.ServiceName))
                EventLog.CreateEventSource(KinesisTapServiceManager.ServiceName, "Application");
            _serviceLoggerFactory = new LoggerFactory();
            _serviceLoggerFactory.AddEventLog(new EventLogSettings
            {
                SourceName = KinesisTapServiceManager.ServiceName,
                LogName = "Application",
                Filter = (msg, level) => level >= LogLevel.Information
            });
#if DEBUG
            _serviceLoggerFactory.AddConsole();
#endif
            _serviceLoggerFactory.AddNLog();
        }

        private void btnStart_Click(object sender, EventArgs e)
        {
            btnStart.Enabled = false;
            _parameterStore.StoreConventionalValues();
            var nlogConfigPath = _parameterStore.GetParameter(HostingUtility.NLogConfigPathKey);
            NLog.LogManager.LoadConfiguration(nlogConfigPath);

            // configure logging
            _serviceManager = new KinesisTapServiceManager(new PluginLoader(), _parameterStore,
            _serviceLoggerFactory.CreateLogger<KinesisTapServiceManager>(), new DefaultNetworkStatusProvider());
            _serviceManager.Start();

            btnStop.Enabled = true;
        }

        private void btnStop_Click(object sender, EventArgs e)
        {
            btnStop.Enabled = false;
            //Off the UI thread
            Task.Run(() =>
            {
                _serviceManager?.Stop();
            }).Wait(5000);
            btnStart.Enabled = true;
        }

        private void frmMain_FormClosing(object sender, FormClosingEventArgs e)
        {
        }
    }
}
