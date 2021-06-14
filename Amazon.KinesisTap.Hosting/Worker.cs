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
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Amazon.KinesisTap.Hosting
{
    public class Worker : BackgroundService
    {
        /// <summary>
        /// Used to exit gracefully on MacOS
        /// </summary>
        public static IHostApplicationLifetime HostApplicationLifeTime;

        private readonly IParameterStore _parameterStore;
        private readonly ILogger _logger;
        private readonly ISessionManager _sessionManager;
        private readonly INetworkStatusProvider _defaultNetworkStatusProvider;

        private void GenerateUniqueClientID()
        {
            //Generate a unique client id for the KinesisTap based on the system properties
            string uniqueClientID = Utility.UniqueClientID;
            string currentUniqueClientID = _parameterStore.GetParameter(ConfigConstants.UNIQUE_CLIENT_ID);

            //Set Unique id here
            if (uniqueClientID != currentUniqueClientID)
            {
                _logger.LogInformation($"Unique Client ID of the system changed from '{currentUniqueClientID}' to '{uniqueClientID}' ");
                _parameterStore.SetParameter(ConfigConstants.UNIQUE_CLIENT_ID, uniqueClientID);
            }

            _logger.LogInformation($"Unique Client ID of the system is '{uniqueClientID}' ");
            _logger.LogInformation($"Unique System properties used to generate Unique Client ID is '{Utility.UniqueSystemProperties}' ");

            //Store the value in enviornment variable
            if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(ConfigConstants.UNIQUE_CLIENT_ID)))
            {
                Environment.SetEnvironmentVariable(ConfigConstants.UNIQUE_CLIENT_ID, uniqueClientID);
            }

            return;
        }

        public Worker(ILoggerFactory loggerFactory,
            IParameterStore parameterStore,
            ISessionManager sessionManager,
            INetworkStatusProvider defaultNetworkStatusProvider,
            IHostApplicationLifetime lifetime)
        {
            HostApplicationLifeTime = lifetime;

            _logger = loggerFactory.CreateLogger("KinesisTap");
            _parameterStore = parameterStore;
            _sessionManager = sessionManager;
            _defaultNetworkStatusProvider = defaultNetworkStatusProvider;
        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            _parameterStore.StoreConventionalValues();
            //Generate a unique client ID;
            GenerateUniqueClientID();
            await _defaultNetworkStatusProvider.StartAsync(cancellationToken);

            // call this to notify the OS that the service has started
            await base.StartAsync(cancellationToken);
            _logger.LogInformation("Started");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await _sessionManager.StartAsync(stoppingToken);
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            try
            {
                _logger.LogInformation("STOP signal received");
                await base.StopAsync(cancellationToken);
                await _sessionManager.StopAsync(cancellationToken);

                _logger.LogInformation("Stopped");
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("Service stopped before all sessions could be terminated");
            }
        }
    }
}
