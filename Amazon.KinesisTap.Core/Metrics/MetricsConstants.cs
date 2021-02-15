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

namespace Amazon.KinesisTap.Core.Metrics
{
    public class MetricsConstants
    {
        //SessionManager
        public const string CONFIGS_LOADED = "ConfigsLoaded";
        public const string CONFIGS_FAILED_TO_LOAD = "ConfigsFailedToLoad";

        //Session
        public const string KINESISTAP_BUILD_NUMBER = "KinesisTapBuildNumber";
        public const string SOURCE_FACTORIES_LOADED = "SourceFactoriesLoaded";
        public const string SOURCE_FACTORIES_FAILED_TO_LOAD = "SourceFactoriesFailedToLoad";

        public const string SINK_FACTORIES_LOADED = "SinkFactoriesLoaded";
        public const string SINK_FACTORIES_FAILED_TO_LOAD = "SinkFactoriesFailedToLoad";

        public const string CREDENTIAL_PROVIDER_FACTORIES_LOADED = "CredentialProviderFactoriesLoaded";
        public const string CREDENTIAL_PROVIDER_FACTORIES_FAILED_TO_LOAD = "CredentialProviderFactoriesFailedToLoad";

        public const string GENERIC_PLUGIN_FACTORIES_LOADED = "GenericPluginFactoriesLoaded";
        public const string GENERIC_PLUGIN_FACTORIES_FAILED_TO_LOAD = "GenericPluginFactoriesFailedToLoad";

        public const string SINKS_STARTED = "SinksStarted";
        public const string SINKS_FAILED_TO_START = "SinksFailedToStart";

        public const string SOURCES_STARTED = "SourcesStarted";
        public const string SOURCES_FAILED_TO_START = "SourcesFailedToStart";

        public const string PIPE_FACTORIES_LOADED = "PipeFactoriesLoaded";
        public const string PIPE_FACTORIES_FAILED_TO_LOAD = "PipeFactoriesFailedToLoad";
        public const string PIPES_CONNECTED = "PipesConnected";
        public const string PIPES_FAILED_TO_CONNECT = "PipesFailedToConnect";

        public const string SELF_UPDATE_FREQUENCY = "SelfUpdateFrequency";

        public const string CONFIG_RELOAD_COUNT = "ConfigReloadCount";
        public const string CONFIG_RELOAD_FAILED_COUNT = "ConfigReloadFailedCount";

        //Sources
        //Directory source
        public const string DIRECTORY_SOURCE_BYTES_TO_READ = "DirectorySourceBytesToRead";
        public const string DIRECTORY_SOURCE_FILES_TO_PROCESS = "DirectorySourceFilesToProcess";
        public const string DIRECTORY_SOURCE_BYTES_READ = "DirectorySourceBytesRead";
        public const string DIRECTORY_SOURCE_RECORDS_READ = "DirectorySourceRecordsRead";
        //Eventlog source
        public const string EVENTLOG_SOURCE_EVENTS_READ = "EventLogSourceEventsRead";
        public const string EVENTLOG_SOURCE_EVENTS_ERROR = "EventLogSourceEventsError";
        //EtwEvent Source
        public const string ETWEVENT_SOURCE_EVENTS_READ = "EtwEventSourceEventsRead";
        public const string ETWEVENT_SOURCE_EVENTS_ERROR = "EtwEventSourceEventsError";

        //AWS Sinks
        public const string RECOVERABLE_SERVICE_ERRORS = "RecoverableServiceErrors";
        public const string NONRECOVERABLE_SERVICE_ERRORS = "NonrecoverableServiceErrors";
        public const string SERVICE_SUCCESS = "ServiceSuccess";
        public const string RECORDS_ATTEMPTED = "RecordsAttempted";
        public const string BYTES_ATTEMPTED = "BytesAccepted";
        public const string RECORDS_SUCCESS = "RecordsSuccess";
        public const string RECORDS_FAILED_RECOVERABLE = "RecordsFailedRecoverable";
        public const string RECORDS_FAILED_NONRECOVERABLE = "RecordsFailedNonrecoverable";
        public const string LATENCY = "Latency";
        public const string CLIENT_LATENCY = "ClientLatency";
        public const string BATCHES_IN_MEMORY_BUFFER = "BatchesLeftInMemoryBuffer";
        public const string BATCHES_IN_PERSISTENT_QUEUE = "BatchesLeftInPersistentQueue";
        public const string IN_MEMORY_BUFFER_FULL = "InMemoryBufferFull";
        public const string PERSISTENT_QUEUE_FULL = "PersistentQueueFull";

        public const string CLOUDWATCHLOG_PREFIX = "CloudWatchLog";
        public const string KINESIS_FIREHOSE_PREFIX = "KinesisFirehose";
        public const string KINESIS_STREAM_PREFIX = "KinesisStream";
        public const string CLOUDWATCH_PREFIX = "CloudWatch";

        //Plugins
        public const string RESTARTED_SINCE_LAST_RUN = "RestartedSinceLastRun";
        public const string MEMORY_MONITOR_PREFIX = "MemoryMonitor";

        //Categories
        public const string CATEGORY_SOURCE = "Source";
        public const string CATEGORY_SINK = "Sink";
        public const string CATEGORY_PROGRAM = "Program";
        public const string CATEGORY_PLUGIN = "Plugin";

        //Generic Plugins
        public const string PLUGINS_STARTED = "PluginsStarted";
        public const string PLUGINS_FAILED_TO_START = "PluginsFailedToStart";

        //Parser
        public const string PARSER_FACTORIES_LOADED = "ParserFactoriesLoaded";
        public const string PARSER_FACTORIES_FAILED_TO_LOAD = "ParserFactoriesFailedToLoad";
    }
}
