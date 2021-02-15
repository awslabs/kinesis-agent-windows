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
namespace Amazon.KinesisTap.Core
{
    public static class ConfigConstants
    {
        public const string ID = "Id";
        public const string CONFIG_DESCRIPTIVE_NAME = "DescriptiveName";
        public const string SOURCE_TYPE = "SourceType";
        public const string SINK_TYPE = "SinkType";
        public const string CREDENTIAL_TYPE = "CredentialType";
        public const string CREDENTIAL_REF = "CredentialRef";
        public const string ACCESS_KEY = "AccessKey";
        public const string SECRET_KEY = "SecretKey";
        public const string REGION = "Region";
        public const string BUFFER_INTERVAL = "BufferInterval";
        public const string BUFFER_SIZE = "BufferSize";
        public const string PROFILE_NAME = "ProfileName";
        public const string ROLE_ARN = "RoleARN";
        public const string FORMAT = "Format";
        public const string TEXT_DECORATION = "TextDecoration";
        public const string TEXT_DECORATION_EX = "TextDecorationEx";
        public const string OBJECT_DECORATION = "ObjectDecoration";
        public const string OBJECT_DECORATION_EX = "ObjectDecorationEx";
        public const string RECORDS_PER_SECOND = "RecordsPerSecond";
        public const string BYTES_PER_SECOND = "BytesPerSecond";
        public const string INTERVAL = "Interval";
        public const string EPOCH = "epoch";
        public const string REQUESTS_PER_SECOND = "RequestsPerSecond";
        public const string RECORD_COUNT = "RecordCount";
        public const string MAX_BATCH_SIZE = "MaxBatchSize";
        public const string CUSTOM_AWS_CLIENT_HEADERS = "CustomAWSClientHeaders";

        // Directory sources attributes
        public const string DEFAULT_FIELD_MAPPING = "DefaultFieldMapping";

        // Regex filter pipe attributes
        public const string FILTER_PATTERN = "FilterPattern";
        public const string MULTILINE = "Multiline";
        public const string IGNORE_CASE = "IgnoreCase";
        public const string RIGHT_TO_LEFT = "RightToLeft";
        public const string NEGATE = "Negate";

        // Proxy and alternate endpoint support
        public const string PROXY_HOST = "ProxyHost";
        public const string PROXY_PORT = "ProxyPort";
        public const string SERVICE_URL = "ServiceURL";
        public const string USE_STS_REGIONAL_ENDPOINTS = "UseSTSRegionalEndpoints";
        public const string STS_REGIONAL_ENDPOINTS_ENV_VARIABLE = "AWS_STS_REGIONAL_ENDPOINTS";

        public const string QUEUE_TYPE = "QueueType";
        public const string QUEUE_TYPE_MEMORY = "memory";
        public const string QUEUE_TYPE_FILE = "file";
        public const string QUEUE_MAX_BATCHES = "QueueMaxBatches";
        public const string QUEUE_PATH = "QueuePath";
        public const string QUEUE = "Queue";

        public const string BOOKMARKS = "Bookmarks";

        public const double DEFAULT_BACKOFF_FACTOR = 0.5d;
        public const double DEFAULT_RECOVERY_FACTOR = 0.5d;
        public const double DEFAULT_JITTING_FACTOR = 0.1d;
        public const double DEFAULT_MIN_RATE_ADJUSTMENT_FACTOR = 1.0d / 128;
        public const int DEFAULT_MAX_ATTEMPTS = 1;

        public const string NEWLINE = "\n";

        public const string KINESISTAP_EXE_NAME = "KinesisTap.exe";
        public const string DONET = "dotnet";
        public const string KINESISTAP_STANDARD_PATH = @"C:\Program Files\Amazon\KinesisTap\" + KINESISTAP_EXE_NAME;

        public const string TYPE = "Type";
        public const string REQUIRED = "Required";

        //Environment variables
        public const string KINESISTAP_PROGRAM_DATA = "KINESISTAP_PROGRAM_DATA";
        public const string KINESISTAP_CONFIG_PATH = "KINESISTAP_CONFIG_PATH";
        //Environment varible for extra-config directory
        public const string KINESISTAP_EXTRA_CONFIG_DIR_PATH = "KINESISTAP_EXTRA_CONFIG_DIR_PATH";
        public const string COMPUTER_NAME = "computername";
        public const string USER_NAME = "USERNAME";

        //None-Windows
        public const string KINESISTAP_CORE = "Amazon.KinesisTap.ConsoleHost.dll";
        public const string LINUX_DEFAULT_PROGRAM_DATA_PATH = "/opt/amazon-kinesistap/var";
        public const string LINUX_DEFAULT_CONFIG_PATH = "/opt/amazon-kinesistap/etc";

        //Telemetry Connector
        public const string REDIRECT_TO_SINK_ID = "RedirectToSinkId";
        public const string TELEMETRY_CONNECTOR = "_TELEMETRY_CONNECTOR";

        //Network priority
        public const string UPLOAD_NETWORK_PRIORITY = "UploadNetworkPriority";
        public const string DOWNLOAD_NETWORK_PRIORITY = "DownloadNetworkPriority";
        public const int DEFAULT_NETWORK_PRIORITY = 3;

        //Formatting
        public const string FORMAT_JSON = "json";
        public const string FORMAT_XML = "xml";
        public const string FORMAT_XML_2 = "xml2";
        public const string FORMAT_RENDERED_XML = "renderedxml";
        public const string FORMAT_SUSHI = "sushi";
    }
}
