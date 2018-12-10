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

namespace Amazon.KinesisTap.Core
{
    public static class ConfigConstants
    {
        public const string ID = "Id";
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

        public const string QUEUE_TYPE = "QueueType";
        public const string QUEUE_TYPE_MEMORY = "memory";
        public const string QUEUE_TYPE_FILE = "file";
        public const string QUEUE_MAX_BATCHES = "QueueMaxBatches";
        public const string QUEUE_PATH = "QueuePath";
        public const string QUEUE = "Queue";

        public const string KINESISTAP_CLIENTID = "KINESISTAP_CLIENTID";

        public const string BOOKMARKS = "Bookmarks";

        public const double DEFAULT_BACKOFF_FACTOR = 0.5d;
        public const double DEFAULT_RECOVERY_FACTOR = 0.5d;
        public const double DEFAULT_JITTING_FACTOR = 0.1d;
        public const double DEFAULT_MIN_RATE_ADJUSTMENT_FACTOR = 1.0d / 32;
        public const int DEFAULT_MAX_ATTEMPTS = 1;

        public const string NEWLINE = "\n";

        public const string KINESISTAP_EXE_NAME = "AWSKinesisTap.exe";
        public const string DONET = "dotnet";
        public const string KINESISTAP_STANDARD_PATH = @"C:\Program Files\Amazon\KinesisTap\" + KINESISTAP_EXE_NAME;

        public const string TYPE = "Type";

        //Environment variables
        public const string KINESISTAP_PROGRAM_DATA = "KINESISTAP_PROGRAM_DATA";
        public const string KINESISTAP_COFIG_PATH = "KINESISTAP_COFIG_PATH";

        //None-Windows
        public const string KINESISTAP_CORE = "Amazon.KinesisTap.ConsoleHost.dll";
        public const string LINUX_DEFAULT_PROGRAM_DATA_PATH = "/opt/amazon-kinesistap/var";
        public const string LINUX_DEFAULT_CONFIG_PATH = "/opt/amazon-kinesistap/etc";
    }
}
