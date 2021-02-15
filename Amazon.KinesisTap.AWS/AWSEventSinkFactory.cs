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
using System.Net.Http;

using Amazon.CloudWatch;
using Amazon.CloudWatchLogs;
using Amazon.Kinesis;
using Amazon.KinesisFirehose;
using Amazon.KinesisTap.AWS.Telemetrics;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.AWS
{
    public class AWSEventSinkFactory : IFactory<IEventSink>
    {
        public const string CLOUD_WATCH_LOG_EMF = "cloudwatchlogsemf";
        private const string CLOUD_WATCH_LOG = "cloudwatchlogs";
        private const string CLOUD_WATCH = "cloudwatch";
        private const string KINESIS_FIREHOSE = "kinesisfirehose";
        private const string KINESIS_STREAM = "kinesisstream";
        private const string FILE_SYSTEM = "filesystem";
        private const string TELEMETRICS = "telemetrics";

        public IEventSink CreateInstance(string sinkType, IPlugInContext context)
        {
            IConfiguration config = context.Configuration;
            ILogger logger = context.Logger;

            switch (sinkType.ToLower())
            {
                case CLOUD_WATCH_LOG:
                case CLOUD_WATCH_LOG_EMF:
                    return new CloudWatchLogsSink(context, AWSUtilities.CreateAWSClient<AmazonCloudWatchLogsClient>(context));
                case KINESIS_FIREHOSE:
                    var firehoseSink = new KinesisFirehoseSink(context, AWSUtilities.CreateAWSClient<AmazonKinesisFirehoseClient>(context));
                    string combineRecords = config["CombineRecords"];
                    if (!string.IsNullOrWhiteSpace(combineRecords) && bool.TryParse(combineRecords, out bool canCombineRecords))
                    {
                        firehoseSink.CanCombineRecords = canCombineRecords;
                    }
                    return firehoseSink;
                case KINESIS_STREAM:
                    return new KinesisStreamSink(context, AWSUtilities.CreateAWSClient<AmazonKinesisClient>(context));
                case CLOUD_WATCH:
                    return new CloudWatchSink(60, context, AWSUtilities.CreateAWSClient<AmazonCloudWatchClient>(context));
                case TELEMETRICS:
#if DEBUG
                    const int TELEMETRICS_DEFAULT_INTERVAL = 60;
#else
                    const int TELEMETRICS_DEFAULT_INTERVAL = 300;
#endif
                    //If RedirectToSinkId is specified, we use TelemetryConnector. Otherwise, TelemtryClient
                    string redirectToSinkId = config[ConfigConstants.REDIRECT_TO_SINK_ID];
                    ITelemetricsClient<HttpResponseMessage> telemetricsClient = null;
                    if (string.IsNullOrWhiteSpace(redirectToSinkId))
                    {
                        telemetricsClient = TelemetricsClient.Default;
                    }
                    else
                    {
                        telemetricsClient = new TelemetricsSinkConnector(context);
                        context.ContextData[ConfigConstants.TELEMETRY_CONNECTOR] = telemetricsClient; //Make telemetricsClient available to caller
                    }
                    return new TelemetricsSink(TELEMETRICS_DEFAULT_INTERVAL, context, telemetricsClient);
                case FILE_SYSTEM:
                    return new FileSystemEventSink(context);
                default:
                    throw new NotImplementedException($"Sink type {sinkType} is not implemented by AWSEventSinkFactory.");
            }
        }

        public void RegisterFactory(IFactoryCatalog<IEventSink> catalog)
        {
            catalog.RegisterFactory(CLOUD_WATCH_LOG, this);
            catalog.RegisterFactory(CLOUD_WATCH_LOG_EMF, this);
            catalog.RegisterFactory(KINESIS_FIREHOSE, this);
            catalog.RegisterFactory(KINESIS_STREAM, this);
            catalog.RegisterFactory(CLOUD_WATCH, this);
            catalog.RegisterFactory(TELEMETRICS, this);
            catalog.RegisterFactory(FILE_SYSTEM, this);
        }
    }
}
