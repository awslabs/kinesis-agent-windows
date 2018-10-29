using Amazon.KinesisTap.Core;
using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Amazon.KinesisFirehose;
using Amazon.Runtime;
using Amazon.Kinesis;
using Amazon.Runtime.CredentialManagement;
using Amazon.CloudWatchLogs;
using System.Runtime.InteropServices;
using Amazon.CloudWatch;
using Amazon.KinesisTap.AWS.Telemetrics;
using Amazon.CognitoIdentity;

namespace Amazon.KinesisTap.AWS
{
    public class AWSEventSinkFactory : IFactory<IEventSink>
    {
        private const string CLOUD_WATCH_LOG = "cloudwatchlogs";
        private const string CLOUD_WATCH = "cloudwatch";
        private const string KINESIS_FIREHOSE = "kinesisfirehose";
        private const string KINESIS_STREAM = "kinesisstream";
        private const string TELEMETRICS = "telemetrics";

        public IEventSink CreateInstance(string sinkType, IPlugInContext context)
        {
            IConfiguration config = context.Configuration;
            ILogger logger = context.Logger;

            switch (sinkType.ToLower())
            {
                case CLOUD_WATCH_LOG:
                    return new CloudWatchLogsSink(context, AWSUtilities.CreateAWSClient<AmazonCloudWatchLogsClient>(context));
                case KINESIS_FIREHOSE:
                    return new KinesisFirehoseSink(context, AWSUtilities.CreateAWSClient<AmazonKinesisFirehoseClient>(context));
                case KINESIS_STREAM:
                    return new KinesisStreamSink(context, AWSUtilities.CreateAWSClient<AmazonKinesisClient>(context));
                case CLOUD_WATCH:
                    return new CloudWatchSink(60, context, AWSUtilities.CreateAWSClient<AmazonCloudWatchClient>(context));
                case TELEMETRICS:
#if DEBUG
                    const int TELEMETRICS_DEFAULT_INTERVAL = 60;
#else
                    const int TELEMETRICS_DEFAULT_INTERVAL = 3600;
#endif
                    return new TelemetricsSink(TELEMETRICS_DEFAULT_INTERVAL, context,
                        TelemetricsClient.Default);
                default:
                    throw new NotImplementedException($"Sink type {sinkType} is not implemented by AWSEventSinkFactory.");
            }
        }

        public void RegisterFactory(IFactoryCatalog<IEventSink> catalog)
        {
            catalog.RegisterFactory(CLOUD_WATCH_LOG, this);
            catalog.RegisterFactory(KINESIS_FIREHOSE, this);
            catalog.RegisterFactory(KINESIS_STREAM, this);
            catalog.RegisterFactory(CLOUD_WATCH, this);
            catalog.RegisterFactory(TELEMETRICS, this);
        }
    }
}
