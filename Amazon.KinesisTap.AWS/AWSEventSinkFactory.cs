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
using Amazon.CloudWatch;
using Amazon.CloudWatchLogs;
using Amazon.CognitoIdentity;
using Amazon.Kinesis;
using Amazon.KinesisFirehose;
using Amazon.KinesisTap.AWS.Failover;
using Amazon.KinesisTap.AWS.Failover.Strategy;
using Amazon.KinesisTap.AWS.Telemetrics;
using Amazon.KinesisTap.Core;
using Amazon.Runtime;
using Amazon.S3;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.AWS
{
    public class AWSEventSinkFactory : IFactory<IEventSink>
    {
#if DEBUG
        const int TELEMETRICS_DEFAULT_INTERVAL = 60;
#else
        const int TELEMETRICS_DEFAULT_INTERVAL = 3600;
#endif

        public const string CLOUD_WATCH_LOG_EMF = "cloudwatchlogsemf";
        public const string CLOUD_WATCH_LOG = "cloudwatchlogs";
        public const string CLOUD_WATCH = "cloudwatch";
        public const string KINESIS_FIREHOSE = "kinesisfirehose";
        public const string KINESIS_STREAM = "kinesisstream";
        public const string FILE_SYSTEM = "filesystem";
        public const string TELEMETRICS = "telemetrics";
        public const string S3 = "s3";

        // Failover Strategies
        public const string PriorityFailover = "PriorityFailover";
        public const string LoadBalanceFailover = "LoadBalanceFailover";
        public const string WeightedLoadBalanceFailover = "WeightedLoadBalanceFailover";
        public const string RoundTripTimeBasedFailover = "RoundTripTimeBasedFailover";

        // Priority region failover
        private const string CloudWatchLogsWithPriorityFailover
            = "cloudwatchlogswithpriorityfailover";
        private const string CloudWatchLogsEMFWithPriorityFailover
            = "cloudwatchlogsemfwithpriorityfailover";
        private const string CloudWatchWithPriorityFailover
            = "cloudwatchwithpriorityfailover";
        private const string KinesisFirehoseWithPriorityFailover
            = "kinesisfirehosewithpriorityfailover";
        private const string KinesisStreamWithPriorityFailover
            = "kinesisstreamwithpriorityfailover";

        // Load-balance region failover
        private const string CloudWatchLogsWithLoadBalanceFailover
            = "cloudwatchlogswithloadbalancefailover";
        private const string CloudWatchLogsEMFWithLoadBalanceFailover
            = "cloudwatchlogsemfwithloadbalancefailover";
        private const string CloudWatchWithLoadBalanceFailover
            = "cloudwatchwithloadbalancefailover";
        private const string KinesisFirehoseWithLoadBalanceFailover
            = "kinesisfirehosewithloadbalancefailover";
        private const string KinesisStreamWithLoadBalanceFailover
            = "kinesisstreamwithloadbalancefailover";

        // Weighted load-balance region failover
        private const string CloudWatchLogsWithWeightedLoadBalanceFailover
            = "cloudwatchlogswithweightedloadbalancefailover";
        private const string CloudWatchLogsEMFWithWeightedLoadBalanceFailover
            = "cloudwatchlogsemfwithweightedloadbalancefailover";
        private const string CloudWatchWithWeightedLoadBalanceFailover
            = "cloudwatchwithweightedloadbalancefailover";
        private const string KinesisFirehoseWithWeightedLoadBalanceFailover
            = "kinesisfirehosewithweightedloadbalancefailover";
        private const string KinesisStreamWithWeightedLoadBalanceFailover
            = "kinesisstreamwithweightedloadbalancefailover";

        // Round trip time based region failover
        private const string CloudWatchLogsWithRoundTripTimeBasedFailover
            = "cloudwatchlogswithroundtriptimebasedfailover";
        private const string CloudWatchLogsEMFWithRoundTripTimeBasedFailover
            = "cloudwatchlogsemfwithroundtriptimebasedfailover";
        private const string CloudWatchWithRoundTripTimeBasedFailover
            = "cloudwatchwithroundtriptimebasedfailover";
        private const string KinesisFirehoseWithRoundTripTimeBasedFailover
            = "kinesisfirehosewithroundtriptimebasedfailover";
        private const string KinesisStreamWithRoundTripTimeBasedFailover
            = "kinesisstreamwithroundtriptimebasedfailover";

        public IEventSink CreateInstance(string sinkType, IPlugInContext context)
        {
            IConfiguration config = context.Configuration;
            ILogger logger = context.Logger;
            var options = new AWSBufferedSinkOptions();
            ParseBufferedSinkOptions(config, options);
            var failoverEnabled = false;
            switch (sinkType.ToLower())
            {
                // Extending old sinks to support the functionality.
                case CLOUD_WATCH_LOG:
                case CLOUD_WATCH_LOG_EMF:
                    // Failover
                    if (bool.TryParse(config["FailoverEnabled"], out failoverEnabled) && failoverEnabled)
                    {
                        return (config["FailoverStrategy"]) switch
                        {
                            PriorityFailover => CreateInstance(CloudWatchLogsWithPriorityFailover, context),
                            WeightedLoadBalanceFailover => CreateInstance(CloudWatchLogsWithWeightedLoadBalanceFailover, context),
                            RoundTripTimeBasedFailover => CreateInstance(CloudWatchLogsWithRoundTripTimeBasedFailover, context),
                            _ => CreateInstance(CloudWatchLogsWithLoadBalanceFailover, context),
                        };
                    }
                    //override some options based on CloudWatchLogs quota
                    options.MaxBatchSize = 10000;
                    options.MaxBatchBytes = 1024 * 1000;
                    options.QueueSizeItems = 1000;

                    return new AsyncCloudWatchLogsSink(config[ConfigConstants.ID], context.SessionName,
                        config["LogGroup"], config["LogStream"],
                        AWSUtilities.CreateAWSClient<AmazonCloudWatchLogsClient>(context),
                        context.Logger, context.Metrics, context.BookmarkManager, context.NetworkStatus, options);

                //return new CloudWatchLogsSink(context, AWSUtilities.CreateAWSClient<AmazonCloudWatchLogsClient>(context));
                case CLOUD_WATCH:
                    // Failover
                    if (bool.TryParse(config["FailoverEnabled"], out failoverEnabled) && failoverEnabled)
                    {
                        return (config["FailoverStrategy"]) switch
                        {
                            PriorityFailover => CreateInstance(CloudWatchWithPriorityFailover, context),
                            WeightedLoadBalanceFailover => CreateInstance(CloudWatchWithWeightedLoadBalanceFailover, context),
                            RoundTripTimeBasedFailover => CreateInstance(CloudWatchWithRoundTripTimeBasedFailover, context),
                            _ => CreateInstance(CloudWatchWithLoadBalanceFailover, context),
                        };
                    }
                    return new CloudWatchSink(60, context, AWSUtilities.CreateAWSClient<AmazonCloudWatchClient>(context));
                case KINESIS_FIREHOSE:
                    // Failover
                    if (bool.TryParse(config["FailoverEnabled"], out failoverEnabled) && failoverEnabled)
                    {
                        return (config["FailoverStrategy"]) switch
                        {
                            PriorityFailover => CreateInstance(KinesisFirehoseWithPriorityFailover, context),
                            WeightedLoadBalanceFailover => CreateInstance(KinesisFirehoseWithWeightedLoadBalanceFailover, context),
                            RoundTripTimeBasedFailover => CreateInstance(KinesisFirehoseWithRoundTripTimeBasedFailover, context),
                            _ => CreateInstance(KinesisFirehoseWithLoadBalanceFailover, context),
                        };
                    }
                    return new KinesisFirehoseSink(context, AWSUtilities.CreateAWSClient<AmazonKinesisFirehoseClient>(context));
                case KINESIS_STREAM:
                    // Failover
                    if (bool.TryParse(config["FailoverEnabled"], out failoverEnabled) && failoverEnabled)
                    {
                        return (config["FailoverStrategy"]) switch
                        {
                            PriorityFailover => CreateInstance(KinesisStreamWithPriorityFailover, context),
                            WeightedLoadBalanceFailover => CreateInstance(KinesisStreamWithWeightedLoadBalanceFailover, context),
                            RoundTripTimeBasedFailover => CreateInstance(KinesisStreamWithRoundTripTimeBasedFailover, context),
                            _ => CreateInstance(KinesisStreamWithLoadBalanceFailover, context),
                        };
                    }
                    return new KinesisStreamSink(context, AWSUtilities.CreateAWSClient<AmazonKinesisClient>(context));

                case TELEMETRICS:
                    //If RedirectToSinkId is specified, we use TelemetryConnector. Otherwise, TelemtryClient
                    var redirectToSinkId = config[ConfigConstants.REDIRECT_TO_SINK_ID];
                    ITelemetricsClient telemetricsClient = null;
                    if (string.IsNullOrWhiteSpace(redirectToSinkId))
                    {
                        var cognitoIdentity = new AmazonCognitoIdentityClient(new AnonymousAWSCredentials(), RegionEndpoint.USWest2);
                        telemetricsClient = new TelemetricsClient(cognitoIdentity, context.ParameterStore);
                    }
                    else
                    {
                        telemetricsClient = new TelemetricsSinkConnector(context);
                        context.ContextData[ConfigConstants.TELEMETRY_CONNECTOR] = telemetricsClient; //Make telemetricsClient available to caller
                    }
                    return new TelemetricsSink($"_{TELEMETRICS}", TELEMETRICS_DEFAULT_INTERVAL * 1000, telemetricsClient, context.Logger);
                case FILE_SYSTEM:
                    return new FileSystemEventSink(context);
                case S3:
                    return new S3Sink(context, AWSUtilities.CreateAWSClient<AmazonS3Client>(context));

                // Exposed directly as well
                // DIRECT - Priority region failover
                case CloudWatchLogsWithPriorityFailover:
                case CloudWatchLogsEMFWithPriorityFailover:
                    {
                        var failoverSinkRegionStrategy = new PriorityRegionFailover<AmazonCloudWatchLogsClient>(
                            context, ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_FIRST_RETRY_IN_MINUTES * 60 * 1000,
                            AWSUtilities.CreateAWSClient<AmazonCloudWatchLogsClient>);
                        return new CloudWatchLogsSink(
                            context, new FailoverSink<AmazonCloudWatchLogsClient>(context, failoverSinkRegionStrategy), failoverSinkRegionStrategy);
                    }
                case CloudWatchWithPriorityFailover:
                    {
                        var failoverSinkRegionStrategy = new PriorityRegionFailover<AmazonCloudWatchClient>(
                            context, ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_FIRST_RETRY_IN_MINUTES * 60 * 1000,
                            AWSUtilities.CreateAWSClient<AmazonCloudWatchClient>);
                        return new CloudWatchSink
                            (60, context, new FailoverSink<AmazonCloudWatchClient>(context, failoverSinkRegionStrategy), failoverSinkRegionStrategy);
                    }
                case KinesisFirehoseWithPriorityFailover:
                    {
                        var failoverSinkRegionStrategy = new PriorityRegionFailover<AmazonKinesisFirehoseClient>(
                            context, ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_FIRST_RETRY_IN_MINUTES * 60 * 1000,
                            AWSUtilities.CreateAWSClient<AmazonKinesisFirehoseClient>);
                        return new KinesisFirehoseSink(
                            context, new FailoverSink<AmazonKinesisFirehoseClient>(context, failoverSinkRegionStrategy), failoverSinkRegionStrategy);
                    }
                case KinesisStreamWithPriorityFailover:
                    {
                        var failoverSinkRegionStrategy = new PriorityRegionFailover<AmazonKinesisClient>(
                            context, ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_FIRST_RETRY_IN_MINUTES * 60 * 1000,
                            AWSUtilities.CreateAWSClient<AmazonKinesisClient>);
                        return new KinesisStreamSink(
                            context, new FailoverSink<AmazonKinesisClient>(context, failoverSinkRegionStrategy), failoverSinkRegionStrategy);
                    }

                // DIRECT - Load-balance region failover
                case CloudWatchLogsWithLoadBalanceFailover:
                case CloudWatchLogsEMFWithLoadBalanceFailover:
                    {
                        var failoverSinkRegionStrategy = new LoadBalanceRegionFailover<AmazonCloudWatchLogsClient>(
                            context, ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_FIRST_RETRY_IN_MINUTES * 60 * 1000,
                            AWSUtilities.CreateAWSClient<AmazonCloudWatchLogsClient>);
                        return new CloudWatchLogsSink(
                            context, new FailoverSink<AmazonCloudWatchLogsClient>(context, failoverSinkRegionStrategy), failoverSinkRegionStrategy);
                    }
                case CloudWatchWithLoadBalanceFailover:
                    {
                        var failoverSinkRegionStrategy = new LoadBalanceRegionFailover<AmazonCloudWatchClient>(
                            context, ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_FIRST_RETRY_IN_MINUTES * 60 * 1000,
                            AWSUtilities.CreateAWSClient<AmazonCloudWatchClient>);
                        return new CloudWatchSink
                            (60, context, new FailoverSink<AmazonCloudWatchClient>(context, failoverSinkRegionStrategy), failoverSinkRegionStrategy);
                    }
                case KinesisFirehoseWithLoadBalanceFailover:
                    {
                        var failoverSinkRegionStrategy = new LoadBalanceRegionFailover<AmazonKinesisFirehoseClient>(
                            context, ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_FIRST_RETRY_IN_MINUTES * 60 * 1000,
                            AWSUtilities.CreateAWSClient<AmazonKinesisFirehoseClient>);
                        return new KinesisFirehoseSink(
                            context, new FailoverSink<AmazonKinesisFirehoseClient>(context, failoverSinkRegionStrategy), failoverSinkRegionStrategy);
                    }
                case KinesisStreamWithLoadBalanceFailover:
                    {
                        var failoverSinkRegionStrategy = new LoadBalanceRegionFailover<AmazonKinesisClient>(
                            context, ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_FIRST_RETRY_IN_MINUTES * 60 * 1000,
                            AWSUtilities.CreateAWSClient<AmazonKinesisClient>);
                        return new KinesisStreamSink(
                            context, new FailoverSink<AmazonKinesisClient>(context, failoverSinkRegionStrategy), failoverSinkRegionStrategy);
                    }

                // DIRECT - Weighted load-balance region failover
                case CloudWatchLogsWithWeightedLoadBalanceFailover:
                case CloudWatchLogsEMFWithWeightedLoadBalanceFailover:
                    {
                        var failoverSinkRegionStrategy = new WeightedLoadBalanceRegionFailover<AmazonCloudWatchLogsClient>(
                            context, ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_FIRST_RETRY_IN_MINUTES * 60 * 1000,
                            AWSUtilities.CreateAWSClient<AmazonCloudWatchLogsClient>);
                        return new CloudWatchLogsSink(
                            context, new FailoverSink<AmazonCloudWatchLogsClient>(context, failoverSinkRegionStrategy), failoverSinkRegionStrategy);
                    }
                case CloudWatchWithWeightedLoadBalanceFailover:
                    {
                        var failoverSinkRegionStrategy = new WeightedLoadBalanceRegionFailover<AmazonCloudWatchClient>(
                            context, ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_FIRST_RETRY_IN_MINUTES * 60 * 1000,
                            AWSUtilities.CreateAWSClient<AmazonCloudWatchClient>);
                        return new CloudWatchSink
                            (60, context, new FailoverSink<AmazonCloudWatchClient>(context, failoverSinkRegionStrategy), failoverSinkRegionStrategy);
                    }
                case KinesisFirehoseWithWeightedLoadBalanceFailover:
                    {
                        var failoverSinkRegionStrategy = new WeightedLoadBalanceRegionFailover<AmazonKinesisFirehoseClient>(
                            context, ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_FIRST_RETRY_IN_MINUTES * 60 * 1000,
                            AWSUtilities.CreateAWSClient<AmazonKinesisFirehoseClient>);
                        return new KinesisFirehoseSink(
                            context, new FailoverSink<AmazonKinesisFirehoseClient>(context, failoverSinkRegionStrategy), failoverSinkRegionStrategy);
                    }
                case KinesisStreamWithWeightedLoadBalanceFailover:
                    {
                        var failoverSinkRegionStrategy = new WeightedLoadBalanceRegionFailover<AmazonKinesisClient>(
                            context, ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_FIRST_RETRY_IN_MINUTES * 60 * 1000,
                            AWSUtilities.CreateAWSClient<AmazonKinesisClient>);
                        return new KinesisStreamSink(
                            context, new FailoverSink<AmazonKinesisClient>(context, failoverSinkRegionStrategy), failoverSinkRegionStrategy);
                    }

                // DIRECT - Round trip time based region failover
                case CloudWatchLogsWithRoundTripTimeBasedFailover:
                case CloudWatchLogsEMFWithRoundTripTimeBasedFailover:
                    {
                        var failoverSinkRegionStrategy = new RoundTripTimeBasedRegionFailover<AmazonCloudWatchLogsClient>(
                            context, ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_FIRST_RETRY_IN_MINUTES * 60 * 1000,
                            AWSUtilities.CreateAWSClient<AmazonCloudWatchLogsClient>, CloudWatchLogsSink.CheckServiceReachable);
                        return new CloudWatchLogsSink(
                            context, new FailoverSink<AmazonCloudWatchLogsClient>(context, failoverSinkRegionStrategy), failoverSinkRegionStrategy);
                    }
                case CloudWatchWithRoundTripTimeBasedFailover:
                    {
                        var failoverSinkRegionStrategy = new RoundTripTimeBasedRegionFailover<AmazonCloudWatchClient>(
                            context, ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_FIRST_RETRY_IN_MINUTES * 60 * 1000,
                            AWSUtilities.CreateAWSClient<AmazonCloudWatchClient>, CloudWatchSink.CheckServiceReachable);
                        return new CloudWatchSink
                            (60, context, new FailoverSink<AmazonCloudWatchClient>(context, failoverSinkRegionStrategy), failoverSinkRegionStrategy);
                    }
                case KinesisFirehoseWithRoundTripTimeBasedFailover:
                    {
                        var failoverSinkRegionStrategy = new RoundTripTimeBasedRegionFailover<AmazonKinesisFirehoseClient>(
                            context, ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_FIRST_RETRY_IN_MINUTES * 60 * 1000,
                            AWSUtilities.CreateAWSClient<AmazonKinesisFirehoseClient>, KinesisFirehoseSink.CheckServiceReachable);
                        return new KinesisFirehoseSink(
                            context, new FailoverSink<AmazonKinesisFirehoseClient>(context, failoverSinkRegionStrategy), failoverSinkRegionStrategy);
                    }
                case KinesisStreamWithRoundTripTimeBasedFailover:
                    {
                        var failoverSinkRegionStrategy = new RoundTripTimeBasedRegionFailover<AmazonKinesisClient>(
                            context, ConfigConstants.DEFAULT_MIN_WAIT_BEFORE_REGION_FAILBACK_FIRST_RETRY_IN_MINUTES * 60 * 1000,
                            AWSUtilities.CreateAWSClient<AmazonKinesisClient>, KinesisStreamSink.CheckServiceReachable);
                        return new KinesisStreamSink(
                            context, new FailoverSink<AmazonKinesisClient>(context, failoverSinkRegionStrategy), failoverSinkRegionStrategy);
                    }

                default:
                    throw new NotImplementedException($"Sink type {sinkType} is not implemented by AWSEventSinkFactory.");
            }
        }

        public void ParseBufferedSinkOptions(IConfiguration config, AWSBufferedSinkOptions options)
        {
            options.TextDecoration = config[ConfigConstants.TEXT_DECORATION];
            options.TextDecorationEx = config[ConfigConstants.TEXT_DECORATION_EX];
            options.ObjectDecoration = config[ConfigConstants.OBJECT_DECORATION];
            options.ObjectDecorationEx = config[ConfigConstants.OBJECT_DECORATION_EX];
            options.Format = config[ConfigConstants.FORMAT];
            options.SecondaryQueueType = config[ConfigConstants.QUEUE_TYPE];

            if (int.TryParse(config[ConfigConstants.BUFFER_INTERVAL], out var bufferInterval))
            {
                options.BufferIntervalMs = bufferInterval * 1000;
            }

            if (int.TryParse(config[ConfigConstants.BUFFER_INTERVAL_MS], out var bufferIntervalMs))
            {
                options.BufferIntervalMs = bufferIntervalMs;
            }

            // this is problematic but BufferSize actually refers to max batch size
            if (int.TryParse(config[ConfigConstants.BUFFER_SIZE], out var bufferSize))
            {
                options.MaxBatchSize = bufferSize;
            }

            // we'll make BufferSizeItems the setting for actual number of items in queue
            if (int.TryParse(config[ConfigConstants.BUFFER_SIZE_ITEMS], out var bufferSizeItems))
            {
                options.QueueSizeItems = bufferSizeItems;
            }

            if (int.TryParse(config["MaxAttempts"], out var maxAttempts))
            {
                options.MaxAttempts = maxAttempts;
            }

            if (double.TryParse(config["JittingFactor"], out var jittingFactor))
            {
                options.JittingFactor = jittingFactor;
            }

            if (double.TryParse(config["BackoffFactor"], out var backoffFactor))
            {
                options.BackoffFactor = backoffFactor;
            }

            if (double.TryParse(config["RecoveryFactor"], out var recoveryFactor))
            {
                options.RecoveryFactor = recoveryFactor;
            }

            if (double.TryParse(config["MinRateAdjustmentFactor"], out var minRateAdjustmentFactor))
            {
                options.MinRateAdjustmentFactor = minRateAdjustmentFactor;
            }

            if (int.TryParse(config[ConfigConstants.UPLOAD_NETWORK_PRIORITY], out var uploadPriority))
            {
                options.UploadNetworkPriority = uploadPriority;
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
            catalog.RegisterFactory(S3, this);
        }
    }
}
