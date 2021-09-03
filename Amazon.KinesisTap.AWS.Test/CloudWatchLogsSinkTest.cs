using Amazon.CloudWatchLogs;
using Amazon.CloudWatchLogs.Model;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Amazon.KinesisTap.Test.Common;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Amazon.KinesisTap.AWS.Test
{
    public class CloudWatchLogsSinkTest
    {
        private readonly ITestOutputHelper _output;
        private readonly IMetrics _mockMetrics = new Mock<IMetrics>().Object;
        private readonly IBookmarkManager _mockBm = new Mock<IBookmarkManager>().Object;
        private readonly AWSBufferedSinkOptions _cloudWatchOptions = new AWSBufferedSinkOptions
        {
            BufferIntervalMs = 200,
            MaxBatchBytes = 1024 * 1024,
            MaxBatchSize = 500,
            QueueSizeItems = 500
        };

        public CloudWatchLogsSinkTest(ITestOutputHelper output)
        {
            _output = output;
            // Make sure that non-Windows system has the 'computername' env var set up
            Environment.SetEnvironmentVariable(ConfigConstants.COMPUTER_NAME, Utility.ComputerName);
        }

        [Fact]
        public async Task SendRecord_MetricIsPublished()
        {
            var records = new LogEnvelope<string>("msg", DateTime.UtcNow, "msg", null, 0, 1);
            var sent = new List<string>();
            var mock = new Mock<IAmazonCloudWatchLogs>();
            var metrics = new InMemoryMetricsSource();

            using var cts = new CancellationTokenSource();
            var sink = new AsyncCloudWatchLogsSink(nameof(LogGroupStreamVariable_ShouldResolve), null, "group", "logStream", mock.Object,
                NullLogger.Instance, metrics, _mockBm, new NetworkStatus(new AlwaysAvailableNetworkProvider()), _cloudWatchOptions);

            await sink.StartAsync(cts.Token);

            // 7 metrics after start-up
            Assert.Equal(7, metrics.Metrics.Count);

            // send a record
            sink.OnNext(records);
            await Task.Delay(1000);

            // 13 metrics after sending data
            Assert.Equal(13, metrics.Metrics.Count);

            cts.Cancel();
            await sink.StopAsync(default);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(10)]
        public async Task SendNormaRecords_ShouldCallPutLogEvents(int recordsCount)
        {
            var recordsSent = new List<InputLogEvent>();

            var mock = new Mock<IAmazonCloudWatchLogs>();
            mock.Setup(x => x.DescribeLogStreamsAsync(It.IsAny<DescribeLogStreamsRequest>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(new DescribeLogStreamsResponse
                {
                    LogStreams = new List<LogStream> { new LogStream { LogStreamName = "logStream", UploadSequenceToken = "123" } }
                }));

            mock.Setup(x => x.PutLogEventsAsync(It.IsAny<PutLogEventsRequest>(), It.IsAny<CancellationToken>()))
                .Returns(async (PutLogEventsRequest req, CancellationToken token) =>
                {
                    await Task.Yield();
                    recordsSent.AddRange(req.LogEvents);

                    return new PutLogEventsResponse { HttpStatusCode = HttpStatusCode.OK, NextSequenceToken = "123" };
                });

            var client = mock.Object;

            using var cts = new CancellationTokenSource();
            var sink = new AsyncCloudWatchLogsSink(nameof(LogGroupStreamVariable_ShouldResolve), null, "logGroup", "logStream", client,
                NullLogger.Instance, _mockMetrics, _mockBm, new NetworkStatus(new AlwaysAvailableNetworkProvider()), _cloudWatchOptions);
            await sink.StartAsync(cts.Token);

            for (var i = 0; i < recordsCount; i++)
            {
                sink.OnNext(new LogEnvelope<string>(i.ToString(), DateTime.UtcNow, i.ToString(), null, 0, 1));
            }

            await Task.Delay(1000);

            // assert that sent records equal to the list ["0","1","2"...]
            Assert.Equal(Enumerable.Range(0, recordsCount).Select(r => r.ToString()), recordsSent.Select(r => r.Message));

            cts.Cancel();
            await sink.StopAsync(default);
        }

        [Fact]
        public async Task SendRecord_ShouldIgnoreEmptyRecords()
        {
            var recordsSent = new List<InputLogEvent>();

            var mock = new Mock<IAmazonCloudWatchLogs>();
            mock.Setup(x => x.DescribeLogStreamsAsync(It.IsAny<DescribeLogStreamsRequest>(), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(new DescribeLogStreamsResponse
                {
                    LogStreams = new List<LogStream> { new LogStream { LogStreamName = "logStream", UploadSequenceToken = "123" } }
                }));

            mock.Setup(x => x.PutLogEventsAsync(It.IsAny<PutLogEventsRequest>(), It.IsAny<CancellationToken>()))
                .Returns(async (PutLogEventsRequest req, CancellationToken token) =>
                {
                    await Task.Yield();
                    recordsSent.AddRange(req.LogEvents);

                    return new PutLogEventsResponse { HttpStatusCode = HttpStatusCode.OK, NextSequenceToken = "123" };
                });

            var client = mock.Object;

            using var cts = new CancellationTokenSource();
            var sink = new AsyncCloudWatchLogsSink(nameof(LogGroupStreamVariable_ShouldResolve), null, "logGroup", "logStream", client,
                NullLogger.Instance, _mockMetrics, _mockBm, new NetworkStatus(new AlwaysAvailableNetworkProvider()), _cloudWatchOptions);
            await sink.StartAsync(cts.Token);

            // send one empty message
            sink.OnNext(new LogEnvelope<string>(string.Empty, DateTime.UtcNow, string.Empty, null, 0, 1));

            // send one message with data "1"
            sink.OnNext(new LogEnvelope<string>("1", DateTime.UtcNow, "1", null, 0, 1));

            // send one empty message
            sink.OnNext(new LogEnvelope<string>(string.Empty, DateTime.UtcNow, string.Empty, null, 0, 1));

            // send one null message
            sink.OnNext(new LogEnvelope<string>(null, DateTime.UtcNow, null, null, 0, 1));

            // send one message with data "2"
            sink.OnNext(new LogEnvelope<string>("2", DateTime.UtcNow, "2", null, 0, 1));

            // send one empty message
            sink.OnNext(new LogEnvelope<string>(string.Empty, DateTime.UtcNow, string.Empty, null, 0, 1));

            await Task.Delay(1000);

            // assert no empty records
            Assert.Equal(2, recordsSent.Count);
            Assert.DoesNotContain(recordsSent, r => string.IsNullOrEmpty(r.Message));

            cts.Cancel();
            await sink.StopAsync(default);
        }

        [Fact]
        public async Task LogGroupStreamVariable_ShouldResolve()
        {
            const string logGroupConfig = "{computername}-LogGroup";
            const string logStream = "{computername}";
            var expectedLogGroup = $"{Utility.ComputerName}-LogGroup";
            var expectedLogStream = Utility.ComputerName;
            const string msg = "testMsg";

            var records = new LogEnvelope<string>(msg, DateTime.UtcNow, msg, null, 0, 1);
            var sent = new List<string>();
            var mock = new Mock<IAmazonCloudWatchLogs>();

            mock.Setup(x => x.DescribeLogStreamsAsync(It.Is<DescribeLogStreamsRequest>(
                j => j.LogGroupName == expectedLogGroup && j.LogStreamNamePrefix == expectedLogStream), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(new DescribeLogStreamsResponse { LogStreams = new List<LogStream> { new LogStream { LogStreamName = expectedLogStream } } }));
            mock.Setup(x => x.PutLogEventsAsync(It.IsAny<PutLogEventsRequest>(), It.IsAny<CancellationToken>()))
                .Returns((PutLogEventsRequest req, CancellationToken token) =>
                {
                    if (req.LogStreamName == expectedLogStream && req.LogGroupName == expectedLogGroup)
                    {
                        sent.AddRange(req.LogEvents.Select(e => e.Message));
                    }
                    return Task.FromResult(new PutLogEventsResponse { HttpStatusCode = HttpStatusCode.OK, NextSequenceToken = "123" });
                });

            var client = mock.Object;

            using var cts = new CancellationTokenSource();
            var sink = new AsyncCloudWatchLogsSink(nameof(LogGroupStreamVariable_ShouldResolve), null, logGroupConfig, logStream, client,
                NullLogger.Instance, _mockMetrics, _mockBm, new NetworkStatus(new AlwaysAvailableNetworkProvider()), _cloudWatchOptions);
            await sink.StartAsync(cts.Token);
            sink.OnNext(records);
            await Task.Delay(1000);

            Assert.Single(sent);
            Assert.Equal(msg, sent[0]);

            cts.Cancel();
            await sink.StopAsync(default);
        }

        [Fact]
        public async Task LogGroupDoesNotExist_ShouldCreateGroup()
        {
            const string msg = "testMsg";
            const string logGroup = "NewLogGroup";
            var mock = new Mock<IAmazonCloudWatchLogs>();

            var sent = new List<string>();
            var logGroups = new List<string>();
            mock.Setup(x => x.DescribeLogStreamsAsync(It.Is<DescribeLogStreamsRequest>(
                    j => j.LogGroupName == logGroup), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ResourceNotFoundException("log group does not exist"));
            mock.Setup(x => x.CreateLogGroupAsync(It.IsAny<CreateLogGroupRequest>(), It.IsAny<CancellationToken>()))
                .Returns((CreateLogGroupRequest req, CancellationToken token) =>
                {
                    logGroups.Add(req.LogGroupName);
                    return Task.FromResult(new CreateLogGroupResponse { HttpStatusCode = HttpStatusCode.OK });
                });
            mock.Setup(x => x.PutLogEventsAsync(It.IsAny<PutLogEventsRequest>(), It.IsAny<CancellationToken>()))
                .Returns((PutLogEventsRequest req, CancellationToken token) =>
                {
                    if (req.LogGroupName == logGroup)
                    {
                        sent.AddRange(req.LogEvents.Select(e => e.Message));
                    }
                    return Task.FromResult(new PutLogEventsResponse { HttpStatusCode = HttpStatusCode.OK, NextSequenceToken = "123" });
                });

            using var cts = new CancellationTokenSource();
            var sink = new AsyncCloudWatchLogsSink(nameof(LogGroupStreamVariable_ShouldResolve), null, logGroup, "logStream", mock.Object,
                NullLogger.Instance, _mockMetrics, _mockBm, new NetworkStatus(new AlwaysAvailableNetworkProvider()), _cloudWatchOptions);
            var records = new LogEnvelope<string>(msg, DateTime.UtcNow, msg, null, 0, 1);
            await sink.StartAsync(cts.Token);

            sink.OnNext(records);
            await Task.Delay(1000);

            Assert.Equal(logGroup, logGroups.Single());
            Assert.Equal(msg, sent.Single());

            cts.Cancel();
            await sink.StopAsync(default);
        }

        [Fact]
        public async Task LogStreamDoesNotExist_ShouldCreateStream()
        {
            const string msg = "testMsg";
            const string logStream = "NewLogStream";
            var mock = new Mock<IAmazonCloudWatchLogs>();

            var sent = new List<string>();
            var logStreams = new List<string>();
            mock.Setup(x => x.DescribeLogStreamsAsync(It.Is<DescribeLogStreamsRequest>(
                    j => j.LogStreamNamePrefix == logStream), It.IsAny<CancellationToken>()))
                .Returns(Task.FromResult(new DescribeLogStreamsResponse { LogStreams = new List<LogStream>() }));
            mock.Setup(x => x.CreateLogStreamAsync(It.IsAny<CreateLogStreamRequest>(), It.IsAny<CancellationToken>()))
                .Returns((CreateLogStreamRequest req, CancellationToken token) =>
                {
                    logStreams.Add(req.LogStreamName);
                    return Task.FromResult(new CreateLogStreamResponse { HttpStatusCode = HttpStatusCode.OK });
                });
            mock.Setup(x => x.PutLogEventsAsync(It.IsAny<PutLogEventsRequest>(), It.IsAny<CancellationToken>()))
                .Returns((PutLogEventsRequest req, CancellationToken token) =>
                {
                    if (req.LogStreamName == logStream)
                    {
                        sent.AddRange(req.LogEvents.Select(e => e.Message));
                    }

                    return Task.FromResult(new PutLogEventsResponse { HttpStatusCode = HttpStatusCode.OK, NextSequenceToken = "123" });
                });

            using var cts = new CancellationTokenSource();
            var sink = new AsyncCloudWatchLogsSink(nameof(LogGroupStreamVariable_ShouldResolve), null, "logGroup", logStream, mock.Object,
                NullLogger.Instance, _mockMetrics, _mockBm, new NetworkStatus(new AlwaysAvailableNetworkProvider()), _cloudWatchOptions);
            var records = new LogEnvelope<string>(msg, DateTime.UtcNow, msg, null, 0, 1);
            await sink.StartAsync(cts.Token);

            sink.OnNext(records);
            await Task.Delay(1000);

            Assert.Equal(logStream, logStreams.Single());
            Assert.Equal(msg, sent.Single());

            cts.Cancel();
            await sink.StopAsync(default);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        public async Task InvalidSequenceToken_KeepRequestingTokens(int timesFailed)
        {
            const string msg = "testMsg";
            const string logStream = "logStream";
            const string correctSeqTok = "CorrectSeqTok";

            // set this so that the throttle doesn't take too long
            _cloudWatchOptions.MinRateAdjustmentFactor = 0.99;

            var failed = 0;
            var mock = new Mock<IAmazonCloudWatchLogs>();

            var sent = new List<string>();
            mock.Setup(x => x.DescribeLogStreamsAsync(It.IsAny<DescribeLogStreamsRequest>(), It.IsAny<CancellationToken>()))
                .Returns((DescribeLogStreamsRequest req, CancellationToken token) =>
                {
                    var nextToken = Interlocked.Increment(ref failed) <= timesFailed
                        ? Guid.NewGuid().ToString()
                        : correctSeqTok;
                    return Task.FromResult(new DescribeLogStreamsResponse
                    {
                        LogStreams = new List<LogStream>
                        {
                            new LogStream{LogStreamName = logStream, UploadSequenceToken = nextToken}
                        }
                    });
                });
            mock.Setup(x => x.PutLogEventsAsync(It.IsAny<PutLogEventsRequest>(), It.IsAny<CancellationToken>()))
                .Returns((PutLogEventsRequest req, CancellationToken token) =>
                {
                    if (req.SequenceToken != correctSeqTok)
                    {
                        throw new InvalidSequenceTokenException("invalid")
                        {
                            ExpectedSequenceToken = Interlocked.Increment(ref failed) <= timesFailed ? Guid.NewGuid().ToString() : correctSeqTok
                        };
                    }

                    sent.AddRange(req.LogEvents.Select(e => e.Message));
                    return Task.FromResult(new PutLogEventsResponse { HttpStatusCode = HttpStatusCode.OK, NextSequenceToken = correctSeqTok });
                });

            using var cts = new CancellationTokenSource();
            var sink = new AsyncCloudWatchLogsSink(nameof(LogGroupStreamVariable_ShouldResolve), null, "logGroup", logStream, mock.Object,
                NullLogger.Instance, _mockMetrics, _mockBm, new NetworkStatus(new AlwaysAvailableNetworkProvider()), _cloudWatchOptions);
            var records = new LogEnvelope<string>(msg, DateTime.UtcNow, msg, null, 0, 1);
            await sink.StartAsync(cts.Token);

            sink.OnNext(records);
            await Task.Delay(timesFailed * 500);

            Assert.Equal(msg, sent.Single());

            cts.Cancel();
            await sink.StopAsync(default);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        public async Task RateExceededExceptionWhenGettingSequenceToken_ShouldRecover(int timesFailed)
        {
            const string msg = "testMsg";
            const string logStream = "logStream";
            const string correctSeqTok = "CorrectSeqTok";

            // set this so that the throttle doesn't take too long
            _cloudWatchOptions.MinRateAdjustmentFactor = 0.99;
            var sent = new List<string>();
            var failed = 0;
            var mock = new Mock<IAmazonCloudWatchLogs>();
            mock.Setup(x => x.DescribeLogStreamsAsync(It.IsAny<DescribeLogStreamsRequest>(), It.IsAny<CancellationToken>()))
                .Returns((DescribeLogStreamsRequest req, CancellationToken token) =>
                {
                    if (Interlocked.Increment(ref failed) <= timesFailed)
                    {
                        throw new AmazonCloudWatchLogsException("Rate exceeded");
                    }

                    return Task.FromResult(new DescribeLogStreamsResponse
                    {
                        LogStreams = new List<LogStream>
                        {
                            new LogStream{LogStreamName = logStream, UploadSequenceToken = correctSeqTok}
                        }
                    });
                });
            mock.Setup(x => x.PutLogEventsAsync(It.IsAny<PutLogEventsRequest>(), It.IsAny<CancellationToken>()))
                .Returns((PutLogEventsRequest req, CancellationToken token) =>
                {
                    sent.AddRange(req.LogEvents.Select(e => e.Message));
                    return Task.FromResult(new PutLogEventsResponse { HttpStatusCode = HttpStatusCode.OK, NextSequenceToken = correctSeqTok });
                });

            using var cts = new CancellationTokenSource();
            var sink = new AsyncCloudWatchLogsSink(nameof(LogGroupStreamVariable_ShouldResolve), null, "logGroup", logStream, mock.Object,
                NullLogger.Instance, _mockMetrics, _mockBm, new NetworkStatus(new AlwaysAvailableNetworkProvider()), _cloudWatchOptions);
            var records = new LogEnvelope<string>(msg, DateTime.UtcNow, msg, null, 0, 1);
            await sink.StartAsync(cts.Token);

            sink.OnNext(records);
            await Task.Delay(timesFailed * 500);

            Assert.Equal(msg, sent.Single());

            cts.Cancel();
            await sink.StopAsync(default);
        }

        [Fact]
        public async Task BookmarkOnBufferFlush()
        {
            var sourceKey = $"{nameof(BookmarkOnBufferFlush)}-sourceKey";
            var sourceName = $"{nameof(BookmarkOnBufferFlush)}-sourceName";
            long expectedPosition = 128;
            long position = 0;
            var mockClient = new Mock<IAmazonCloudWatchLogs>();
            mockClient.Setup(x => x.DescribeLogStreamsAsync(It.IsAny<DescribeLogStreamsRequest>(), It.IsAny<CancellationToken>()))
                .Returns((DescribeLogStreamsRequest req, CancellationToken token) =>
                {
                    return Task.FromResult(new DescribeLogStreamsResponse
                    {
                        LogStreams = new List<LogStream>
                        {
                                        new LogStream{LogStreamName = "logStream", UploadSequenceToken = "12345"}
                        }
                    });
                });
            mockClient.Setup(i => i.PutLogEventsAsync(It.IsAny<PutLogEventsRequest>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(() =>
                {
                    return new PutLogEventsResponse { HttpStatusCode = HttpStatusCode.OK, NextSequenceToken = "12334" };
                });

            var mockBookmarkManager = new Mock<IBookmarkManager>();
            mockBookmarkManager
                .Setup(b => b.BookmarkCallback(It.IsAny<string>(), It.IsAny<IEnumerable<RecordBookmark>>()))
                .Returns((string key, IEnumerable<RecordBookmark> bookmarks) =>
                {
                    if (key == sourceKey && bookmarks.First() is IntegerPositionRecordBookmark integerBookmark)
                    {
                        Interlocked.Exchange(ref position, integerBookmark.Position);
                    }
                    return ValueTask.CompletedTask;
                });

            using var cts = new CancellationTokenSource();
            var sink = new AsyncCloudWatchLogsSink(nameof(LogGroupStreamVariable_ShouldResolve), null, "logGroup", "logStream", mockClient.Object,
                NullLogger.Instance, _mockMetrics, mockBookmarkManager.Object, new NetworkStatus(new AlwaysAvailableNetworkProvider()), _cloudWatchOptions);
            var record = new LogEnvelope<string>("msg", DateTime.UtcNow, "msg", null, expectedPosition, expectedPosition)
            {
                BookmarkData = new IntegerPositionRecordBookmark(sourceKey, sourceName, expectedPosition)
            };

            await sink.StartAsync(cts.Token);

            sink.OnNext(record);
            await Task.Delay(1000);

            Assert.Equal(expectedPosition, Interlocked.Read(ref position));

            cts.Cancel();
            await sink.StopAsync(default);
        }

        [Fact]
        public async Task ExpiredSessionToken_ShouldRetry()
        {
            var sent = 0;
            var mock = new Mock<IAmazonCloudWatchLogs>();
            mock.Setup(x => x.DescribeLogStreamsAsync(It.IsAny<DescribeLogStreamsRequest>(), It.IsAny<CancellationToken>()))
                .Returns((DescribeLogStreamsRequest req, CancellationToken token) =>
                {
                    return Task.FromResult(new DescribeLogStreamsResponse
                    {
                        LogStreams = new List<LogStream>
                        {
                            new LogStream{LogStreamName = "logStream", UploadSequenceToken = "12345"}
                        }
                    });
                });
            mock.SetupSequence(i => i.PutLogEventsAsync(It.IsAny<PutLogEventsRequest>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new AmazonCloudWatchLogsException(AWSConstants.SecurityTokenExpiredError))
                .ReturnsAsync(() =>
                {
                    Interlocked.Exchange(ref sent, 1);
                    return new PutLogEventsResponse { HttpStatusCode = HttpStatusCode.OK, NextSequenceToken = "12334" };
                });

            using var cts = new CancellationTokenSource();
            var sink = new AsyncCloudWatchLogsSink(nameof(LogGroupStreamVariable_ShouldResolve), null, "logGroup", "logStream", mock.Object,
                NullLogger.Instance, _mockMetrics, _mockBm, new NetworkStatus(new AlwaysAvailableNetworkProvider()), _cloudWatchOptions);
            var records = new LogEnvelope<string>("msg", DateTime.UtcNow, "msg", null, 0, 1);
            await sink.StartAsync(cts.Token);

            sink.OnNext(records);
            await Task.Delay(1000);
            Assert.Equal(1, sent);

            cts.Cancel();
            await sink.StopAsync(default);
        }
    }
}
