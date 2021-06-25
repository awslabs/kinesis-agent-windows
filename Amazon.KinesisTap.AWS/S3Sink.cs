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
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Core.Metrics;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.AWS
{
    public class S3Sink : AWSBufferedEventSink<string>, IDisposable
    {
        protected virtual IAmazonS3 S3Client { get; set; }
        protected readonly string _bucketName;
        protected readonly string _filePath;
        protected readonly string _fileName;
        protected readonly Throttle _throttle;

        private const long OneHundredSixtyGigabytes = 160000000000;

        private readonly TimeSpan _batchMaximumTimeSpan = TimeSpan.FromHours(24);

        /// <summary>
        /// Initializes a new instance of the <see cref="S3Sink"/> class.
        /// Note that the default batch interval is set to five minutes,
        /// the default number of allowed records is set to ten thousand,
        /// and the maximum batch size is 160 GB (the max size of a singe-file
        /// S3 upload.  These values can be configured at the agent level.
        /// </summary>
        /// <param name="context">The <see cref="IPlugInContext"/> that contains configuration info, logger, metrics etc.</param>
        public S3Sink(IPlugInContext context) : base(context, 5 * 60, 10000, OneHundredSixtyGigabytes)
        {
            _bucketName = ResolveVariables(_config[AWSConstants.BucketName]).ToLower();
            if (string.IsNullOrWhiteSpace(_bucketName) || _bucketName.Equals(AWSConstants.BucketName))
                throw new ArgumentException("'BucketName' setting in config file cannot be null, whitespace, or 'BucketName'");

            _filePath = ParseStaticConfig(ResolveVariables(_config[AWSConstants.FilePath]));
            _fileName = ParseStaticConfig(ResolveVariables(_config[AWSConstants.FileName]));

            if (string.IsNullOrWhiteSpace(_fileName) || _bucketName.Equals(AWSConstants.FileName))
                throw new ArgumentException("'FileName' setting in config file cannot be null, whitespace, or 'FileName'");

            _throttle = new AdaptiveThrottle(new TokenBucket(1, 5), _backoffFactor, _recoveryFactor, _minRateAdjustmentFactor);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="S3Sink"/> class.
        /// </summary>
        /// <param name="context">The <see cref="IPlugInContext"/> that contains configuration info, logger, metrics etc.</param>
        /// <param name="s3Client">The <see cref="IAmazonS3"/> that defines the Amazon S3 Client.</param>
        public S3Sink(IPlugInContext context, IAmazonS3 s3Client) : this(context)
        {
            S3Client = s3Client;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            S3Client.Dispose();
        }

        /// <summary>
        /// Initialize metrics for <see cref="S3Sink"/>.
        /// </summary>
        public override void Start()
        {
            _metrics?.InitializeCounters(Id, MetricsConstants.CATEGORY_SINK, CounterTypeEnum.Increment,
            new Dictionary<string, MetricValue>()
            {
                { MetricsConstants.S3_PREFIX + MetricsConstants.BYTES_ACCEPTED, new MetricValue(0, MetricUnit.Bytes) },
                { MetricsConstants.S3_PREFIX + MetricsConstants.RECORDS_ATTEMPTED, MetricValue.ZeroCount },
                { MetricsConstants.S3_PREFIX + MetricsConstants.RECORDS_FAILED_NONRECOVERABLE, MetricValue.ZeroCount },
                { MetricsConstants.S3_PREFIX + MetricsConstants.RECORDS_FAILED_RECOVERABLE, MetricValue.ZeroCount },
                { MetricsConstants.S3_PREFIX + MetricsConstants.RECORDS_SUCCESS, MetricValue.ZeroCount },
                { MetricsConstants.S3_PREFIX + MetricsConstants.RECOVERABLE_SERVICE_ERRORS, MetricValue.ZeroCount },
                { MetricsConstants.S3_PREFIX + MetricsConstants.NONRECOVERABLE_SERVICE_ERRORS, MetricValue.ZeroCount }
            });
            _logger?.LogInformation("S3Sink id {0} for bucket {1} started.", Id, _bucketName);
        }

        /// <summary>
        /// Stop the batch timer and flush its queue.
        /// </summary>
        public override void Stop()
        {
            base.Stop();
            _logger?.LogInformation("S3Sink id {0} for bucket {1} stopped.", Id, _bucketName);
        }

        /// <summary>
        /// Send the most recent batch of logs to the configured S3 bucket.
        /// </summary>
        /// <param name="records">List of envelope-wrapped logs.</param>
        public async Task SendBatchAsync(List<Envelope<string>> records)
        {
            try
            {
                _logger?.LogDebug("S3Sink id {0} sending {1} logs", Id, records.Count);

                // Create the PutObject request to send to S3
                var request = new PutObjectRequest
                {
                    BucketName = _bucketName,
                    Key = await GenerateS3ObjectKey(_filePath, _fileName)
                };

                // Turn the list of records into a stream that can be consumed by S3 and will register as a zipped file
                var memStream = new MemoryStream();
                using (var archive = new ZipArchive(memStream, ZipArchiveMode.Create, true))
                {
                    var logFile = archive.CreateEntry("AlarmLogs.txt", CompressionLevel.Optimal);
                    using (var entryStream = logFile.Open())
                    using (var compressedStream = new MemoryStream())
                    {
                        foreach (Envelope<string> record in records)
                        {
                            var byteArray = Encoding.UTF8.GetBytes(record.Data + "\n");
                            compressedStream.Write(byteArray, 0, byteArray.Length);
                        }
                        compressedStream.Seek(0, SeekOrigin.Begin);
                        compressedStream.CopyTo(entryStream);
                    }
                }
                var batchBytes = memStream.Length;
                request.InputStream = memStream;

                // Send the file to S3
                while (true)
                {
                    var utcNow = DateTime.UtcNow;

                    long elapsedMilliseconds = Utility.GetElapsedMilliseconds();
                    try
                    {
                        var response = await S3Client.PutObjectAsync(request);
                        _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                        _throttle.SetSuccess();
                        _recordsAttempted += records.Count;
                        _bytesAttempted += batchBytes;

                        _logger?.LogDebug("S3Sink id {0} successfully sent {1} logs, compressed to {2} bytes",
                            Id, records.Count, batchBytes);
                        _recordsSuccess += records.Count;
                        await SaveBookmarks(records);

                        break;
                    }
                    catch (AmazonS3Exception ex)
                    {
                        _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                        _throttle.SetError();

                        // Retry PutObjectRequest if possibe
                        if (ex.Retryable != null)
                        {
                            if (_buffer.Requeue(records, _throttle.ConsecutiveErrorCount < _maxAttempts))
                            {
                                _logger?.LogWarning("S3Sink id {0} attempt={1} exception={2}. Will retry.", Id, _throttle.ConsecutiveErrorCount, ex.Message);
                                _recoverableServiceErrors++;
                                _recordsFailedRecoverable += records.Count;
                                break;
                            }
                        }

                        _recordsFailedNonrecoverable += records.Count;
                        _nonrecoverableServiceErrors++;
                        throw;
                    }
                    catch (Exception)
                    {
                        _latency = Utility.GetElapsedMilliseconds() - elapsedMilliseconds;
                        _throttle.SetError();
                        _recordsFailedNonrecoverable += records.Count;
                        _nonrecoverableServiceErrors++;
                        throw;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError("S3Sink id {0} exception (attempt={1}): {2}", Id, _throttle.ConsecutiveErrorCount, ex.ToMinimized());
            }

            PublishMetrics(MetricsConstants.S3_PREFIX);
        }

        /// <summary>
        /// Calls the GetDelayMilliseconds method of <see cref="Throttle"/> for one token.
        /// </summary>
        /// <param name="recordCount">Number of records in batch.</param>
        /// <param name="batchBytes">Number of bytes in batch.</param>
        /// <returns>Number of milliseconds in the delay for one token.</returns>
        protected override long GetDelayMilliseconds(int recordCount, long batchBytes)
        {
            return _throttle.GetDelayMilliseconds(1);
        }

        /// <summary>
        /// If the buffer contains records, compile them into a batch and send them to S3.
        /// </summary>
        /// <param name="records">List of envelope-wrapped log records.</param>
        /// <param name="batchBytes">Number of bytes in batch of logs.</param>
        protected override async Task OnNextAsync(List<Envelope<string>> records, long batchBytes)
        {
            if (records == null || records.Count == 0)
            {
                return;
            }

            records.Sort((r1, r2) => r1.Timestamp.CompareTo(r2.Timestamp));
            var batch = new List<Envelope<string>>();
            var idx = 0;
            while (idx < records.Count)
            {
                var earliestTime = records[idx].Timestamp;
                batch.Clear();

                while (idx < records.Count && records[idx].Timestamp - earliestTime < _batchMaximumTimeSpan)
                {
                    batch.Add(records[idx]);
                    idx++;
                }

                await SendBatchAsync(batch);
            }
        }

        /// <summary>
        /// Creates a log record in the form of a string; useful for the <see cref="S3Sink"/>.
        /// </summary>
        /// <param name="record">Log record.</param>
        /// <param name="envelope">log record wrapper, contains record and timestamp.</param>
        /// <returns>String log record.</returns>
        protected override string CreateRecord(string record, IEnvelope envelope)
        {
            return record;
        }

        /// <summary>
        /// Gets the binary serializer for a string list.
        /// </summary>
        /// <returns>Binary serializer for a string list generated by <see cref="AWSSerializationUtility"/>.</returns>
        protected override ISerializer<List<Envelope<string>>> GetSerializer()
        {
            return AWSSerializationUtility.StringListBinarySerializer;
        }

        /// <summary>
        /// Get the size of a log record in bytes.
        /// </summary>
        /// <param name="record">Envelope-wrapped string record.</param>
        /// <returns>Size of log record in bytes.</returns>
        protected override long GetRecordSize(Envelope<string> record)
        {
            var recordSize = Encoding.UTF8.GetByteCount(record.Data + "\n");
            return recordSize;
        }

        /// <summary>
        /// Resolve all non-timestamp variables in config values like file name and file path.
        /// </summary>
        /// <param name="arg">Configuration value string.</param>
        /// <returns>Configuration value with variables resolved.</returns>
        protected virtual string ParseStaticConfig(string arg)
        {
            return Utility.ResolveVariables(arg, AWSUtilities.ResolveConfigVariable);
        }

        private async Task<string> GenerateS3ObjectKey(string filepath, string filename)
        {
            var matches = 0;
            var now = DateTime.UtcNow;
            var path = Utility.ResolveVariables(filepath, v => Utility.ResolveTimestampVariable(v, DateTime.UtcNow));
            var name = Utility.ResolveVariables(filename, v => Utility.ResolveTimestampVariable(v, DateTime.UtcNow));
            var key = path + name;

            ListObjectsRequest listRequest = new ListObjectsRequest
            {
                BucketName = _bucketName,
                Prefix = path,
                MaxKeys = 100
            };

            ListObjectsResponse listResponse;
            do
            {
                listResponse = await S3Client.ListObjectsAsync(listRequest);
                foreach (S3Object obj in listResponse.S3Objects)
                {
                    if (obj.Key.Contains(name))
                    {
                        matches++;
                    }
                }
            }
            while (listResponse.IsTruncated);

            if (matches > 0)
            {
                key = $"{key}({matches})";
            }

            return $"{key}.zip";
        }
    }
}
