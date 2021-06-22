# Amazon Kinesis Agent for Windows 1.2.3.3 (Prod) release notes
You can download Kinesis Agent for Windows [here](https://s3-us-west-2.amazonaws.com/kinesis-agent-windows/downloads/index.html).

## New feature:

### Self-contained application with new .Net Core 5.0 

This release uses a newer version of .Net core 5.0 that has more stability fixes and allows us to have common code and a self-contained application. With this feature, customers can install KinesisTap independently, without requiring pre-installed runtimes, eliminating any potential issues that may arise due to missing/mismatched assemblies.

### Unique Client ID support to correlate log records 

This release adds Unique Client ID support to log records that allows customers to correlate log records and uniquely identify endpoints. Customers can use {UNIQUECLIENTID} variable in KinesisTap configuration to decorate log records. By default, unique client id is added to Windows event logs and autorun security data.

### Uniform Timestamp to correlate events across multiple systems
KinesisTap now supports uniform timestamps so that events in the data can be correlated when data is received from multiple clients with differing time zones at backend. Customers can define {uniformtimestamp} variable in the log decorator to pass the uniform timestamp along with other data to the back-end.

### S3 Sink feature to upload service monitoring and troubleshooting logs

This feature enables KinesisTap customers to upload service monitoring and troubleshooting logs to S3 sink.

### High Performance Windows event log source

This build allows streaming high volumes of Windows events logs in JSON format with the use of “WindowsEventLogPollingSource”. With this new feature, KinesisTap processes events ~ 224 times faster than Sushi (average end-to-end delay: Sushi -19,095 secs, KinesisTap – 85 secs). KinesisTap now exposes a number of customizable parameters that can be further fine-tuned to increase throughput up to ~10.5 million events per hour assuming sufficient system resources are available.

## Stability and Performance fixes:

### Graceful shutdown

This build enables a signaling mechanism to ensure that all components stop their processing before the shutdown timeout is expired

### Fixes issues regarding W3SVCLogSource and ULSSource 

This build 1) Fixes the issue where Timestamp logged in UTC (TimestampUtc header), 2) Fixes an issue where ktdiag returns error when validating DirectorySource configuration with Delimited log parser without TimestampFormat attribute, and 3) Improves the sources' start-up performance when parsing large files.
