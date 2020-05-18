# Amazon Kinesis Agent for Windows 1.1.215.1 (Beta) release notes
You can download Kinesis Agent for Windows [here](https://s3-us-west-2.amazonaws.com/kinesis-agent-windows/beta/index.html).
## New feature: Local FileSystem sink

The sink type `FileSystem` saves log and event records to a file on the local file system (instead of streaming them to AWS services). `FileSystem` sinks are useful for testing and diagnostic. For example, it can be used to examine the records before sending them to AWS. With `FileSystem` sinks, you can also simulate batching, throttling, and retry-on-error to mimic the behavior of actual AWS sinks through its configuration parameters.

The following snippet shows an example configuration for the sink:
```javascript
{
  "Id": "LocalFileSink", //Required
  "SinkType": "FileSystem", //Required. 
  "FilePath": "C:\\ProgramData\\Amazon\\local_sink.txt", //Optional: specify the file where records are saved. Default to <TempPath>\\<SinkId>.txt
  "Format": "json", //Optional: ""(default), "json" or "xml". If blank, the event is written to the file in plain text.
  "TextDecoration": "", //Optional. Used only if the event is written in plain text.
  "ObjectDecoration":"", //Optional. Used only if Format="json".
}
```

Note that all records from all sources connected to a `FileSystem` sink will be saved to a single file, specified by `FilePath`. If `FilePath` is not specified, the records will be saved to a file named <SinkId>.txt in the %TEMP% directory, which is usually at `C:\Users\<UserName>\AppData\Local\Temp`.

### Advanced usage: Record throttling & Failure simulation
`FileSystem` can mimic the behavior of "real" AWS sinks by simulating record throttling. The following optional attributes can be used to configure this behavior:
```javascript
{
  "RequestsPerSecond": "100", //Optional, default to 5.
  "BufferSize": "10", //Optional: Indicates the maximum number of records that the sink batches events before saving to file. Type is string.
  "MaxBatchSize": "1024", //Optional: Indicates the maximum amount of record data (in bytes) that the sink batches events before saving to file. Type is string.
}
```
Note that `RequestsPerSecond` controls the rate of requests that the sink processes (i.e. writes to file), not the number of records. Kinesis Agent for Windows make batch requests to AWS endpoints so a request may contain multiple records. The maximum number of records per request is controlled by the `BufferSize` attribute. So the record rate limit can be calculated as `RecordRate = BufferSize * RequestsPerSecond`. For example, the config above shows the maximum record rate of 1000 records per second.

You can also use `FileSystem` sinks to simulate and examine the behavior of AWS sinks when the network fails. This can be done by preventing the destination file from being written to (e.g. by acquiring a lock on the file).

## Support for resolving variables in more sink attributes

Kinesis agent for Windows currently supports using environment variables in several sink configuration attributes. In this build, we have expanded that support to include the `Region` and `RoleARN` attributes.
The following code snippet shows an example sink configuration that uses these two attributes:
```javascript
  "Id": "myCloudWatchLogsSink",
  "SinkType": "CloudWatchLogs",
  "LogGroup": "EC2Logs",
  "LogStream": "logs-{instance_id}" // use the EC2 instance ID varialbe
  "Region": "{env:Region}" // use the 'Region' environment variable
  "RoleARN": "{ec2tag:MyRoleARN}" // use the value of the 'MyRoleARN' EC2 tag
``` 

## Better response to Windows shutdown
In this release, we have made Kinesis Agent for Windows subscribe to the Windows shutdown event and get notified in advance. This means Kinesis Agent for Windows is now able to recognize a system shutdown early, and therefore has more time to flush the records in the sinks before stopping.

## Fixes & Improvements

* The release of the BookmarkOnBufferFlush feature in the last beta build (version 1.1.212.1) introduced a race condition where a bookmark file is being accessed by both the sink and the source at the same time when Kinesis Agent for Windows restarts (e.g. due to a change in configuration file). This causes the Windows Event Log source to be unable to save bookmarks. This bug has been fixed in this build. 
* Previously, the Windows Event Log source stores bookmark data to the file system every time a batch of logs are streamed to AWS. This causes high CPU utilization when Windows events are generated at a high rate. In this build, we have optimized the way Kinesis Agent for Windows stores bookmarks. When the agent is running, bookmark data is stored in-memory and flushed to the file system every 20 seconds (we are considering making this interval configurable in the upcoming builds). When the agent stops (e.g. due to a machine shutdown), any outstanding bookmark data is flushed to the file system immediately. This optimization makes the agent less CPU-consuming when streaming Windows Event Logs.
