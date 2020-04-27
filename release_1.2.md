# Amazon Kinesis Agent for Windows 1.1.212.1 release notes

## Updated all references to the AWS SDK's to use the latest versions

The old version of .Net AWS SDK makes calls to the STS global endpoint, which would cause certain policy violation for software making calls to the STS global endpoint. The latest SDK uses regional endpoint instead.



## DirectorySource: IncludeSubdirectories

IncludeSubdirectories will allow DirectorySource to monitor subdirectories to arbitrary depth limited by the OS. This feature is useful for monitoring web servers with multiple websites. Previously, we have to create multiple DirectorySources to monitor each directory. You can also use "IncludeDirectoryFilter" attribute to monitor only certain subdirectories specified in the filter.

Configuration example:

```
{
  "Sources": [
    {
      "Id": "KinesisTap",
      "SourceType": "DirectorySource",
      "Directory": "C:\\ProgramData\\Amazon\\KinesisTap\\logs",
      "FileNameFilter": "*",
      "IncludeSubdirectories": true,
      "IncludeDirectoryFilter": "cpu\cpu-1;cpu\cpu-2;load;memory",
      "TimeZoneKind": "UTC",
      "RecordParser": "Timestamp",
      "SkipLines": 0
    }
  ],
  "Sinks": [
    {
      "Id": "iCloudWatchMetricsSink",
      "SinkType": "CloudWatch",
      "Namespace": "CoreDevelopment"
    }
  ],
  "Pipes": [
    {
      "Id": "CollectdToCloudWatchMetrics",
      "SourceRef": "KinesisTap",
      "SinkRef": "iCloudWatchMetricsSink"
    }
  ],
  "SelfUpdate": 0
}
```


## BookmarkOnBufferFlush

Previously, bookmark is saved by source regularly without getting confirmation of upload, which would cause event lose during a system restart. "BookmarkOnBufferFlush" will make sure bookmark is updated only when a sink successfully ships an event off to AWS. However, you will only be able to subscribe a single sink to a source. If you are shipping logs to multiple destinations and guarantee no loss of events, you will need to duplicate your sources. This setting can be added in any bookmarkable source. It is feature-flagged by virtue of a setting in the source named "BookmarkOnBufferFlush", with a value of "true".

Configuration example:

```
{
  "Sources": [
    {
      "Id": "KinesisTap",
      "SourceType": "DirectorySource",
      "Directory": "C:\\ProgramData\\Amazon\\KinesisTap\\logs",
      "FileNameFilter": "*",
      "BookmarkOnBufferFlush": true,
      "TimeZoneKind": "UTC",
      "RecordParser": "Timestamp",
      "SkipLines": 0
    }
  ],
  "Sinks": [
    {
      "Id": "iCloudWatchMetricsSink",
      "SinkType": "CloudWatch",
      "Namespace": "CoreDevelopment"
    }
  ],
  "Pipes": [
    {
      "Id": "CollectdToCloudWatchMetrics",
      "SourceRef": "KinesisTap",
      "SinkRef": "iCloudWatchMetricsSink"
    }
  ],
  "SelfUpdate": 0
}
```


## Turn on Bookmark by default

Previously by default, KinesisTap only sends future events. Now we make “Bookmark” the default for the InitialPosition. In the bookmark mode, we save bookmark files for each source in the %ProgramData%\Amazon\KinesisTap directory. If you already have it configured, it will still use your configuration. 

To configure Bookmark, you can change the value of "InitialPosition":

```
{
  "Sources": [
    {
      "Id": "SyslogDirectorySource",
      "SourceType": "DirectorySource",
      "Directory": "C:\\LogSource\\",
      "FileNameFilter": "*.log",
      "RecordParser": "SysLog",
      "TimeZoneKind": "UTC",
      "RecordParser": "Timestamp",
      "InitialPosition": "EOS"
    }
  ],
  "Sinks": [
    {
      "Id": "KinesisStreamSink",
      "SinkType": "KinesisStream",
      "StreamName": "SyslogKinesisDataStream",
      "Region": "us-east-1"
    }
  ],
  "Pipes": [
    {
      "Id": "SyslogDS2KSSink",
      "SourceRef": "SyslogDirectorySource",
      "SinkRef": "KinesisStreamSink"
    }
  ]
}
```

## Delayed Start

KinesisTap will start shortly after all other services designated as Automatic have been started. This will resolve issues like KinesisTap starts before system Environment variables are set during a system reboot.


[firehose-pricing]: https://aws.amazon.com/kinesis/data-firehose/pricing/
[msdn-encoding]: https://docs.microsoft.com/en-us/dotnet/api/system.text.encoding?view=netframework-4.8&viewFallbackFrom=netframework-4.7.2.
[msdn-regex-options]: https://docs.microsoft.com/en-us/dotnet/api/system.text.regularexpressions.regexoptions?view=netframework-4.7.2
[ssm-on-prem]: https://docs.aws.amazon.com/systems-manager/latest/userguide/systems-manager-managedinstances.html
