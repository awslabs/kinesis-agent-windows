# Amazon Kinesis Agent for Windows 1.1.216.4 release notes

## Fixed WindowsEventLogSource to RegexFilterPipe error

Previously, when you connect an WindowsEventLogSource to a RegexFilterPipe , KinesisTap would emit an error(@stacktrace_id 258 23911 -68280721) and the data would not go through. This was due to misinterpretation, the generic data type of the WindowsEventLogSource class so it did not match with RegexFilterPipe. This bug has been fixed in this build.



## Fixed partial JSON log records issue

Fixed an issue to allow KinesisTap to correctly handle partial JSON log records. To log big records, some applications need to write twice to the log file to finish writing a single log record. Previously, when handling that scenario, KinesisTap SingleLineJson record parser was not able to distinguish between a partial and a complete JSON record.


[firehose-pricing]: https://aws.amazon.com/kinesis/data-firehose/pricing/
[msdn-encoding]: https://docs.microsoft.com/en-us/dotnet/api/system.text.encoding?view=netframework-4.8&viewFallbackFrom=netframework-4.7.2.
[msdn-regex-options]: https://docs.microsoft.com/en-us/dotnet/api/system.text.regularexpressions.regexoptions?view=netframework-4.7.2
[ssm-on-prem]: https://docs.aws.amazon.com/systems-manager/latest/userguide/systems-manager-managedinstances.html
