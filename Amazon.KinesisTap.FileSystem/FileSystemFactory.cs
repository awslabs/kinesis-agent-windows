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
using System.Globalization;
using System.Text;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Configuration;

namespace Amazon.KinesisTap.Filesystem
{
    public class FileSystemFactory : IFactory<ISource>
    {
        private const int DefaultBufferSize = 1024;
        private const string FileSystemSource = "DirectorySource";
        private const string W3SVCLogSource = "W3SVCLogSource";
        private const string ULSSource = "ULSSource";
        private const string ExchangeLogSource = "ExchangeLogSource";

        ISource IFactory<ISource>.CreateInstance(string entry, IPlugInContext context)
        {
            var options = new DelimitedLogParserOptions();
            ParseDelimitedParserOptions(context.Configuration, options);
            if (FileSystemSource.Equals(entry, StringComparison.OrdinalIgnoreCase))
            {
                return CreateFsSource(context);
            }
            else if (W3SVCLogSource.Equals(entry, StringComparison.OrdinalIgnoreCase))
            {
                return CreateDelimitedSource(context, new AsyncW3SVCLogParser(
                    context.Logger,
                    context.Configuration[ConfigConstants.DEFAULT_FIELD_MAPPING], options));
            }
            else if (ULSSource.Equals(entry, StringComparison.OrdinalIgnoreCase))
            {
                return CreateDelimitedSource(context, new AsyncULSLogParser(
                    context.Logger,
                    options.TextEncoding,
                    options.BufferSize));
            }
            else if (ExchangeLogSource.Equals(entry, StringComparison.OrdinalIgnoreCase))
            {
                return CreateDelimitedSource(context, new AsyncExchangeLogParser(
                    context.Logger,
                    context.Configuration["TimestampField"],
                    options.TextEncoding,
                    options.BufferSize));
            }

            throw new ArgumentOutOfRangeException(nameof(entry), "Unknown entry");
        }

        public static ISource CreateDelimitedSource<TData, TContext>(IPlugInContext context, ILogParser<TData, TContext> parser)
            where TContext : DelimitedTextLogContext, new()
        {
            var id = context.Configuration["Id"];
            var path = context.Configuration["Directory"];
            var options = ParseDirectorySourceOptions(context.Configuration);

            // Resolve any variables in the path.
            path = Utility.ResolveVariables(path, Utility.ResolveVariable);

            return new AsyncDirectorySource<TData, TContext>(id, path, parser, context.BookmarkManager, options, context);
        }

        void IFactory<ISource>.RegisterFactory(IFactoryCatalog<ISource> catalog)
        {
            catalog.RegisterFactory(FileSystemSource, this);
            catalog.RegisterFactory(ULSSource, this);
            catalog.RegisterFactory(W3SVCLogSource, this);
            catalog.RegisterFactory(ExchangeLogSource, this);
        }

        public static ISource CreateFsSource(IPlugInContext context)
        {
            var recordParser = context.Configuration["RecordParser"];
            var path = context.Configuration["Directory"];
            var id = context.Configuration["Id"];
            var skipLines = int.TryParse(context.Configuration["SkipLines"], out var sl) ? sl : 0;
            var options = ParseDirectorySourceOptions(context.Configuration);
            var timestampFormat = context.Configuration["TimestampFormat"];
            var timestampField = context.Configuration["TimestampField"];
            var parserBufferSize = int.TryParse(context.Configuration[ConfigConstants.BUFFER_SIZE], out var bufSize)
                ? bufSize
                : DefaultBufferSize;

            // Resolve any variables in the path.
            path = Utility.ResolveVariables(path, Utility.ResolveVariable);

            switch (recordParser.ToLower())
            {
                case "singleline":
                    var singleLineParser = new SingleLineLogParser(context.Logger, skipLines, options.PreferedEncoding, parserBufferSize);
                    return CreateDirectorySource(id, path, singleLineParser, options, context);
                case "regex":
                    var regexParser = new RegexLogParser(context.Logger,
                        context.Configuration["Pattern"],
                        ParseRegexOptions(context.Configuration),
                        options.PreferedEncoding,
                        parserBufferSize);
                    return CreateDirectorySource(id, path, regexParser, options, context);
                case "timestamp":
                    var timestampParser = new TimestampLogParser(context.Logger,
                        ParseRegexOptions(context.Configuration),
                        options.PreferedEncoding,
                        parserBufferSize);
                    return CreateDirectorySource(id, path, timestampParser, options, context);
                case "syslog":
                    var syslogParser = new SyslogLogParser(context.Logger, false, options.PreferedEncoding, parserBufferSize);
                    return CreateDirectorySource(id, path, syslogParser, options, context);
                case "singlelinejson":
                    var singleLineJsonParser = new SingleLineJsonTextParser(context.Logger,
                        timestampField, timestampFormat, options.PreferedEncoding);
                    return CreateDirectorySource(id, path, singleLineJsonParser, options, context);
                case "delimited":
                    return CreateGenericDelimitedSource(id, path, timestampFormat, timestampField, options, context);
                default:
                    throw new ArgumentOutOfRangeException("RecordParser");
            }
        }

        private static ISource CreateGenericDelimitedSource(string id, string path,
            string timestampFormat, string timestampField,
            DirectorySourceOptions options, IPlugInContext context)
        {
            var parserOptions = new GenericDelimitedLogParserOptions();
            ParseDelimitedParserOptions(context.Configuration, parserOptions);
            parserOptions.TimestampField = timestampField;
            parserOptions.TimestampFormat = timestampFormat;
            parserOptions.Headers = context.Configuration["Headers"];
            parserOptions.HeadersPattern = context.Configuration["HeaderPattern"];
            parserOptions.RecordPattern = context.Configuration["RecordPattern"];
            parserOptions.CommentPattern = context.Configuration["CommentPattern"];

            if (bool.TryParse(context.Configuration["CSVEscapeMode"], out var csvMode))
            {
                parserOptions.CSVEscapeMode = csvMode;
            }

            var parser = new GenericDelimitedLogParser(context.Logger, context.Configuration["Delimiter"], parserOptions);
            return CreateDirectorySource(id, path, parser, options, context);
        }

        private static ISource CreateDirectorySource<TData, TContext>(string id, string path,
            ILogParser<TData, TContext> recordParser, DirectorySourceOptions options, IPlugInContext context)
            where TContext : LogContext, new()
        {
            var directorySourceType = typeof(AsyncDirectorySource<,>);
            var genericDirectorySourceType = directorySourceType.MakeGenericType(typeof(TData), typeof(TContext));
            var source = (ISource)Activator.CreateInstance(genericDirectorySourceType,
                id,
                path,
                recordParser,
                context.BookmarkManager,
                options,
                context);

            return source;
        }

        private static RegexParserOptions ParseRegexOptions(IConfiguration config)
        {
            var options = new RegexParserOptions
            {
                ExtractionPattern = config["ExtractionPattern"],
                ExtractionRegexOptions = config["ExtractionRegexOptions"],
                TimeZoneKind = Utility.ParseTimeZoneKind(config["TimeZoneKind"]),
                TimestampFormat = config["TimestampFormat"]
            };
            if (bool.TryParse(config["RemoveUnmatched"], out var removeUnmatched) && removeUnmatched)
            {
                options.RemoveUnmatchedRecord = true;
            }
            return options;
        }

        public static void ParseDelimitedParserOptions(IConfiguration config, DelimitedLogParserOptions options)
        {
            options.TrimDataFields = bool.TryParse(config["TrimDataFields"], out var trimConfig) && trimConfig;
            options.TextEncoding = ParseEncoding(config);
            if (int.TryParse(config[ConfigConstants.BUFFER_SIZE], out var bufferSize))
            {
                options.BufferSize = bufferSize;
            }
        }

        public static DirectorySourceOptions ParseDirectorySourceOptions(IConfiguration config)
        {
            var filterSpec = config["FileNameFilter"];
            if (string.IsNullOrEmpty(filterSpec))
            {
                throw new ArgumentNullException("FileNameFilter");
            }

            var initialPosition = ParseInitialPosition(config["InitialPosition"]);
            var queryPeriodMs = 1000;
            if (int.TryParse(config["IntervalMs"], out var intervalMs))
            {
                queryPeriodMs = intervalMs;
            }
            else if (int.TryParse(config["Interval"], out var intervalSeconds))
            {
                queryPeriodMs = intervalSeconds * 1000;
            }

            return new DirectorySourceOptions
            {
                NameFilters = filterSpec.Split('|', StringSplitOptions.RemoveEmptyEntries),
                QueryPeriodMs = queryPeriodMs,
                InitialPosition = initialPosition,
                PreferedEncoding = ParseEncoding(config),
                InitialPositionTimestamp = ParseInitialPositionTimestamp(config),
                BookmarkOnBufferFlush = "true".Equals(config["BookmarkOnBufferFlush"], StringComparison.OrdinalIgnoreCase),
                IncludeSubdirectories = "true".Equals(config["IncludeSubdirectories"], StringComparison.OrdinalIgnoreCase),
                OmitLineNumber = "true".Equals(config["OmitLineNumber"], StringComparison.OrdinalIgnoreCase),
                IncludeDirectoryFilter = config["IncludeDirectoryFilter"] is null
                    ? null : config["IncludeDirectoryFilter"].Split(';', StringSplitOptions.RemoveEmptyEntries)
            };
        }

        private static DateTime ParseInitialPositionTimestamp(IConfiguration config)
        {
            var timeZoneKind = Utility.ParseTimeZoneKind(config["TimeZoneKind"]);
            var spec = config["InitialPositionTimestamp"];
            if (spec is null)
            {
                return DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Local);
            }

            return timeZoneKind == DateTimeKind.Utc
                ? DateTime.Parse(spec, null, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal)
                : DateTime.Parse(spec, null, DateTimeStyles.AssumeLocal);
        }

        private static Encoding ParseEncoding(IConfiguration config)
        {
            var encodingSpec = config["Encoding"];
            if (encodingSpec is null)
            {
                return null;
            }
            var encoding = Encoding.GetEncoding(encodingSpec);

            //make sure this is some encoding we support
            switch (encoding)
            {
                case UnicodeEncoding:
                case UTF8Encoding:
                case UTF32Encoding:
                case ASCIIEncoding:
                    return encoding;
                default:
                    throw new NotSupportedException($"Encoding '{encodingSpec}' is not supported");
            }
        }

        private static InitialPositionEnum ParseInitialPosition(string configValue)
        {
            if (configValue is null || configValue.Equals("Bookmark", StringComparison.OrdinalIgnoreCase))
            {
                return InitialPositionEnum.Bookmark;
            }

            if (int.TryParse(configValue, out var intValue) && intValue == 0)
            {
                return InitialPositionEnum.BOS;
            }

            if (configValue.Equals("EOS", StringComparison.OrdinalIgnoreCase))
            {
                return InitialPositionEnum.EOS;
            }

            if (configValue.Equals("Timestamp", StringComparison.OrdinalIgnoreCase))
            {
                return InitialPositionEnum.Timestamp;
            }

            throw new ArgumentOutOfRangeException("InitialPosition");
        }
    }
}
