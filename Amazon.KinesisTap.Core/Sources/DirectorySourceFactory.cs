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
using System.IO;
using System.Linq;
using System.Reflection;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Core
{
    public class DirectorySourceFactory: IFactory<ISource>
    {
        public virtual ISource CreateInstance(string entry, IPlugInContext context)
        {
            IConfiguration config = context.Configuration;
            ILogger logger = context.Logger;

            switch (entry.ToLower())
            {
                case "directorysource":
                    string recordParser = (config["RecordParser"] ?? string.Empty).ToLower();
                    string timetampFormat = config["TimestampFormat"];
                    string timestampField = config["TimestampField"];
                    DateTimeKind timeZoneKind = DateTimeKind.Utc; //Default
                    string timeZoneKindConfig = Utility.ProperCase(config["TimeZoneKind"]);
                    if (!string.IsNullOrWhiteSpace(timeZoneKindConfig))
                    {
                        timeZoneKind = (DateTimeKind)Enum.Parse(typeof(DateTimeKind), timeZoneKindConfig);
                    }
                    string removeUnmatchedConfig = config["RemoveUnmatched"];
                    bool removeUnmatched = false;
                    if (!string.IsNullOrWhiteSpace(removeUnmatchedConfig))
                    {
                        removeUnmatched = bool.Parse(removeUnmatchedConfig);
                    }
                    string extractionPattern = config["ExtractionPattern"];
                    string extractionRegexOptions = config["ExtractionRegexOptions"];
                    switch (recordParser)
                    {
                        case "singleline":
                            return CreateEventSource(context,
                                new SingeLineRecordParser(),
                                CreateLogSourceInfo);
                        case "regex":
                            string pattern = config["Pattern"];
                            return CreateEventSource(context, 
                                new RegexRecordParser(pattern, 
                                    timetampFormat, 
                                    logger, 
                                    extractionPattern,
                                    extractionRegexOptions,
                                    timeZoneKind,
                                    new RegexRecordParserOptions { RemoveUnmatchedRecord = removeUnmatched }), 
                                CreateLogSourceInfo);
                        case "timestamp":
                            return CreateEventSource(context, 
                                new TimeStampRecordParser(timetampFormat, logger, extractionPattern, extractionRegexOptions, timeZoneKind,
                                    new RegexRecordParserOptions { RemoveUnmatchedRecord = removeUnmatched }), 
                                CreateLogSourceInfo);
                        case "syslog":
                            return CreateEventSource(context, 
                                new SysLogParser(logger, timeZoneKind, 
                                    new RegexRecordParserOptions { RemoveUnmatchedRecord = removeUnmatched}), 
                                CreateLogSourceInfo);
                        case "delimited":
                            return CreateEventSourceWithDelimitedLogParser(context, timetampFormat, timeZoneKind);
                        case "singlelinejson":
                            return CreateEventSource(context, 
                                new SingleLineJsonParser(timestampField, timetampFormat), 
                                CreateLogSourceInfo);
                        default:
                            IFactoryCatalog<IRecordParser> parserFactories = 
                                context?.ContextData?[PluginContext.PARSER_FACTORIES] as IFactoryCatalog<IRecordParser>;
                            var parser = parserFactories.GetFactory(recordParser);
                            if (parser == null)
                            {
                                throw new ArgumentException($"Unknown parser {recordParser}");
                            }
                            else
                            {
                                return CreateEventSource(context, 
                                    parser.CreateInstance(recordParser, context), CreateLogSourceInfo);
                            }
                    }
                case "w3svclogsource":
                    return CreateEventSource(
                        context,
                        new W3SVCLogParser(),
                        CreateDelimitedLogSourceInfo);
                default:
                    throw new ArgumentException($"Source {entry} not recognized.");
            }
        }

        public static IEventSource<TData> CreateEventSource<TData, TContext>(
            IPlugInContext context, 
            IRecordParser<TData, TContext> recordParser,
            Func<string, long, TContext> logSourceInfoFactory
        ) where TContext : LogContext
        {
            IConfiguration config = context.Configuration;
            GetDirectorySourceParameters(config, out string directory, out string filter, out int interval);
            DirectorySource<TData, TContext> source = new DirectorySource<TData, TContext>(
                directory,
                filter,
                interval * 1000, //milliseconds
                context,
                recordParser,
                logSourceInfoFactory);
            source.NumberOfConsecutiveIOExceptionsToLogError = 3;

            EventSource<TData>.LoadCommonSourceConfig(config, source);

            source.Id = config[ConfigConstants.ID] ?? Guid.NewGuid().ToString();
            return source;
        }

        public static LogContext CreateLogSourceInfo(string filePath, long position)
        {
            var context = new LogContext() { FilePath = filePath, Position = position };
            if (position > 0)
            {
                context.LineNumber = GetLineCount(filePath, position);
            }
            return context;
        }

        public static DelimitedLogContext CreateDelimitedLogSourceInfo(string filePath, long position)
        {
            var context = new DelimitedLogContext() { FilePath = filePath, Position = position };
            if (position > 0)
            {
                context.LineNumber = GetLineCount(filePath, position);
            }
            return context;
        }

        public virtual void RegisterFactory(IFactoryCatalog<ISource> catalog)
        {
            catalog.RegisterFactory("DirectorySource", this);
            catalog.RegisterFactory("W3SVCLogSource", this);
        }

        public static DelimitedLogParser CreateDelimitedLogParser(IPlugInContext context, string timestampFormat, DateTimeKind timeZoneKind)
        {
            IConfiguration config = context.Configuration;

            string delimiter = config["Delimiter"];
            string timestampField = config["TimestampField"];

            //Validate required attributes
            Guard.ArgumentNotNullOrEmpty(delimiter, "Delimiter is required for DelimitedLogParser");
            Guard.ArgumentNotNullOrEmpty(timestampFormat, "TimestampFormat is required for DelimitedLogParser");
            Guard.ArgumentNotNullOrEmpty(timestampField, "TimestampField is required for DelimitedLogParser");
            var delimitedLogTimestampExtractor = new TimestampExtrator(timestampField, timestampFormat);

            string commentPattern = config["CommentPattern"];
            string headerPattern = config["HeaderPattern"];
            string recordPattern = config["RecordPattern"];
            string headers = config["Headers"];

            DelimitedLogRecord recordFactoryMethod(string[] data, DelimitedLogContext logContext) =>
                new DelimitedLogRecord(data, logContext, delimitedLogTimestampExtractor.GetTimestamp);

            var parser = new DelimitedLogParser(delimiter, recordFactoryMethod, headerPattern, recordPattern, commentPattern, headers, timeZoneKind);
            return parser;
        }

        /// <summary>
        /// This is the non-generic version of the CreateEventSource relying on reflection to instantiate generic methods.
        /// </summary>
        /// <param name="context">Plug-in context</param>
        /// <param name="recordParser">Record Parser. Must implement IRecordParser<TData,LogContext></param>
        /// <param name="logSourceInfoFactory">Factory method for generating LogContext</param>
        /// <returns>Generated Directory Source</returns>
        internal static ISource CreateEventSource(IPlugInContext context,
            IRecordParser recordParser,
            Func<string, long, LogContext> logSourceInfoFactory)
        {
            Guard.ArgumentNotNull(recordParser, "recordParser");

            IConfiguration config = context.Configuration;
            GetDirectorySourceParameters(config, out string directory, out string filter, out int interval);

            var recordParserType = recordParser.GetType().GetTypeInfo().ImplementedInterfaces
                .FirstOrDefault(t => t.GetTypeInfo().IsGenericType && t.GetGenericTypeDefinition() == typeof(IRecordParser<,>));
            if (recordParserType == null) throw new ConfigurationException("recordParser must implement generic interface IRecordParser<,>");

            var directorySourceType = typeof(DirectorySource<,>);
            var genericDirectorySourceType = directorySourceType.MakeGenericType(recordParserType.GenericTypeArguments);
            var source = (ISource)Activator.CreateInstance(genericDirectorySourceType,
                directory,
                filter,
                interval * 1000, //milliseconds
                context,
                recordParser,
                logSourceInfoFactory);

            ((dynamic)source).NumberOfConsecutiveIOExceptionsToLogError = 3;

            //The following is the translation of EventSource<TData>.LoadCommonSourceConfig(config, source);
            typeof(EventSource<>)
                .MakeGenericType(recordParserType.GenericTypeArguments[0])
                .GetMethod("LoadCommonSourceConfig", BindingFlags.Static | BindingFlags.Public)
                .Invoke(null, new object[] { config, source });

            source.Id = config[ConfigConstants.ID] ?? Guid.NewGuid().ToString();
            return source;
        }

        private static ISource CreateEventSourceWithDelimitedLogParser(IPlugInContext context, string timestampFormat, DateTimeKind timeZoneKind)
        {
            DelimitedLogParser parser = CreateDelimitedLogParser(context, timestampFormat, timeZoneKind);

            return CreateEventSource(
                context,
                parser,
                CreateDelimitedLogSourceInfo);
        }

        private static long GetLineCount(string filePath, long position)
        {
            long lineCount = 0;
            using (var fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            using (var sr = new StreamReader(fs))
            {
                while(!sr.EndOfStream && sr.BaseStream.Position < position)
                {
                    sr.ReadLine();
                    lineCount++;
                }
            }
            return lineCount;
        }

        private static void GetDirectorySourceParameters(IConfiguration config, out string directory, out string filter, out int interval)
        {
            directory = config["Directory"];
            filter = config["FileNameFilter"];
            string intervalSetting = config["Interval"];
            interval = 0;
            if (!string.IsNullOrEmpty(intervalSetting))
            {
                int.TryParse(intervalSetting, out interval);
            }
            if (interval == 0)
            {
                interval = 1;
            }
        }
    }
}
