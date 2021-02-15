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
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Factory for DirectorySource and W3SVCSource
    /// </summary>
    public class DirectorySourceFactory : IFactory<ISource>
    {
        /// <summary>
        /// Create an instance o the DirectorySource
        /// </summary>
        /// <param name="entry">Name of the source</param>
        /// <param name="context">Plug-in Context</param>
        /// <returns></returns>
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
                    DateTimeKind timeZoneKind = Utility.ParseTimeZoneKind(config["TimeZoneKind"]);
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
                                new SingleLineRecordParser());
                        case "regex":
                            string pattern = config["Pattern"];
                            return CreateEventSource(context,
                                new RegexRecordParser(pattern,
                                    timetampFormat,
                                    logger,
                                    extractionPattern,
                                    extractionRegexOptions,
                                    timeZoneKind,
                                    new RegexRecordParserOptions { RemoveUnmatchedRecord = removeUnmatched }));
                        case "timestamp":
                            return CreateEventSource(context,
                                new TimeStampRecordParser(timetampFormat, logger, extractionPattern, extractionRegexOptions, timeZoneKind,
                                    new RegexRecordParserOptions { RemoveUnmatchedRecord = removeUnmatched }));
                        case "syslog":
                            return CreateEventSource(context,
                                new SyslogParser(logger, false));
                        case "delimited":
                            return CreateEventSourceWithDelimitedLogParser(context, timetampFormat, timeZoneKind);
                        case "singlelinejson":
                            return CreateEventSource(context,
                                new SingleLineJsonParser(timestampField, timetampFormat, logger));
                        default:
                            IFactoryCatalog<IRecordParser> parserFactories =
                                context?.ContextData?[PluginContext.PARSER_FACTORIES] as IFactoryCatalog<IRecordParser>;
                            var parserFactory = parserFactories.GetFactory(recordParser);
                            if (parserFactory == null)
                            {
                                throw new ArgumentException($"Unknown parser {recordParser}");
                            }
                            else
                            {
                                return CreateEventSource(context,
                                    parserFactory.CreateInstance(recordParser, context));
                            }
                    }
                case "w3svclogsource":
                    var defaultMapping = config[ConfigConstants.DEFAULT_FIELD_MAPPING];
                    return CreateEventSource(
                        context,
                        new W3SVCLogParser(context, defaultMapping));
                default:
                    throw new ArgumentException($"Source {entry} not recognized.");
            }
        }

        /// <summary>
        /// A generic version of the Factory method to create directory source
        /// </summary>
        /// <typeparam name="TData">Parser output type</typeparam>
        /// <typeparam name="TLogContext">Log context</typeparam>
        /// <param name="context">Plug-in Context</param>
        /// <param name="recordParser">Record Parser</param>
        /// <returns>Event Source</returns>
        public static IEventSource<TData> CreateEventSource<TData, TLogContext>(
            IPlugInContext context,
            IRecordParser<TData, TLogContext> recordParser
        ) where TLogContext : LogContext, new()
        {
            IConfiguration config = context.Configuration;
            GetDirectorySourceParameters(config, out string directory, out string filter, out int interval);
            DirectorySource<TData, TLogContext> source = new DirectorySource<TData, TLogContext>(
                directory,
                filter,
                interval * 1000, //milliseconds
                context,
                recordParser);
            source.NumberOfConsecutiveIOExceptionsToLogError = 3;

            EventSource<TData>.LoadCommonSourceConfig(config, source);

            source.Id = config[ConfigConstants.ID] ?? Guid.NewGuid().ToString();
            return source;
        }

        /// <summary>
        /// Register factories
        /// </summary>
        /// <param name="catalog">Source catalog</param>
        public virtual void RegisterFactory(IFactoryCatalog<ISource> catalog)
        {
            catalog.RegisterFactory("DirectorySource", this);
            catalog.RegisterFactory("W3SVCLogSource", this);
        }

        /// <summary>
        /// Create delimited parser
        /// </summary>
        /// <param name="context">Plug-in context</param>
        /// <param name="timestampFormat">Timestamp format</param>
        /// <param name="timeZoneKind">Timezone Kind</param>
        /// <returns>Delimited log parser</returns>
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

            var parser = new DelimitedLogParser(context, delimiter, recordFactoryMethod, headerPattern, recordPattern, commentPattern, headers, timeZoneKind);
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
            IRecordParser recordParser)
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
                recordParser);

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
                parser);
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
