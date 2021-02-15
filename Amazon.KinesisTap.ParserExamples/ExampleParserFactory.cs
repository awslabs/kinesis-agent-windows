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
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Amazon.KinesisTap.Core;
using Microsoft.Extensions.Logging.Abstractions;

namespace Amazon.KinesisTap.ParserExamples
{
    /// <summary>
    /// This an example for parser plug-in. We take an existing parser, but give it a name of SingleLineJson2.
    /// It can be used as a mock in unit or integrated testing
    /// To test, need to drop the assembly into the KinesisTap directory
    /// </summary>
    public class ExampleParserFactory : IFactory<IRecordParser>
    {
        public const string SINGLE_LINE_JSON2 = "singlelinejson2";
        public const string DELIMITED2 = "delimited2";

        /// <summary>
        /// Call by the infrastructure to register the name of the factory into the catalog
        /// </summary>
        /// <param name="catalog">Catalog</param>
        public void RegisterFactory(IFactoryCatalog<IRecordParser> catalog)
        {
            catalog.RegisterFactory(SINGLE_LINE_JSON2, this);
            catalog.RegisterFactory(DELIMITED2, this);
        }

        /// <summary>
        /// Factory method to create instance
        /// </summary>
        /// <param name="entry">Name of the factory</param>
        /// <param name="context">Context supplied by the infrastructure</param>
        /// <returns>Instance instantiated by the factory.</returns>
        public IRecordParser CreateInstance(string entry, IPlugInContext context)
        {
            IConfiguration config = context.Configuration;
            ILogger logger = context.Logger;
            string timetampFormat = config["TimestampFormat"];
            string timestampField = config["TimestampField"];

            switch (entry.ToLower())
            {
                case SINGLE_LINE_JSON2:
                    return new SingleLineJsonParser(timestampField, timetampFormat, NullLogger.Instance);
                case DELIMITED2:
                    DateTimeKind timeZoneKind = DateTimeKind.Utc; //Default
                    string timeZoneKindConfig = Utility.ProperCase(config["TimeZoneKind"]);
                    if (!string.IsNullOrWhiteSpace(timeZoneKindConfig))
                    {
                        timeZoneKind = (DateTimeKind)Enum.Parse(typeof(DateTimeKind), timeZoneKindConfig);
                    }
                    return DirectorySourceFactory.CreateDelimitedLogParser(context, timetampFormat, timeZoneKind);
                default:
                    throw new ArgumentException($"Parser {entry} not recognized.");
            }
        }
    }
}
