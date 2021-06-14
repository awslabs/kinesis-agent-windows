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
using Amazon.KinesisTap.Core;
using System;

namespace Amazon.KinesisTap.Hosting.Test
{
    public class MockFactory : IFactory<IEventSink>, IFactory<ISource>
    {
        ISource IFactory<ISource>.CreateInstance(string entry, IPlugInContext context)
        {
            switch (entry)
            {
                case "MockSource":
                    return new MockSource()
                    {
                        Id = context.Configuration["Id"]
                    };
                default:
                    throw new NotImplementedException($"Source type '{entry}' is not implemented by {nameof(MockFactory)}.");
            }
        }

        void IFactory<ISource>.RegisterFactory(IFactoryCatalog<ISource> catalog)
        {
            catalog.RegisterFactory("MockSource", this);
        }

        IEventSink IFactory<IEventSink>.CreateInstance(string entry, IPlugInContext context)
        {
            switch (entry)
            {
                case nameof(MockListSink):
                    return new MockListSink(context);
                case "RateExceeded":
                    return new MockSinkWithRateExceededException(int.Parse(context.Configuration["NumberOfExceptions"]))
                    {
                        Id = context.Configuration["Id"]
                    };
                default:
                    throw new NotImplementedException($"Sink type '{entry}' is not implemented by {nameof(MockFactory)}.");
            }
        }

        void IFactory<IEventSink>.RegisterFactory(IFactoryCatalog<IEventSink> catalog)
        {
            catalog.RegisterFactory(nameof(MockListSink), this);
            catalog.RegisterFactory("RateExceeded", this);
        }
    }
}
