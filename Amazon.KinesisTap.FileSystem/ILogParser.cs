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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.KinesisTap.Core;
using System;

namespace Amazon.KinesisTap.Filesystem
{
    /// <summary>
    /// Interface for log parsers
    /// </summary>
    /// <remarks>
    /// As a general guideline, implementations of this class should be state-less i.e. it should not contains
    /// changing information about the log files. Any such meta data should be set in the <typeparamref name="TContext"/> parameter.
    /// </remarks>
    public interface ILogParser<TData, TContext> : IRecordParser where TContext : LogContext
    {
        /// <summary>
        /// Parse the log file for records.
        /// </summary>
        /// <param name="context">Log file's context</param>
        /// <param name="output">Where parsed records are appended</param>
        /// <param name="recordCount">Maximum number of records to be parsed</param>
        /// <param name="stopToken">Token to stop the operation</param>
        /// <remarks>
        /// Regarding the cancellation behavior of this method: when <paramref name="stopToken"/> throws <see cref="OperationCanceledException"/>,
        /// records might still have been added to <paramref name="output"/> and metadata in <paramref name="context"/> might have been changed.
        /// It is up to the user to decide how to deal with this updated information, for example, a source can simple ignore it and not use it to save bookmarks.
        /// </remarks>
        Task ParseRecordsAsync(TContext context, IList<IEnvelope<TData>> output, int recordCount, CancellationToken stopToken = default);
    }
}
