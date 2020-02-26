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
namespace Amazon.KinesisTap.Core.EMF
{
    using System;

    /// <summary>
    /// An <see cref="Envelope{T}"/> implementation based on the <see cref="MetricScope"/> class.
    /// This class facilitates the transformation of the <see cref="MetricScope"/> objects returned
    /// by the <see cref="PowerShellExecutor"/> into the input format required by the sinks.
    /// </summary>
    public class MetricScopeEnvelope : Envelope<MetricScope>
    {
        public MetricScopeEnvelope(MetricScope data) : base(data)
        {
        }

        /// <inheritdoc />
        public override DateTime Timestamp => this._data.EventTimestamp;

        /// <inheritdoc />
        public override string GetMessage(string format)
        {
            return this._data.ToString();
        }

        /// <inheritdoc />
        public override object ResolveLocalVariable(string variable)
        {
            return this._data.DimensionValues.TryGetValue(variable, out string value) ? value : null;
        }

        /// <inheritdoc />
        public override object ResolveMetaVariable(string variable)
        {
            return this.ResolveLocalVariable(variable);
        }
    }
}
