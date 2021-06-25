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
using Amazon.EC2;
using Amazon.EC2.Model;
using Amazon.KinesisTap.Core;
using Amazon.Util;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Amazon.KinesisTap.AWS
{
    /// <summary>
    /// Utility to read and cache EC2 tags
    /// The EC2 instance must have permission to access to its own tags
    /// The tags are cached for 1 hour
    /// We will try 3 times to get the tags to avoid indefinitely retrying
    /// </summary>
    public class EC2Utility
    {
        private static readonly object _lockObject = new object();
        private static readonly TimeSpan _cacheTime = TimeSpan.FromHours(1.0); //Cache for 1 hour
        private static readonly IAmazonEC2 _ec2Client = new AmazonEC2Client(); //Require permission to access EC2 tags
        private static readonly string _instanceId = EC2InstanceMetadata.InstanceId;
        private static int _errCount = 0; //Keep trap of the error counts
        private const int ERR_LIMIT = 3; //Put a upper limit on retry
        private static DateTime _refreshDateTime; //Keep track of tags freshness
        private static IDictionary<string, string> _tags; //Cache tags

        public static string GetTagValue(string tag)
        {
            if (string.IsNullOrEmpty(_instanceId) || _errCount >= ERR_LIMIT)
            {
                return null; //short circuit as we have not chance of getting tags
            }

            if (_tags == null && (DateTime.Now - _refreshDateTime) > _cacheTime)
            {
                lock (_lockObject)
                {
                    GetTags();
                }
            }
            if (_tags != null && _tags.TryGetValue(tag, out string value))
            {
                return value;
            }
            else
            {
                return null;
            }
        }

        private static void GetTags()
        {
            try
            {
                _refreshDateTime = DateTime.Now;
                var response = _ec2Client.DescribeInstancesAsync(new DescribeInstancesRequest
                {
                    InstanceIds = new List<string> {
                    _instanceId
                }
                }).Result;
                _tags = response.Reservations[0].Instances[0].Tags.ToDictionary(t => t.Key, t => t.Value);
                _errCount = 0;
            }
            catch(Exception ex)
            {
                _errCount++;
                throw new Exception($"EC2Utility.GetTags: {ex.ToMinimized()}");
            }
        }
    }
}
