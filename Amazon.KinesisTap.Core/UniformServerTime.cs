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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.Net.NetworkInformation;
using System.Threading;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Queries NTP server to get the uniform time from NTP server..
    /// </summary>
    public class UniformServerTime : IUniformServerTime
    {
        private DateTime GetBaseNTPServerTime(string ntpserver = "pool.ntp.org")
        {
            var retrycount = 0;
            DateTime servertime = DateTime.MinValue;

            while (retrycount < 3)
            {
                DateTime startTime = DateTime.Now.ToLocalTime();

                try
                {
                    // NTP message size
                    var ntpData = new byte[48];

                    //Setting the Leap Indicator, Version Number and Mode values
                    ntpData[0] = 0x1B; 

                    var addresses = Dns.GetHostEntry(ntpserver).AddressList;

                    //The UDP port number assigned to NTP is 123. 
                    var ipEndPoint = new IPEndPoint(addresses[0], 123);

                    var start = Utility.GetElapsedMilliseconds();

                    using (var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp))
                    {
                        socket.Connect(ipEndPoint);
                        socket.ReceiveTimeout = 3000;

                        socket.Send(ntpData);
                        socket.Receive(ntpData);
                        socket.Close();
                    }

                    // Calculate the network latency
                    var latency = start - Utility.GetElapsedMilliseconds();

                    //Offset to get to the "Transmit Timestamp" field (time at which the reply 
                    //departed the server for the client, in 64-bit timestamp format."
                    const byte serverReplyTime = 40;

                    //Get the seconds part
                    ulong intPart = BitConverter.ToUInt32(ntpData, serverReplyTime);

                    //Get the seconds fraction
                    ulong fractPart = BitConverter.ToUInt32(ntpData, serverReplyTime + 4);

                    intPart = (UInt32)IPAddress.NetworkToHostOrder((int)intPart);
                    fractPart = (UInt32)IPAddress.NetworkToHostOrder((int)fractPart);

                    var milliseconds = (intPart * 1000) + ((fractPart * 1000) / 0x100000000L);

                    //**UTC** time
                    var networkDateTime = (new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc)).AddMilliseconds((long)((long)milliseconds + (latency / 2)));
                    startTime = networkDateTime;// DateTime.Now;
                    servertime = startTime;

                }
                catch (Exception)
                {
                    servertime = DateTime.MinValue;
                }

                if (servertime == DateTime.MinValue)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                }
                else
                {
                    break;
                }
                retrycount++;
            }

            return servertime;
        }

        /// <summary>
        /// Queries NTP server to get uniform timestamp
        /// </summary>
        /// <returns>Uniform Timestamp</returns>
        public DateTime GetNTPServerTime()
        {
            DateTime baseNTPServerTime = DateTime.MinValue;
            baseNTPServerTime = GetBaseNTPServerTime();

            //If unable to get from general ntp server, try to get from amazon ntp server.
            if (baseNTPServerTime == DateTime.MinValue)
            {
                baseNTPServerTime = GetBaseNTPServerTime("0.amazon.pool.ntp.org");
            }

            return baseNTPServerTime;
        }
    }
}
