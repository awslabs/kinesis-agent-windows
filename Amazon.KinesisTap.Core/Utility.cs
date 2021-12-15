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
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using Amazon.KinesisTap.Shared;
using Microsoft.Extensions.Logging.Abstractions;
using System.Net.NetworkInformation;
using System.Threading.Tasks;
using System.Net.Sockets;

namespace Amazon.KinesisTap.Core
{
    public static class Utility
    {
        /// <summary>
        /// Product code name. Subject to change depending on distribution.
        /// </summary>
        public const string ProductCodeName = "AWSKinesisTap";

        /// <summary>
        /// Company name.
        /// </summary>
        public const string CompanyName = "Amazon";

        // Cache the OS platform information
        public static readonly bool IsWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
        public static readonly bool IsMacOs = RuntimeInformation.IsOSPlatform(OSPlatform.OSX);
        public static readonly bool IsLinux = RuntimeInformation.IsOSPlatform(OSPlatform.Linux);
        public static readonly string Platform = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Windows" :
            RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ? "Linux" :
            RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? "macOS" : "Unknown";
        public const string ExtraConfigDirectoryName = "configs";

        public static Func<string, string> ResolveEnvironmentVariable = Environment.GetEnvironmentVariable; //Can override this function for different OS

        private static string _computerName;
        private static string _hostName;
        private static string _uniqueClientID;
        private static string _uniqueSystemProperties;
        private static readonly long _ticksPerMillisecond = Stopwatch.Frequency / 1000;
        private static readonly long _initialTicks = Stopwatch.GetTimestamp();

        private static DateTime _baseNTPTime = new DateTime();
        private static Stopwatch _stopwatchForUniformTime = Stopwatch.StartNew();
        private static bool _isCorrectNTPTime = false;
        private static long _elapsedmsSinceFailure = -60000;
        private static string _currentLoggedInUser;
        public static IBashShell processor = new BashShell();

        private static readonly ThreadLocal<Random> _random = new(() => new Random(Guid.NewGuid().ToString().GetHashCode()));

        /// <summary>
        /// Get the amount of time passed since KinesisTap starts in milliseconds
        /// </summary>
        public static long GetElapsedMilliseconds() => (Stopwatch.GetTimestamp() - _initialTicks) / _ticksPerMillisecond;

        /// <summary>
        /// Gets or sets the UniformTime object to query for uniform time.
        /// </summary>
        public static IUniformServerTime UniformTime { get; set; }

        public static string AgentId { get; set; }

        public static string UserId { get; set; }

        public static string UniqueSystemProperties
        {
            get
            {
                return _uniqueSystemProperties;
            }
        }

        /// <summary>
        /// Get the currently logged-in user
        /// </summary>
        public static string CurrentLoggedInUser
        {
            get
            {
                try
                {
                    if (IsMacOs)
                    {
                        var command = $"stat -f %Su /dev/console";

                        var output = processor.RunCommand(command).Trim();
                        _currentLoggedInUser = output.ToString();

                        if (_currentLoggedInUser.ToString().Length == 0)
                        {
                            _currentLoggedInUser = GetCurrentLoggedInUserFromDirectory();
                        }

                        // If username is root then there is no current logged in user and system is at the login screen. In that case do not resolve the username.
                        if (_currentLoggedInUser.Equals("root"))
                        {
                            _currentLoggedInUser = $"{{{ConfigConstants.CURRENT_USER}}}";
                        }
                    }
                    else
                    {
                        throw new PlatformNotSupportedException("Operating system not supported");
                    }
                }
                catch { }
                return _currentLoggedInUser ?? $"{{{ConfigConstants.CURRENT_USER}}}";
            }
        }
        public static string ComputerName
        {
            get
            {
                if (_computerName == null)
                {
                    try
                    {
                        //On Linux, system does not create environment variable for nologin users so use Dns.GetHostName();
                        //Dns.GetHostName on Linux eventually call gethostname() system function but it first check if socket exists.
                        //In later version of .net, can just use Environment.MachineName
                        if (IsWindows)
                        {
                            _computerName = Environment.GetEnvironmentVariable("COMPUTERNAME");
                        }
                        else
                        {
                            string hostName = Dns.GetHostName();
                            int dotPos = hostName.IndexOf('.');
                            _computerName = dotPos == -1 ? hostName : hostName.Substring(0, dotPos);
                        }
                    }
                    catch { }
                }
                return _computerName ?? string.Empty;
            }
        }

        public static string HostName
        {
            get
            {
                if (_hostName == null)
                {
                    try
                    {
                        //On Linux, Dns.GetHostEntryAsync("LocalHost") will return "LocalHost"
                        _hostName = IsWindows ? Dns.GetHostEntryAsync("LocalHost").Result.HostName : Dns.GetHostName();
                    }
                    catch { }
                }
                return _hostName ?? "unresolved";
            }
        }

        /// <summary>
        /// Get the IP address info of the network interface used to connect to an endpoint.
        /// </summary>
        /// <param name="host">Remote host</param>
        /// <param name="port">Remote port</param>
        /// <param name="timeoutMs">Timeout in milliseconds</param>
        /// <param name="cancelToken">Cancellation token</param>
        /// <returns>List of <see cref="UnicastIPAddressInformation"/> used for connection</returns>
        /// <remarks>
        /// The returned list might contain both IPv4 and IPv6 info of the same address, in which case the
        /// caller can choose which form it requires.
        /// </remarks>
        /// <exception cref="SocketException">When the remote endpoint cannot be connected.</exception>
        public static async Task<IEnumerable<UnicastIPAddressInformation>> GetAddrInfoOfConnectionAsync(string host, int port,
            int timeoutMs = 5000,
            CancellationToken cancelToken = default)
        {
            //TODO right now we're using TCP connection because that's what most Internet endpoint are using.
            //howerver HTTP3 uses QUIC instead of TCP so this might need revisions in the future
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancelToken);
            cts.CancelAfter(timeoutMs);
            using var tcpClient = new TcpClient();

            try
            {
                await tcpClient.ConnectAsync(host, port, cts.Token);
                if (!tcpClient.Connected || tcpClient.Client.LocalEndPoint is not IPEndPoint localEndpoint)
                {
                    throw new SocketException();
                }

                var localAddress = localEndpoint.Address;
                var localAddressIPv4 = localAddress.AddressFamily == AddressFamily.InterNetwork
                    ? localAddress
                    : localAddress.IsIPv4MappedToIPv6 ? localAddress.MapToIPv4() : null;

                return NetworkInterface.GetAllNetworkInterfaces()
                     .Where(n => n.OperationalStatus == OperationalStatus.Up &&
                         n.NetworkInterfaceType != NetworkInterfaceType.Tunnel &&
                         n.NetworkInterfaceType != NetworkInterfaceType.Loopback)
                     .Where(n => n.GetIPProperties().UnicastAddresses.Any(
                         ip => ip.Address.AddressFamily == AddressFamily.InterNetwork || ip.Address.AddressFamily == AddressFamily.InterNetworkV6))
                     .SelectMany(ni => ni.GetIPProperties().UnicastAddresses)
                     .Where(unicastAddr =>
                     {
                         var addr = unicastAddr.Address;
                         switch (addr.AddressFamily)
                         {
                             case AddressFamily.InterNetwork:
                                 return addr.Equals(localAddressIPv4);
                             case AddressFamily.InterNetworkV6:
                                 if (addr.IsIPv4MappedToIPv6)
                                 {
                                     addr = addr.MapToIPv4();
                                 }
                                 return addr.Equals(localAddress);
                             default:
                                 return false;
                         }
                     });
            }
            catch (OperationCanceledException)
            {
                if (cancelToken.IsCancellationRequested)
                {
                    // the cancel signal comes from the stopToken parameter, we throw it here so the caller can catch it
                    cancelToken.ThrowIfCancellationRequested();
                }
                // throw a time-out 10060 exception
                throw new SocketException(10060);
            }
        }

        /// <summary>
        /// This will create a unique client id for a system based on the unique system properties such as MAC address, OS type and computername
        /// </summary>
        public static string UniqueClientID
        {
            get
            {
                if (_uniqueClientID == null)
                {
                    try
                    {
                        //Create a string based on system properties
                        var hashstring = GetSerialNumber() + " " + Platform + " " + GetSystemUUID();

                        //Generate a hash for the string
                        var inputBytes = Encoding.UTF8.GetBytes(hashstring);
                        using var hasher = new SHA256Managed();
                        var hashBytes = hasher.ComputeHash(inputBytes);
                        var hash = new StringBuilder();

                        foreach (var b in hashBytes)
                        {
                            hash.Append(string.Format("{0:x2}", b));
                        }

                        _uniqueClientID = hash.ToString();
                        _uniqueSystemProperties = hashstring;
                    }
                    catch { }
                }
                return _uniqueClientID ?? string.Empty;
            }
        }

        /// <summary>
        /// This function gets the current logged in user using directory search.
        /// </summary>
        /// <returns>The current logged in user</returns>
        private static string GetCurrentLoggedInUserFromDirectory()
        {
            var username = "";

            // If we are unable to get the current user from bash command, then get using directory list.
            // Exclude below directory when looking for user directory.
            var excludeDirectory = new List<string>(new string[] { "/users/admin", "/users/tokenadmin", "/users/Shared" });
            var directories = Directory.GetDirectories("/users");

            // If there are more than 1 user directory, get the one with the latest write time to get the user who has logged in recently.
            var userDirectories = new SortedList<DateTime, string>();
            foreach (var dir in directories)
            {
                if (!excludeDirectory.Contains(dir.ToString(), StringComparer.OrdinalIgnoreCase))
                {
                    userDirectories.Add(Directory.GetLastAccessTimeUtc(dir), dir);
                }
            }

            // Get the recently active user.
            if (userDirectories.Count > 0)
            {
                username = userDirectories.Last().Value;
            }

            return username;
        }
        /// <summary>
        /// The function gets the hardware Serial Number of the system.
        /// </summary>
        /// <returns>The hardware Serial Number of the system</returns>
        private static string GetSerialNumber()
        {
            if (OperatingSystem.IsWindows())
            {
                var wmiClass = "Win32_Bios";
                var wmiProperty = "SerialNumber";

                var wmiResult = new WmiDeviceIdComponent(wmiClass, wmiProperty);
                return wmiResult.GetValue();
            }
            else if (OperatingSystem.IsMacOS())
            {
                var command = $"ioreg -rd1 -c IOPlatformExpertDevice | awk '/IOPlatformSerialNumber/'";

                IBashShell processor = new BashShell();

                var output = processor.RunCommand(command).Trim();

                if (output.Contains("error", StringComparison.OrdinalIgnoreCase))
                {
                    return string.Empty;
                }

                return output.ToString();
            }
            else if (OperatingSystem.IsLinux())
            {
                var result = new FileDeviceIdComponent("/sys/class/dmi/id/board_serial");
                return result.GetValue();
            }

            return string.Empty;

        }

        /// <summary>
        /// The function gets the UUID of the system.
        /// </summary>
        /// <returns>The UUID of the system</returns>
        private static string GetSystemUUID()
        {
            if (OperatingSystem.IsWindows())
            {
                var wmiClass = "Win32_ComputerSystemProduct";
                var wmiProperty = "UUID";

                var wmiResult = new WmiDeviceIdComponent(wmiClass, wmiProperty);
                return wmiResult.GetValue();
            }
            else if (OperatingSystem.IsMacOS())
            {
                var command = $"ioreg -rd1 -c IOPlatformExpertDevice | awk '/IOPlatformUUID/'";

                IBashShell processor = new BashShell();

                var output = processor.RunCommand(command).Trim();

                if (output.Contains("error", StringComparison.OrdinalIgnoreCase))
                {
                    return string.Empty;
                }

                return output.ToString();
            }
            else if (OperatingSystem.IsLinux())
            {
                var result = new FileDeviceIdComponent("/sys/class/dmi/id/product_uuid");
                return result.GetValue();
            }
            return string.Empty;

        }

        public static readonly Regex VARIABLE_REGEX = new Regex("{[^}]+}");

        public static MemoryStream StringToStream(string str)
        {
            return StringToStream(str, null);
        }

        public static MemoryStream StringToStream(string str, string delimiter)
        {
            var memStream = new MemoryStream();
            var textWriter = new StreamWriter(memStream);
            textWriter.Write(str);
            if (!string.IsNullOrEmpty(delimiter))
            {
                textWriter.Write(delimiter);
            }
            textWriter.Flush();
            memStream.Seek(0, SeekOrigin.Begin);

            return memStream;
        }

        public static string ResolveVariables(string value, Func<string, string> evaluator)
        {
            return VARIABLE_REGEX.Replace(value, m => evaluator(m.Groups[0].Value));
        }

        public static string ResolveVariables(string value, IEnvelope envelope, Func<string, IEnvelope, object> evaluator)
        {
            return VARIABLE_REGEX.Replace(value, m => $"{evaluator(m.Groups[0].Value, envelope)}");
        }

        /// <summary>
        /// Resolve a variable. If the variable does not have a prefix, it tries to resolve to environment variable or return the variable itself if it cannot resolve.
        /// If the variable has a prefix, it will resolve the variable if the prefix is for environment variable or return the variable for the next step.
        /// </summary>
        /// <param name="variable">The name of the variable to resolve</param>
        /// <returns></returns>
        public static string ResolveVariable(string variable)
        {
            if (variable != null && variable.StartsWith("{"))
            {
                if (variable.EndsWith("}"))
                {
                    variable = variable.Substring(1, variable.Length - 2);
                }
                else
                {
                    throw new ArgumentException("variable must be in the format of \"{variable}\" or \"{prefix:variable}\".");
                }
            }

            if (string.IsNullOrWhiteSpace(variable))
            {
                throw new ArgumentException("Missing variable name.");
            }

            if (string.Equals("uniformtimestamp", variable, StringComparison.CurrentCultureIgnoreCase))
                return GetUniformTimeStamp().ToString("yyyy-MM-ddTHH:mm:ss.fff UTC");

            (string prefix, string variableNoPrefix) = SplitPrefix(variable, ':');
            if (!string.IsNullOrWhiteSpace(prefix) && !"env".Equals(prefix, StringComparison.CurrentCultureIgnoreCase))
            {
                //I don't know the prefix. Return the original form to let others resolve
                return $"{{{variable}}}";
            }

            string value = ResolveEnvironmentVariable(variableNoPrefix);
            if ("env".Equals(prefix, StringComparison.CurrentCultureIgnoreCase))
            {
                //User specifically asking for environment variable
                return value;
            }
            else
            {
                if (variableNoPrefix.Contains(ConfigConstants.CURRENT_USER))
                {
                    value = CurrentLoggedInUser;
                    return value;
                }
                //return the variable itself for the next step in the pipeline to resolve
                return string.IsNullOrWhiteSpace(value) ? $"{{{variable}}}" : value;
            }
        }

        /// <summary>
        /// This function will resolve the variable if it is a time stamp variable or return the variable itself for the next step in the pipeline to resolve
        /// </summary>
        /// <param name="variable">The name of the variable</param>
        /// <param name="timestamp">The timestamp to resolve to</param>
        /// <returns></returns>
        public static string ResolveTimestampVariable(string variable, DateTime timestamp)
        {
            if (!variable.StartsWith("{") || !variable.EndsWith("}"))
            {
                return variable;
            }

            (string prefix, string variableNoPrefix) = Utility.SplitPrefix(variable.Substring(1, variable.Length - 2), ':');
            if ("timestamp".Equals(prefix, StringComparison.CurrentCultureIgnoreCase))
            {
                return timestamp.ToString(variableNoPrefix);
            }
            else
            {
                return variable;
            }
        }

        /// <summary>
        /// This function will get the current uniform timestamp
        /// </summary>
        /// <returns>Uniform timestamp</returns>
        public static DateTime GetUniformTimeStamp()
        {
            // if a valid unexpired ntp server time is available then just calculate the time based on that. The time expires in 12 hrs.
            if (_baseNTPTime != DateTime.MinValue && _isCorrectNTPTime && _stopwatchForUniformTime.ElapsedMilliseconds < 43200000)
            {
                return _baseNTPTime.AddMilliseconds(_stopwatchForUniformTime.ElapsedMilliseconds);
            }

            DateTime baseNTPServerTime = DateTime.MinValue;
            bool useLastServerTime = false;

            var defaultProvider = new DefaultNetworkStatusProvider(NullLogger.Instance);
            defaultProvider.StartAsync(default).AsTask().GetAwaiter().GetResult();
            var networkstatus = new NetworkStatus(defaultProvider);

            // Query NTP server only if network is available
            if (networkstatus.IsAvailable())
            {
                if (GetElapsedMilliseconds() - _elapsedmsSinceFailure > 60000)
                {
                    if (UniformTime == null)
                        UniformTime = new UniformServerTime();

                    // Get NTP server time.
                    baseNTPServerTime = UniformTime.GetNTPServerTime();

                    if (baseNTPServerTime == DateTime.MinValue)
                    {
                        _elapsedmsSinceFailure = GetElapsedMilliseconds();
                    }
                }
            }

            if (baseNTPServerTime == DateTime.MinValue)
            {
                // Get time based on local system time in case of failure.
                if (_baseNTPTime == DateTime.MinValue || !_isCorrectNTPTime)
                {
                    baseNTPServerTime = DateTime.UtcNow;
                }
                else
                {
                    _isCorrectNTPTime = true;
                    useLastServerTime = true;
                }
            }
            else
            {
                _isCorrectNTPTime = true;
            }

            if (useLastServerTime == false)
            {
                _baseNTPTime = baseNTPServerTime;
                _stopwatchForUniformTime.Stop();
                _stopwatchForUniformTime = Stopwatch.StartNew();
            }
            else
            {
                baseNTPServerTime = _baseNTPTime.AddMilliseconds(_stopwatchForUniformTime.ElapsedMilliseconds);
            }

            return baseNTPServerTime;
        }

        /// <summary>
        /// This function splits a string based on input separator
        /// </summary>
        /// <param name="variable">The name of the variable</param>
        /// <param name="separator">The separator to split the string</param>
        /// <returns>prefix and suffix string</returns>
        public static (string prefix, string suffix) SplitPrefix(string variable, char separator)
        {
            int x = variable.IndexOf(separator);
            string prefix = null;
            if (x > -1)
            {
                prefix = variable.Substring(0, x);
                variable = variable.Substring(x + 1);
            }
            return (prefix, variable);
        }

        /// <summary>
        /// This function parses a CSV string
        /// </summary>
        /// <param name="input">The input string</param>
        /// <param name="stringBuilder">StringBuilder object to help parse the input string param</param>
        /// <returns>Array of string extracted from input param</returns>
        public static IEnumerable<string> ParseCSVLine(string input, StringBuilder stringBuilder)
        {
            const char columnSeparator = ',';
            if (string.IsNullOrEmpty(input))
            {
                yield break;
            }

            stringBuilder.Clear();

            int index = 0;
            int escapeCount = 0;

            for (; index < input.Length; index++)
            {
                if (input[index] == '"')
                {
                    escapeCount++;
                    stringBuilder.Append('"');
                }
                else if (input[index] == columnSeparator)
                {
                    if ((escapeCount % 2) == 0)
                    {
                        if (escapeCount == 0)
                        {
                            yield return stringBuilder
                                .ToString();
                        }
                        else
                        {
                            yield return stringBuilder
                                .Extract('"')
                                .Replace(@"""""", @"""");
                        }

                        stringBuilder.Clear();
                        escapeCount = 0;
                    }
                    else
                    {
                        stringBuilder.Append(columnSeparator);
                    }
                }
                else
                {
                    stringBuilder.Append(input[index]);
                }
            }

            if (escapeCount == 0)
            {
                yield return stringBuilder
                    .ToString();
            }
            else
            {
                yield return stringBuilder
                    .Extract('"')
                    .Replace(@"""""", @"""");
            }
        }

        /// <summary>
        /// Returns the ProgramData path, used to store bookmarks, logs, and update packages.
        /// </summary>
        public static string GetKinesisTapProgramDataPath()
        {
            var kinesisTapProgramDataPath = Environment.GetEnvironmentVariable(ConfigConstants.KINESISTAP_PROGRAM_DATA);
            if (string.IsNullOrWhiteSpace(kinesisTapProgramDataPath))
            {
                if (IsWindows)
                {
                    var commonAppDataFolder = Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData);
                    kinesisTapProgramDataPath = Path.Combine(commonAppDataFolder, CompanyName, ProductCodeName);
                }
                else
                {
                    kinesisTapProgramDataPath = ConfigConstants.UNIX_DEFAULT_PROGRAM_DATA_PATH;
                }
            }
            return kinesisTapProgramDataPath;
        }

        /// <summary>
        /// Gets the path to the session bookmark directory, relative to the AppData directory.
        /// </summary>
        /// <param name="sessionName">Session name, null for default session.</param>
        public static string GetBookmarkDirectory(string sessionName)
        {
            var bookmarksDir = ConfigConstants.BOOKMARKS;
            if (sessionName is not null)
            {
                bookmarksDir = Path.Combine(bookmarksDir, sessionName);
            }
            return bookmarksDir;
        }

        /// <summary>
        /// Gets the path to the session queue directory, relative to the AppData directory.
        /// </summary>
        /// <param name="sessionName">Session name, null for default session.</param>
        public static string GetSessionQueuesDirectoryRelativePath(string sessionName)
        {
            var path = ConfigConstants.QUEUE;
            if (sessionName is not null)
            {
                path = Path.Combine(path, sessionName);
            }
            return path;
        }

        /// <summary>
        /// Returns the path to the directory that stores the appsettings.json configuration file.
        /// </summary>
        public static string GetKinesisTapConfigPath()
        {
            var kinesisTapConfigPath = Environment.GetEnvironmentVariable(ConfigConstants.KINESISTAP_CONFIG_PATH);
            if (string.IsNullOrWhiteSpace(kinesisTapConfigPath))
            {
                if (IsWindows)
                {
                    // For windows, use the installation path
                    kinesisTapConfigPath = AppContext.BaseDirectory;
                }
                else
                {
#if DEBUG
                    kinesisTapConfigPath = AppContext.BaseDirectory;
#else
                    kinesisTapConfigPath = ConfigConstants.UNIX_DEFAULT_CONFIG_PATH;
#endif
                }
            }
            return kinesisTapConfigPath;
        }

        /// <summary>
        /// Returns the path to the directory that stores the NLog.xml file.
        /// </summary>
        public static string GetNLogConfigDirectory()
        {
            var nlogPath = Environment.GetEnvironmentVariable(ConfigConstants.KINESISTAP_NLOG_PATH);
            if (nlogPath is null)
            {
                // fall back to config path
                return GetKinesisTapConfigPath();
            }

            return nlogPath;
        }

        /// <summary>
        /// Returns the path to the directory that store the 'config' parameter store file.
        /// </summary>
        public static string GetProfileDirectory()
        {
            var confPath = Environment.GetEnvironmentVariable(ConfigConstants.KINESISTAP_PROFILE_PATH);
            if (confPath is null)
            {
                //fall back to config path
                return GetKinesisTapConfigPath();
            }

            return confPath;
        }

        /// <summary>
        /// Resolve the directory that contains the extra configuration files.
        /// </summary>
        public static string GetKinesisTapExtraConfigPath() => Path.Combine(GetKinesisTapConfigPath(), ExtraConfigDirectoryName);

        public static string ProperCase(string constant)
        {
            if (string.IsNullOrWhiteSpace(constant))
            {
                return constant;
            }
            else
            {
                return string.Join("", constant.Split(new char[] { '_' }, StringSplitOptions.RemoveEmptyEntries).Select(s => s[0].ToString().ToUpper() + s.Substring(1).ToLower()).ToArray());
            }
        }

        public static Random Random => _random.Value;

        public static T[] CloneArray<T>(T[] array)
        {
            T[] clone = new T[array.Length];
            array.CopyTo(clone, 0);
            return clone;
        }

        public static int ParseInteger(string value, int defaultValue)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return defaultValue;
            }
            else if (int.TryParse(value, out int result))
            {
                return result;
            }
            else
            {
                return defaultValue;
            }
        }

        public static DateTime? ToUniversalTime(DateTime? datetime)
        {
            if (datetime.HasValue)
            {
                return datetime.Value.ToUniversalTime();
            }
            else
            {
                return datetime;
            }
        }

        /// <summary>
        /// Parse the time zone kind from the configuration value.
        /// </summary>
        /// <param name="config"></param>
        /// <returns>UTC (default) or Local DateTimeKind</returns>
        public static DateTimeKind ParseTimeZoneKind(string config)
        {
            string timeZoneKindConfig = ProperCase(config);
            DateTimeKind timeZoneKind = DateTimeKind.Utc;
            if (!string.IsNullOrWhiteSpace(timeZoneKindConfig))
            {
                timeZoneKind = (DateTimeKind)Enum.Parse(typeof(DateTimeKind), timeZoneKindConfig);
            }
            return timeZoneKind;
        }

        /// <summary>
        /// Detect if a path expression is a wildcard expressions, containing ? or *
        /// </summary>
        /// <param name="nameOrPattern">Expression to check</param>
        /// <returns>true or false</returns>
        public static bool IsWildcardExpression(string nameOrPattern)
        {
            return (nameOrPattern.IndexOf("*") > -1) || (nameOrPattern.IndexOf("?") > -1);
        }

        /// <summary>
        /// Convert a wildcard expression to regular expression. 
        /// Match '?' to a single character and '*' to any single characters
        /// Escape all special characters
        /// </summary>
        /// <param name="pattern">The wildcard expression to convert</param>
        /// <returns>Regular expressions converted from wildcard expression</returns>
        public static string WildcardToRegex(string pattern, bool matchWholePhrase)
        {
            const string CHARS_TO_ESCAPE = ".+|{}()[]^$\\";
            string regex = string.Concat(pattern.Select(c => CHARS_TO_ESCAPE.IndexOf(c) > -1 ? "\\" + c : c.ToString()))
                .Replace("?", ".")
                .Replace("*", ".*");
            return matchWholePhrase ? "^" + regex + "$" : regex;
        }

        public static T ParseEnum<T>(string value)
        {
            return (T)Enum.Parse(typeof(T), value, true);
        }

        /// <summary>
        /// Extract fields from a string using regex named groups
        /// </summary>
        /// <param name="extractionRegex">Regex used for extracting fields</param>
        /// <param name="rawRecord">string</param>
        /// <returns>A dictionary of fields and values</returns>
        public static IDictionary<string, string> ExtractFields(Regex extractionRegex, string rawRecord)
        {
            IDictionary<string, string> fields = new Dictionary<string, string>();
            Match extractionMatch = extractionRegex.Match(rawRecord);
            if (extractionMatch.Success)
            {
                GroupCollection groups = extractionMatch.Groups;
                string[] groupNames = extractionRegex.GetGroupNames();
                foreach (string groupName in extractionRegex.GetGroupNames())
                {
                    if (!int.TryParse(groupName, out int n))
                    {
                        fields[groupName] = groups[groupName].Value;
                    }
                }
            }

            return fields;
        }

        /// <summary>
        /// Extend the DateTime.ParseExact to support additional formats such as epoch
        /// </summary>
        /// <param name="datetimeString">Value to be parsed.</param>
        /// <param name="format">DateTime format</param>
        /// <returns></returns>
        public static DateTime ParseDatetime(string datetimeString, string format)
        {
            if (format is null)
            {
                //format is unknown, we let DateTime try to figure out
                return DateTime.Parse(datetimeString, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal);
            }

            if (ConfigConstants.EPOCH.Equals(format, StringComparison.OrdinalIgnoreCase))
            {
                return FromEpochTime(long.Parse(datetimeString));
            }

            return DateTime.ParseExact(datetimeString, format, CultureInfo.InvariantCulture);
        }

        public static DateTime FromEpochTime(long epochTime) => DateTime.SpecifyKind(DateTimeOffset.FromUnixTimeMilliseconds(epochTime).DateTime, DateTimeKind.Utc);

        public static long ToEpochSeconds(DateTime utcTime) => new DateTimeOffset(utcTime).ToUnixTimeSeconds();

        public static long ToEpochMilliseconds(DateTime utcTime) => new DateTimeOffset(utcTime).ToUnixTimeMilliseconds();

        /// <summary>
        /// Strip quotes from a string if it is quoted
        /// </summary>
        /// <param name="value">string to strip</param>
        /// <returns>stripped string</returns>
        public static string StripQuotes(string value)
        {
            if (string.IsNullOrWhiteSpace(value)) return value;

            if (value.StartsWith("'") || value.StartsWith("\""))
            {
                return value.Substring(1, value.Length - 2);
            }

            return value;
        }

        /// <summary>
        /// Path of the process main module
        /// </summary>
        public static string MainModulePath
        {
            get { return Process.GetCurrentProcess().MainModule.FileName; }
        }

        /// <summary>
        /// Exchange the value of a <see cref="long"/> field a comparand number is greater than that field.
        /// This method is thread-safe.
        /// </summary>
        /// <param name="location">Reference to the field to exchange.</param>
        /// <param name="value">Value to exchange.</param>
        /// <param name="comparand">Comparand number</param>
        /// <returns>The value in <paramref name="location"/> before exchanging</returns>
        public static long InterlockedExchangeIfGreaterThan(ref long location, long value, long comparand)
        {
            long original;
            do
            {
                // first we store the original value. Note that by the time this assignment completes, the value at location might already change
                original = Interlocked.Read(ref location);
                if (comparand <= original)
                {
                    // if the condition is not satisfied, we return the original
                    return original;
                }
                // if the condition is met, we exchange the value if and only if the location value hasn't changed
                // if the location value has changed, we simply retry, hence the while loop
            }
            while (Interlocked.CompareExchange(ref location, value, original) != original);
            return original;
        }

        /// <summary>
        /// A helper method that will return the value of the defaultValue parameter if the value is null, empty, or whitespace.
        /// </summary>
        /// <param name="value">The string value that is expected to be not null or whitespace.</param>
        /// <param name="defaultValue">The default value to return if the value parameter is null or whitespace.</param>
        public static string ValueOrDefault(string value, string defaultValue)
        {
            if (!string.IsNullOrWhiteSpace(value)) return value;
            return defaultValue;
        }

        /// <summary>
        /// A helper method that will throw an exception if a config property is null or whitespace.
        /// </summary>
        /// <param name="config">The <see cref="IConfiguration"/> to retrieve the property from.</param>
        /// <param name="propertyName">The name of the property to retrieve.</param>
        public static string GetRequiredConfigValue(IConfiguration config, string propertyName)
        {
            if (!string.IsNullOrWhiteSpace(config[propertyName])) return config[propertyName];
            throw new ArgumentException($"Configuration property '{propertyName}' cannot be null or whitespace");
        }
    }

    internal static class StringBuilderExtensions
    {
        public static string Extract(this StringBuilder input, char character)
        {
            var startIndex = input.IndexOf(character);
            var lastIndex = input.LastIndexOf(character);

            var result = input.ToString(
                startIndex + 1,
                lastIndex - startIndex - 1);

            return result;
        }

        public static int LastIndexOf(this StringBuilder input, char character)
        {
            for (int i = input.Length - 1; i >= 0; i--)
            {
                if (input[i] == character)
                {
                    return i;
                }
            }

            return -1;
        }

        public static int IndexOf(this StringBuilder input, char character)
        {
            for (int i = 0; i < input.Length; i++)
            {
                if (input[i] == character)
                {
                    return i;
                }
            }

            return -1;
        }
    }

    public static class LinqExtensions
    {
        public static IEnumerable<IList<T>> Chunk<T>(this IEnumerable<T> source, int chunkSize)
        {
            return source
            .Select((x, i) => new { Index = i, Value = x })
            .GroupBy(x => x.Index / chunkSize)
            .Select(x => x.Select(v => v.Value).ToList());
        }
    }

    public static class DateTimeExtensions
    {
        public static DateTime Round(this DateTime date)
        {
            long ticks = (date.Ticks + (TimeSpan.TicksPerSecond / 2) + 1) / TimeSpan.TicksPerSecond;
            return new DateTime(ticks * TimeSpan.TicksPerSecond);
        }
    }
}
