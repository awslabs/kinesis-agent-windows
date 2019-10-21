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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Text.RegularExpressions;

using Microsoft.Deployment.WindowsInstaller;
using Microsoft.Win32;

namespace KinesisTapMsiCustomAction
{
    public class CustomActions
    {
        /// <summary>
        /// Install appsettings.json if not existing. Do not override the existing.
        /// </summary>
        /// <param name="session"></param>
        /// <returns></returns>
        [CustomAction]
        public static ActionResult InstallAppSettings(Session session)
        {
            session.Log("Begin installing appsettings.json");
            //Need to run InstallAppSettings.SetProperty custom action first to set the value
            string installLocation = session.CustomActionData["INSTALLFOLDER"];
            string appSettingsPath = Path.Combine(installLocation, "appsettings.json");
            if (!File.Exists(appSettingsPath))
            {
                session.Log("appsettings.json not found. Copy from appsettingsTemplate.json.");
                string appSettingsTemplatePath = Path.Combine(installLocation, "appsettingsTemplate.json");
                try
                {
                    File.Copy(appSettingsTemplatePath, appSettingsPath);
                }
                catch(Exception ex)
                {
                    session.Log($"Failed to install appsettings.json: {ex.Message}");
                    return ActionResult.Failure;
                }
            }
            session.Log("Finish installing appsettings.json");

            return ActionResult.Success;
        }

        /// <summary>
        /// Install or merge AWSKinesisTap.exe.config
        /// If AWSKinesisTap.exe.config does not exist, copy from AWSKinesisTap.exe.config.new
        /// If AWSKinesisTap.exe.config already exists, merge policies from AWSKinesisTap.exe.config.new
        /// </summary>
        /// <param name="session"></param>
        /// <returns></returns>
        [CustomAction]
        public static ActionResult UpdateKinesisTapExeConfig(Session session)
        {
            session.Log("Begin installing AWSKinesisTap.exe.config");
            string installLocation = session.CustomActionData["INSTALLFOLDER"];
            string configPath = Path.Combine(installLocation, "AWSKinesisTap.exe.config");
            string newConfigPath = Path.Combine(installLocation, "AWSKinesisTap.exe.config.new");
            try
            {
                if (!File.Exists(configPath))
                {
                    session.Log("AWSKinesisTap.exe.config not found. Copy from AWSKinesisTap.exe.config.new.");
                    File.Copy(newConfigPath, configPath);
                }
                else
                {
                    session.Log("AWSKinesisTap.exe.config found. Updating runtime section.");
                    string configContents = File.ReadAllText(configPath);
                    string newConfigContents = File.ReadAllText(newConfigPath);
                    Regex runtimeRegex = new Regex("<runtime>.*</runtime>", RegexOptions.Singleline);
                    string newRuntimeSection = runtimeRegex.Match(newConfigContents).Value;
                    configContents = runtimeRegex.Replace(configContents, newRuntimeSection);
                    File.WriteAllText(configPath, configContents);
                    session.Log("Updated runtime section.");
                }
            }
            catch (Exception ex)
            {
                session.Log($"Failed to install AWSKinesisTap.exe.config: {ex.Message}");
                return ActionResult.Failure;
            }
            session.Log("Finish installing AWSKinesisTap.exe.config");

            return ActionResult.Success;
        }

        /// <summary>
        /// Install NLog.xml if not existing. Do not override the existing.
        /// </summary>
        /// <param name="session"></param>
        /// <returns></returns>
        [CustomAction]
        public static ActionResult InstallNLog(Session session)
        {
            session.Log("Begin installing NLog.xml");
            //Need to run InstallAppSettings.SetProperty custom action first to set the value
            string installLocation = session.CustomActionData["INSTALLFOLDER"];
            string nlogPath = Path.Combine(installLocation, "NLog.xml");
            if (!File.Exists(nlogPath))
            {
                session.Log("NLog.xml not found. Writing a new NLog.xml.");
                string nlogTemplatePath = Path.Combine(installLocation, "NLogTemplate.xml");
                try
                {
                    File.Copy(nlogTemplatePath, nlogPath);
                }
                catch (Exception ex)
                {
                    session.Log($"Failed to install NLog.xml: {ex.Message}");
                    return ActionResult.Failure;
                }
            }
            session.Log("Finish installing NLog.xml");

            return ActionResult.Success;
        }

        /// <summary>
        /// Upgrade from Powershell installed KinesisTap. Detect if the KinesisTap is installed via Powershell, if it is, remove powershell registers.
        /// </summary>
        /// <param name="session"></param>
        /// <returns></returns>
        [CustomAction]
        public static ActionResult UpgradePowershellInstalledKinesisTap(Session session)
        {
            session.Log("Begin upgrading Powershell installed KinesisTap");
            //Need to run UpgradePowershellInstalledKinesisTap.SetProperty custom action first to set the value

            string serviceName = session.CustomActionData["SERVICENAME"];
            RegistryKey registryKeyHKLM = RegistryKey.OpenBaseKey(RegistryHive.LocalMachine, RegistryView.Registry64);
            string keyPath = $@"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{serviceName}";

            try
            {
                RegistryKey key = registryKeyHKLM.OpenSubKey(keyPath, true);
 
                string valueData;
                if (key != null && (valueData = (string)key.GetValue("UninstallString")) != null)
                {
                    // KinesisTap is PowerShell installed, delete the registery key
                    if (valueData.Contains("uninstall.ps1"))
                    {
                        session.Log($@"KinesisTap found is PowerShell installed.");
                        session.Log($@"Deleting registry 'SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\{serviceName}'.");
                        key.Close();
                        registryKeyHKLM.DeleteSubKeyTree(keyPath);
                    }
                }
            }
            catch (Exception ex)
            {
                session.Log($"Failed to upgrade from Powershell installed KinesisTap: {ex.Message}");
                return ActionResult.Failure;
            }
            finally
            {
                registryKeyHKLM.Close();
            }

            session.Log("Finish upgrading Powershell installed KinesisTap");
            return ActionResult.Success;
        }

        /// <summary>
        /// Ensure KinesisTap service is stopped. If not, kill the process
        /// </summary>
        /// <param name="session"></param>
        /// <returns></returns>
        [CustomAction]
        public static ActionResult EnsureKinesisTapNotRunning(Session session)
        {
            session.Log("Begin ensuring KinesisTap Service is stopped");
            //Need to run InstallAppSettings.SetProperty custom action first to set the value
            string serviceName = session.CustomActionData["SERVICENAME"];

            try
            {
                EnsureKinesisTapServiceStopped(session, serviceName);

                EnsureKinesisTapProcessNotRunning(session, serviceName);
            }
            catch (Exception ex)
            {
                session.Log($"EnsureKinesisTapServiceStopped exception: {ex.Message}");
            }

            session.Log("Finish ensuring KinesisTap Service is stopped");

            return ActionResult.Success;

        }

        private static void EnsureKinesisTapProcessNotRunning(Session session, string serviceName)
        {
            Process[] processes = Process.GetProcessesByName(serviceName);
            if (processes.Length == 0)
            {
                session.Log($"{serviceName} process is not running.");
            }
            else
            {
                session.Log($"{serviceName} process is running. Killing process.");
                foreach (var process in processes)
                {
                    try
                    {
                        process.Kill();
                    }
                    catch (Exception ex)
                    {
                        session.Log($"Failed to kill the process {process.MainModule}: {ex.Message}");
                    }
                }
            }
        }

        private static void EnsureKinesisTapServiceStopped(Session session, string serviceName)
        {
            ServiceController sc = ServiceController.GetServices().FirstOrDefault(s => s.ServiceName == serviceName);
            if (sc == null)
            {
                session.Log($"{serviceName} service is not installed.");
            }
            else
            {
                session.Log($"{serviceName} service is installed. Stopping service.");
                try
                {
                    if (sc.Status != ServiceControllerStatus.Stopped && sc.Status != ServiceControllerStatus.StopPending)
                    {
                        sc.Stop();
                        sc.WaitForStatus(ServiceControllerStatus.Stopped, TimeSpan.FromSeconds(10)); //Give service 10 seconds to shut down but not forever
                    }
                }
                catch (Exception ex)
                {
                    session.Log($"EnsureKinesisTapServiceStopped stopping service exception: {ex.Message}");
                }

            }
        }
    }
}
