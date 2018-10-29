#################################
#
# KinesisTap Installation
#

# KinesisTap Variables
$serviceName = 'AWSKinesisTap'
$installPath = [System.IO.Path]::Combine($env:ProgramFiles, 'Amazon', $serviceName)

#####################################################################
# Extract KinesisTap binaries
Write-Verbose ("Checking to see if $serviceName service has been previously deployed.")
$serviceDisplayName = 'AWS KinesisTap'
$serviceDescription = 'AWS Windows Log Agent'
$serviceImagePath = Join-Path -Path $installPath -ChildPath "$serviceName.exe"
$registryKey = "HKLM:\SYSTEM\CurrentControlSet\Services\$serviceName"

$service = Get-Service -Name $serviceName -ErrorAction Ignore
if ($null -eq $service) {
    Write-Verbose 'Service doesnt exist, creating it...'
    $null = [System.IO.Directory]::CreateDirectory($installPath)
    New-Service -Name $serviceName -DisplayName $serviceDisplayName -BinaryPathName "$serviceImagePath" -Description $serviceDescription -StartupType Automatic -Confirm:$false
    $isNewInstall = $true
} else {
    if ($service.Status -eq 'Running') {
        $serviceRunning = $true
        Write-Verbose 'Service has already been installed, stopping it...'
        Stop-Service -Name $serviceName -Force
        Get-Process -Name "$serviceName*" -ErrorAction Ignore | Stop-Process -Force
    }

    Write-Verbose 'Backing up existing config files...'
    $configFiles = ('appsettings.json', 'NLog.xml', "$serviceName.exe.config", 'endpoints.json')
    foreach ($configFile in $configFiles) {
        $configFilePath = Join-Path -Path $InstallPath -ChildPath $configFile
        if (Test-Path -Path $configFilePath) {
            Copy-Item -Path $configFilePath -Destination $env:TEMP -Force
        }
    }

    Write-Verbose 'Removing existing service content...'
    if (Test-Path -Path $installPath) {
        Remove-Item -Path $installPath -Force -Recurse -ErrorAction 'SilentlyContinue'
    }
    # The .NET method succeeds even if the path already exists
    [System.IO.Directory]::CreateDirectory($installPath) | Out-Null

    Write-Verbose 'Updating Windows Service details...'
    $registryData = Get-ItemProperty -Path $registryKey
    if ($registryData.DisplayName -ne $serviceDisplayName) {
        Set-ItemProperty -Path $registryKey -Name 'DisplayName' -Value $serviceDisplayName
    }
    if ($registryData.ImagePath -ne $serviceImagePath) {
        Set-ItemProperty -Path $registryKey -Name 'ImagePath' -Value ('"{0}"' -f $serviceImagePath)
    }
    if ($registryData.Description -ne $serviceDescription) {
        Set-ItemProperty -Path $registryKey -Name 'Description' -Value $serviceDescription
    }

    $isNewInstall = $false
}

$toolsDir = "$(Split-Path -Parent $MyInvocation.MyCommand.Definition)"
$fileLocation = Join-Path -Path $toolsDir -ChildPath "KinesisTap.zip"
Write-Verbose 'Extracting AWS KinesisTap Binaries...'
$null = [System.Reflection.Assembly]::LoadWithPartialName('System.IO.Compression.FileSystem')
[System.IO.Compression.ZipFile]::ExtractToDirectory($fileLocation, $installPath)

if (-not $isNewInstall) {
    #####################################################################
    # Restore KinesisTap Database
    foreach ($configFile in $configFiles) {
        $configFilePath = Join-Path -Path $env:TEMP -ChildPath $configFile
        if (Test-Path -Path $configFilePath) {
            Copy-Item -Path $configFilePath -Destination $installPath  -Force
        }
    }
}

# Merge KinesisTap.exe.config files
$KinesisTapConfigPath = Join-Path -Path $installPath -ChildPath "$serviceName.exe.config.new"

$KinesisTapConfigNew = [System.Xml.XmlDocument](Get-Content $KinesisTapConfigPath)
$newPolicy = $KinesisTapConfigNew["configuration"]["runtime"]

$KinesisTapConfigPath = Join-Path -Path $installPath -ChildPath "$serviceName.exe.config"
$KinesisTapConfig = [System.Xml.XmlDocument](Get-Content $KinesisTapConfigPath)
$oldPolicy = $KinesisTapConfig["configuration"]["runtime"]

$KinesisTapConfig["configuration"].RemoveChild($oldPolicy) | Out-Null
$KinesisTapConfig["configuration"].AppendChild($KinesisTapConfig.ImportNode($newPolicy, $TRUE)) | Out-Null

$KinesisTapConfig.Save($KinesisTapConfigPath)

#####################################################################
# Create Uninstall registry keys
$registryPath = "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\$serviceName"
if (-not (Test-Path $registryPath)) {
    New-Item -Path $registryPath -Force | Out-Null
}
$kinesisPath = Join-Path -Path $installPath -ChildPath "$serviceName.exe"
New-ItemProperty -Path $registryPath -Name "DisplayIcon" -Value $kinesisPath -PropertyType String -Force | Out-Null
New-ItemProperty -Path $registryPath -Name "DisplayName" -Value $serviceName -PropertyType String -Force | Out-Null
New-ItemProperty -Path $registryPath -Name "DisplayVersion" -Value `
([System.Diagnostics.FileVersionInfo]::GetVersionInfo($kinesisPath).FileVersion) -PropertyType String -Force | Out-Null
New-ItemProperty -Path $registryPath -Name "EstimatedSize" -Value `
([int]((Get-ChildItem $installPath -Recurse | Measure-Object -Property Length -sum).Sum / 1024)) -PropertyType DWORD -Force | Out-Null
New-ItemProperty -Path $registryPath -Name "InstallLocation" -Value $installPath -PropertyType String -Force | Out-Null
New-ItemProperty -Path $registryPath -Name "NoModify" -Value "1" -PropertyType DWORD -Force | Out-Null
New-ItemProperty -Path $registryPath -Name "NoRepair" -Value "1" -PropertyType DWORD -Force | Out-Null
New-ItemProperty -Path $registryPath -Name "Publisher" -Value "Amazon.com" -PropertyType String -Force | Out-Null
$uninstallScriptPath = Join-Path -Path $toolsDir -ChildPath "chocolateyuninstall.ps1"
New-ItemProperty -Path $registryPath -Name "UninstallString" -Value "Powershell -file ""$uninstallScriptPath""" -PropertyType String -Force | Out-Null

if ($serviceRunning) {
    Start-Service -Name $serviceName
}
