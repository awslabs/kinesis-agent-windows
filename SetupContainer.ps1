$ProgressPreference = 'SilentlyContinue'
$ErrorActionPreference = 'Stop'

$net46InstallerUri = 'https://download.microsoft.com/download/8/2/F/82FF2034-83E6-4F93-900D-F88C7AD9F3EE/NDP46-TargetingPack-KB3045566-ENU.exe'
$netCoreInstallerUri = 'https://dot.net/v1/dotnet-install.ps1'
$installDir = "$env:TMP\install"
$installLog = "$installDir\dotnet46install.txt"
if (![IO.Directory]::Exists($installDir)) { [IO.Directory]::CreateDirectory($installDir) }

if (![Net.ServicePointManager]::SecurityProtocol.HasFlag([Net.SecurityProtocolType]::Tls12)) {
    [Net.ServicePointManager]::SecurityProtocol = ([Net.ServicePointManager]::SecurityProtocol + [Net.SecurityProtocolType]::Tls12)
}

Write-Host 'Downloading .NET 46 targeting pack'
Invoke-WebRequest -Uri $net46InstallerUri -OutFile "$installDir\NDP46-TargetingPack-KB3045566-ENU.exe" -UseBasicParsing

Write-Host 'Installing .NET 46 targeting pack'
$proc = Start-Process -FilePath "$installDir\NDP46-TargetingPack-KB3045566-ENU.exe" -ArgumentList "/install /quiet /norestart /log $installLog" -PassThru -NoNewWindow
$proc.WaitForExit()
Write-Host "Finished installing .NET 46 targeting pack with exit code '$($proc.ExitCode)'"
Get-Content -Path $installLog

Write-Host 'Enumerating installed targeting packs'
Get-ChildItem -Path 'C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework'

Write-Host 'Downloading .NET Core Install script'
Invoke-WebRequest -Uri $netCoreInstallerUri -OutFile "$installDir\dotnet-install.ps1" -UseBasicParsing

Write-Host 'Running .NET Core Install script'
& "$installDir\dotnet-install.ps1" -Verbose