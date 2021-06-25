$ProgressPreference = 'SilentlyContinue'
$ErrorActionPreference = 'Stop'


if (![Net.ServicePointManager]::SecurityProtocol.HasFlag([Net.SecurityProtocolType]::Tls12)) {
    [Net.ServicePointManager]::SecurityProtocol = ([Net.ServicePointManager]::SecurityProtocol + [Net.SecurityProtocolType]::Tls12)
}

choco install -y wixtoolset --version 3.11.0.20170506 --no-progress;

# Write-Host 'Enumerating installed targeting packs'
# Get-ChildItem -Path 'C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework'

Write-Host 'Installing dotnet 5.0 SDK'
Invoke-WebRequest "https://download.visualstudio.microsoft.com/download/pr/2892493e-df43-409e-af68-8b14aa75c029/53156c889fc08f01b7ed8d7135badede/dotnet-sdk-5.0.100-win-x64.exe" -OutFile dotnet-sdk-5.0.exe
Start-Process -Wait -FilePath .\dotnet-sdk-5.0.exe -Argument "/silent" -PassThru -NoNewWindow

Get-Command MSBuild.exe

Get-ChildItem "C:\Program Files (x86)\WiX Toolset v3.11"
