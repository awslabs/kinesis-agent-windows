$ProgressPreference = 'SilentlyContinue'
$ErrorActionPreference = 'Stop'

if (![Net.ServicePointManager]::SecurityProtocol.HasFlag([Net.SecurityProtocolType]::Tls12)) {
    [Net.ServicePointManager]::SecurityProtocol = ([Net.ServicePointManager]::SecurityProtocol + [Net.SecurityProtocolType]::Tls12)
}

choco install -y wixtoolset --version 3.11.0.20170506 --no-progress;

Write-Host 'Enumerating installed targeting packs'
Get-ChildItem -Path 'C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework'

Get-Command MSBuild.exe

Get-ChildItem "C:\Program Files (x86)\WiX Toolset v3.11"
