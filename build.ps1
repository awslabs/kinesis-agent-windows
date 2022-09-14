﻿param (
    [switch]$packageOnly = $false
)

function FixLastWriteTime
{
    param
    (
        [string] $directoryPath
    )
    $dirContents = Get-ChildItem -LiteralPath $directoryPath -Recurse
    foreach($currentContent in $dirContents)
    {
        $isContainer = $currentContent -is [System.IO.DirectoryInfo]
        if(!$isContainer)
        {
            $lastWriteTime = $currentContent.LastWriteTime
            if ($lastWriteTime.Year -lt 1980)
            {
                Write-Warning "'$currentContent.FullName' has LastWriteTime earlier than 1980. Compress-Archive will store any files with LastWriteTime values earlier than 1980 as 1/1/1980 00:00."
                $lastWriteTime = [DateTime]::Now
            }
            $currentContent.LastWriteTime = $lastWriteTime
        }
    }
}

$stopwatch = New-Object System.Diagnostics.Stopwatch
$stopwatch.Start()
$VerbosePreference = "Continue"
$InformationPreference = "Continue"
$ErrorActionPreference = "Stop"

$serviceName="AWSKinesisTap"
Set-Location -Path "$PSScriptRoot"
$projDir = Join-Path -Path $PSScriptRoot -ChildPath "Amazon.KinesisTap"
$releaseDir = Join-Path -Path $projDir -ChildPath "bin\Release\win10-x64\publish"
$productVersion = Get-Content ./version -Raw

if (!$packageOnly)
{
    # try to use the environment-defined MSBuild if possible
    $msbuild = Get-Command MSBuild.exe -ErrorAction Ignore | Select-Object -ExpandProperty Path
    if ($null -eq $msbuild){
        $vsVersions = "Professional", "Enterprise", "Community", "BuildTools"
        $yearMaps = @{ '2017' = '15.0'; '2019' = 'Current' }
        foreach ($year in $yearMaps.Keys)
        {
            foreach ( $vsVersion in $vsVersions ) 
            { 
                $msbuild = "C:\Program Files (x86)\Microsoft Visual Studio\$year\$vsVersion\MSBuild\$($yearMaps[$year])\Bin\MSBuild"
                if (Test-Path -Path $msbuild)
                {
                    break
                }
            }
        }
    }
    $sln = Join-Path -Path $PSScriptRoot -ChildPath "$serviceName.sln"
    $msiBuildSln = Join-Path -Path $PSScriptRoot -ChildPath "KinesisTapMsiBuild.sln"
    $service = Get-Service -Name $serviceName -ErrorAction Ignore

    if ($service -and $service.Status -eq 'Running')
    {
        Stop-Service -Name $serviceName -Force
        Start-Sleep -Milliseconds 2000
    }

    if (Test-Path -Path $releaseDir)
    {
	    Write-Verbose 'Deleting previous files from release directory'
	    Remove-Item -Path $releaseDir -Recurse -Force -Confirm:$false
    }

    try
    {
        Write-Verbose 'Building agent'
        dotnet publish .\Amazon.KinesisTap.DiagnosticTool\Amazon.KinesisTap.DiagnosticTool.csproj -c Release -p:Version="$productVersion" --self-contained true -r win10-x64
        dotnet publish .\Amazon.KinesisTap\Amazon.KinesisTap.csproj -c Release -p:Version="$productVersion" --self-contained true -r win10-x64
	    & "$msbuild" "$msiBuildSln" /p:Configuration=Release /p:Platform="x64"
    }
    catch
    {
	    throw "Failed to publish project. Error: $_"
    }
}

$kinesisTapPath = Join-Path -Path $releaseDir -ChildPath "$serviceName.exe"
$outputDir = Join-Path -Path $projDir -ChildPath "bin\"

Write-Verbose 'Copy msi to output dir'
Copy-Item -Path "$(Join-Path -Path $PSScriptRoot -ChildPath "KinesisTapWixSetup\bin\x64\Release\$serviceName.msi")" -Destination "$(Join-Path -Path $outputDir -ChildPath "$serviceName.$productVersion.msi")"

$msiFile = Join-Path -Path $projDir -ChildPath "bin\$serviceName.$productVersion.msi"

#Write-Verbose 'Deleting PDB files'
#Get-ChildItem -Path $releaseDir -Recurse *.pdb | Remove-Item -Force

Write-Verbose 'Deleting XML files'
Get-ChildItem -Path $releaseDir -Recurse *.xml | Remove-Item -Force

Copy-Item "$projDir\Nlog.xml" "$releaseDir"
Copy-Item "$projDir\appsettingsTemplate.json" "$releaseDir\appsettings.json"

Write-Verbose 'Fix LastWriteTime of each file: https://github.com/PowerShell/Microsoft.PowerShell.Archive/issues/55'
FixLastWriteTime $releaseDir

#build chocolatey package
$chocolateyTemplateDir = Join-Path -Path $PSScriptRoot -ChildPath "ChocolateyTemplate"
Write-Verbose 'Prepare chocolatey package'

$chocolateyPackageName = "KinesisTap.$productVersion"
$chocolateyPackageDir = Join-Path -Path $outputDir -ChildPath $chocolateyPackageName
$chocolateyPackageFile = "$chocolateyPackageDir.nupkg"
if (Test-Path -Path $chocolateyPackageDir)
{
	Write-Verbose 'Deleting previously published chocolatey folder'
	Remove-Item -Path $chocolateyPackageDir -Recurse -Confirm:$false -Force
}

if (Test-Path -Path $chocolateyPackageFile)
{
	Write-Verbose 'Deleting previously published chocolatey package'
	Remove-Item -Path $chocolateyPackageFile -Recurse -Confirm:$false -Force
}

$null = New-Item -ItemType Directory -Path $chocolateyPackageDir -Force
& robocopy.exe $chocolateyTemplateDir $chocolateyPackageDir /E /R:1 /W:1 /NJH /NJS /NP
& Copy-Item -Path "$(Join-Path -Path $outputDir -ChildPath "$serviceName.$productVersion.msi")" -Destination "$chocolateyPackageDir\tools\$serviceName.msi"

$nuspec = [xml](Get-Content -Path "$chocolateyPackageDir\KinesisTap.nuspec")
$nuspec.SelectSingleNode("//package/metadata/version")."#text" = [string]$productVersion
$nuspec.Save("$chocolateyPackageDir\KinesisTap.nuspec")

Write-Verbose 'Create chocolatey package'
#Compress-Archive -Path "$chocolateyPackageDir\*" -DestinationPath "$chocolateyPackageDir.zip" -CompressionLevel Fastest
#Rename-Item "$chocolateyPackageDir.zip"  "$chocolateyPackageName.nupkg"
try
{
    & nuget.exe pack "$chocolateyPackageDir\KinesisTap.nuspec" -OutputDirectory "$outputDir" -nopackageanalysis
}
catch
{
    Write-Verbose "Could not build nupkg file: $_"
}

# Birdwatcher package file build
$birdwatcherOutputDir = Join-Path -Path $projDir -ChildPath "Birdwatcher\bin"
$birdwatcherReleaseDir = Join-Path -Path $birdwatcherOutputDir -ChildPath "Release"

if (Test-Path -Path $birdwatcherOutputDir)
{
    Write-Verbose 'Deleting previously published Birdwatcher package'
    Remove-Item -Path $birdwatcherOutputDir -Recurse -Confirm:$false -Force
}

New-Item -ItemType Directory -Path $birdwatcherReleaseDir -Force

Copy-Item -Path $msiFile -Destination "$(Join-Path -Path $birdwatcherReleaseDir -ChildPath $serviceName'.msi')"
Copy-Item -Path "$(Join-Path -Path $chocolateyTemplateDir 'tools\chocolateyinstall.ps1')" -Destination "$(Join-Path -Path $birdwatcherReleaseDir -ChildPath 'install.ps1')" 
Copy-Item -Path "$(Join-Path -Path $chocolateyTemplateDir 'tools\chocolateyuninstall.ps1')" -Destination "$(Join-Path -Path $birdwatcherReleaseDir -ChildPath 'uninstall.ps1')"

$birdwatcherZipFileName = $serviceName + ".zip"
Compress-Archive -Path "$birdwatcherReleaseDir\*" -DestinationPath "$(Join-Path -Path $birdwatcherOutputDir -ChildPath $birdwatcherZipFileName)"

Exit $LASTEXITCODE