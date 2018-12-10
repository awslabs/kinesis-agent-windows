param (
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

$serviceName="AWSKinesisTap"
Set-Location -Path "$PSScriptRoot"
$projDir = Join-Path -Path $PSScriptRoot -ChildPath "Amazon.KinesisTap"
$releaseDir = Join-Path -Path $projDir -ChildPath "bin\Release\"

if (!$packageOnly)
{
    $sln = Join-Path -Path $PSScriptRoot -ChildPath "$serviceName.sln"
    $vsVersions = "Professional", "Enterprise", "Community", "BuildTools"
    foreach ( $vsVersion in $vsVersions ) 
    { 
        $msbuild = 'C:\Program Files (x86)\Microsoft Visual Studio\2017\' + $vsVersion + '\MSBuild\15.0\Bin\MSBuild'
        if (Test-Path -Path $msbuild)
        {
            break
        }
    }

    $service = Get-Service -Name $serviceName -ErrorAction Ignore

    if ($service -and $service.Status -eq 'Running')
    {
        Stop-Service -Name $serviceName -Force
        sleep -Milliseconds 2000
    }

    if (Test-Path -Path $releaseDir)
    {
	    Write-Verbose 'Deleting previous files from release directory'
	    Remove-Item -Path $releaseDir -Recurse -Force -Confirm:$false
    }

    try
    {
	    Write-Verbose "Building agent"
	    & "$msbuild" $sln /p:Configuration=Release /p:Platform="Any CPU"
    }
    catch
    {
	    throw "Failed to publish project. Error: $_"
    }
}

$zipFile = Join-Path -Path $projDir -ChildPath "bin\KinesisTap.zip"
if (Test-Path -Path $zipFile)
{
	Write-Verbose 'Deleting previously created zip file'
	Remove-Item -Path $zipFile -Recurse -Confirm:$false -Force
}

#Write-Verbose 'Deleting PDB files'
#Get-ChildItem -Path $releaseDir -Recurse *.pdb | Remove-Item -Force

Write-Verbose 'Deleting XML files'
Get-ChildItem -Path $releaseDir -Recurse *.xml | Remove-Item -Force

$diagDir = Join-Path -Path $PSScriptRoot -ChildPath "Amazon.KinesisTap.DiagnosticTool"
$diagReleaseDir = Join-Path -Path $diagDir -ChildPath "bin\Release\"

Write-Verbose 'Copying KTdiag.exe and its Configuration to bin\release'
Copy-Item "$diagReleaseDir\KTDiag.exe" "$releaseDir"
Copy-Item "$diagReleaseDir\KTDiag.exe.config" "$releaseDir"

Write-Verbose 'Copying log4net.dll to bin\release'
Copy-Item "$diagReleaseDir\log4net.dll" "$releaseDir"

Write-Verbose 'Copying Newtonsoft.Json.Schema.dll to bin\release'
Copy-Item "$diagReleaseDir\Newtonsoft.Json.Schema.dll" "$releaseDir"

Write-Verbose 'Copying System.Console.dll to bin\release'
Copy-Item "$diagReleaseDir\System.Console.dll" "$releaseDir"

$diagCoreDir = Join-Path -Path $PSScriptRoot -ChildPath "Amazon.KinesisTap.DiagnosticTool.Core\bin\Release\netstandard1.3\"
Write-Verbose 'Copying Amazon.KinesisTap.DiagnosticTool.Core.??? and .pdb to bin\release'
Copy-Item "$diagCoreDir\Amazon.KinesisTap.DiagnosticTool.Core.???" "$releaseDir" 

Copy-Item "$projDir\Nlog.xml" "$releaseDir"
Copy-Item "$projDir\appsettingsTemplate.json" "$releaseDir\appsettings.json"

Write-Verbose "Duplicating $serviceName.exe.config in bin\release"
Copy-Item "$releaseDir\$serviceName.exe.config" "$releaseDir\$serviceName.exe.config.new" 

$ulsPlugInDir = Join-Path -Path $PSScriptRoot -ChildPath "Amazon.KinesisTap.Uls\bin\release\netstandard1.3"
Write-Verbose 'Copying Amazon.KinesisTap.Uls.??? and .pdb to bin\release'
Copy-Item "$ulsPlugInDir\Amazon.KinesisTap.Uls.???" "$releaseDir" 

Write-Verbose 'Copying Amazon.KinesisTap.AutoUpdate.??? and .pdb to bin\release'
Copy-Item "$PSScriptRoot\Amazon.KinesisTap.AutoUpdate\bin\release\netstandard1.3\Amazon.KinesisTap.AutoUpdate.???" "$releaseDir" 


Write-Verbose 'Fix LastWriteTime of each file: https://github.com/PowerShell/Microsoft.PowerShell.Archive/issues/55'
FixLastWriteTime $releaseDir

Write-Verbose 'Zipping release directory'
Compress-Archive -Path "$releaseDir\*" -DestinationPath $zipFile -CompressionLevel Fastest

#build chocolatey package
$chocolateyTemplateDir = Join-Path -Path $PSScriptRoot -ChildPath "ChocolateyTemplate"
Write-Verbose 'Prepare chocolatey package'
$kinesisTapPath = Join-Path -Path $releaseDir -ChildPath "$serviceName.exe"

$chocolateyPackageVersion = [System.Diagnostics.FileVersionInfo]::GetVersionInfo($kinesisTapPath).FileVersion
$chocolateyPackageName = "KinesisTap.$chocolateyPackageVersion"
$outputDir = Join-Path -Path $projDir -ChildPath "bin\"
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
& Copy-Item -Path $zipFile -Destination "$chocolateyPackageDir\tools"

$nuspec = [xml](Get-Content -Path "$chocolateyPackageDir\KinesisTap.nuspec")
$nuspec.SelectSingleNode("//package/metadata/version")."#text" = $chocolateyPackageVersion
$nuspec.Save("$chocolateyPackageDir\KinesisTap.nuspec")

Write-Verbose 'Create chocolatey package'
#Compress-Archive -Path "$chocolateyPackageDir\*" -DestinationPath "$chocolateyPackageDir.zip" -CompressionLevel Fastest
#Rename-Item "$chocolateyPackageDir.zip"  "$chocolateyPackageName.nupkg"
& nuget.exe pack "$chocolateyPackageDir\KinesisTap.nuspec" -OutputDirectory "$outputDir" -nopackageanalysis

# Birdwatcher package file build
$birdwatcherOutputDir = Join-Path -Path $projDir -ChildPath "Birdwatcher\bin"
$birdwatcherReleaseDir = Join-Path -Path $birdwatcherOutputDir -ChildPath "Release"

if (Test-Path -Path $birdwatcherOutputDir)
{
    Write-Verbose 'Deleting previously published Birdwatcher package'
    Remove-Item -Path $birdwatcherOutputDir -Recurse -Confirm:$false -Force
}

New-Item -ItemType Directory -Path $birdwatcherReleaseDir -Force

Copy-Item -Path $zipFile -Destination "$(Join-Path -Path $birdwatcherReleaseDir -ChildPath 'KinesisTap.zip')"
Copy-Item -Path "$(Join-Path -Path $chocolateyTemplateDir 'tools\chocolateyinstall.ps1')" -Destination "$(Join-Path -Path $birdwatcherReleaseDir -ChildPath 'install.ps1')" 
Copy-Item -Path "$(Join-Path -Path $chocolateyTemplateDir 'tools\chocolateyuninstall.ps1')" -Destination "$(Join-Path -Path $birdwatcherReleaseDir -ChildPath 'uninstall.ps1')"

$birdwatcherZipFileName = $serviceName + ".zip"
Compress-Archive -Path "$birdwatcherReleaseDir\*" -DestinationPath "$(Join-Path -Path $birdwatcherOutputDir -ChildPath $birdwatcherZipFileName)"

Write-Verbose "Script completed in $($stopwatch.Elapsed.TotalSeconds) seconds"

