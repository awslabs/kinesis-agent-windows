#################################
#
# KinesisTap Installation
#

# KinesisTap Variables
$serviceName = 'AWSKinesisTap'
$installPath = [System.IO.Path]::Combine($env:ProgramFiles, 'Amazon', $serviceName)

#####################################################################
# Install KinesisTap
Write-Verbose ("Installing $serviceName.")
$toolsDir = "$(Split-Path -Parent $MyInvocation.MyCommand.Definition)"
$fileLocation = Join-Path -Path $toolsDir -ChildPath "$serviceName.msi"

$proc = Start-Process -FilePath "msiexec.exe" -ArgumentList "/i $fileLocation /q" -PassThru
if (!$proc.WaitForExit([Timespan]::FromSeconds(60).TotalMilliseconds))
{
    Write-Verbose ("$serverName installation did not complete after 60 seconds")
}

exit $proc.ExitCode
