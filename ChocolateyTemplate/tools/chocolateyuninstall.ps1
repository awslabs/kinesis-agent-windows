#################################
#
# KinesisTap Uninstallation
#
 
# KinesisTap Variables
$serviceName = 'AWSKinesisTap'
$installPath = Join-Path -Path $env:ProgramFiles -ChildPath "Amazon\$serviceName"

Write-Verbose ('Checking to see if {0} service has been previously deployed.' -f $serviceName)

if ($null -ne (Get-Service -Name $serviceName -ErrorAction Ignore))
{
    Write-Verbose 'KinesisTap has been installed. Uninstalling KinesisTap...'
    $toolsDir = "$(Split-Path -Parent $MyInvocation.MyCommand.Definition)"
    $fileLocation = Join-Path -Path $toolsDir -ChildPath "KinesisTap.msi"
    msiexec /x $fileLocation /q
}