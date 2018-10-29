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
    Stop-Service -Name $serviceName -Force
    $service = Get-WmiObject -Class Win32_Service -Filter "Name='$serviceName'"
    $service.delete()
	Get-Process -Name "$serviceName*" -ErrorAction Ignore | Stop-Process -Force

    Write-Verbose 'Removing existing service content...'
    Remove-Item -Path $InstallPath -Recurse -Force
}

#####################################################################
# Delete Uninstall registry keys
$registryPath="HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\$serviceName"
IF(Test-Path $registryPath)
{
	Remove-Item -Path $registryPath -Recurse -Force | Out-Null
}
