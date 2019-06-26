# Amazon Kinesis Agent for Windows MSI Installer

## Why we created MSI installer?

We created MSI installer in response to customer requests to simply the installation experience. MSI is a Microsoft Windows standard installer. MSI is reliable as it has built-in mechanism to rollback on failed installations.

## MSI installation by end users.

To install, just download the latest Amazon Kinesis Agent for Windows msi package. Double click to run the installer and follow the installer UI.

## Silent installation by system administrators

System administrators can use msiexec command to install silently, for example:

```
msiexec /i AWSKinesisTap.1.1.168.1.msi /q
```

To trouble-shoot installations, you can turn on logging from the command line:

```
msiexec /i AWSKinesisTap.1.1.168.1.msi /q /L*V logfile.log
```

## Uninstall

You can uninstall from the "Add or remove program" applet or execute:

```
msiexec.exe /x {ADAB3982-68AA-4B45-AE09-7B9C03F3EBD3} /q
```

## To build the MSI package from the source code

You need to install:

* [WixTools](http://wixtoolset.org/releases/) 3.11.1.
* [Wix Toolset Visual Studio 2017 extension](https://marketplace.visualstudio.com/items?itemName=RobMensching.WixToolsetVisualStudio2017Extension).
