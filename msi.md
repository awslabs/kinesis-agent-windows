# Amazon Kinesis Agent for Windows MSI Installer

## Why we created MSI installer?

We created MSI installer in response to customer request to simply the installation experience. MSI is a Microsoft Windows standard. MSI is reliable as it has built-in mechanism to rollback on failed installation.

## MSI installation by end users.

To install, just download the latest msi installer. Double click to run the installer and follow the installer UI.

## Silent installation by system administrators

System administrators can use msiexec command to install silently:

```
msiexec /i pathTo.msi /q
```

To trouble-shoot installations, you can turn on logging from the command line:

```
msiexec /i pathTo.msi /q /L*V pathTo.log 
```

## To build the MSI package from the source code

You need to install:

* [WixTools](http://wixtoolset.org/releases/) 3.11.1.
* [Wix Toolset Visual Studio 2017 extension](https://marketplace.visualstudio.com/items?itemName=RobMensching.WixToolsetVisualStudio2017Extension).
