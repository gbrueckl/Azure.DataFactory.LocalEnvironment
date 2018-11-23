# Azure.DataFactory.LocalEnvironment
This repository provides some tools which make it easier to work with Azure Data Factory (ADF). It mainly contains two features:
- Debug Custom .Net Activities locally (within VS and without deployment to the ADF Service!)
- Export existing ADF Visual Studio projects a Azure Resource Manager (ARM) template for deployment

In addition, the repository also contains various samples to demonstrate how to work with the ADF Local Environment.

## Update 2018-11-23:
- published as NuGet-package: https://www.nuget.org/packages/gbrueckl.Azure.DataFactory.LocalEnvironment

## Update 2017-03-02:
- added support for some of the built-in ADF Date/Time functions
- you are now able to debug multiple activties in the same run
- temporary fix so no exception is thrown during clean-up of the temporary files after debugging
- fix to overwrite all settings defined in the config-file (not just the ones with a value of "\<config\>")
- config files now also support nested JSON as values to overwrite whole JSON subtrees 

# Setup
The package can now be downloaded from the NuGet gallery: https://www.nuget.org/packages/gbrueckl.Azure.DataFactory.LocalEnvironment

If you install it from within Visual Studio, simply search for "gbrueckl":

![Alt text](http://files.gbrueckl.at/github/Azure.DataFactory.LocalEnvironment/ADF_LocalEnvironment_Add_NuGet_Package_VS.png "Manage Nuget packages of project")
![Alt text](http://files.gbrueckl.at/github/Azure.DataFactory.LocalEnvironment/ADF_LocalEnvironment_Add_NuGet_Package_VS_Explorer.png "Search for NuGet package in package explorer searching for 'gbrueckl'")

Alternatively you can also use the NuGet package manager console and run
```powershell
Install-Package gbrueckl.Azure.DataFactory.LocalEnvironment
```
![Alt text](http://files.gbrueckl.at/github/Azure.DataFactory.LocalEnvironment/ADF_LocalEnvironment_Add_NuGet_Package_VS_Console.png "Install the NuGet package via the package manager console")

Further details can also be found here:
https://docs.microsoft.com/en-us/nuget/consume-packages/ways-to-install-a-package

# Debug Custom .Net Activities
To set everything up, it is recommended to add a new Console Application project to your existing VS solution which already contains the code for the custom activity and also the ADF project itself. Other setups (e.g. with multiple solutions/projects) would also work but make it much harder to develop and debug your custom code.
Once you have added your Console Application, you need to add the NuGet package "gbrueckl.Azure.DataFactory.LocalEnvironment" - see [Setup] above.

Next step is to add the namespace of the ADFLocalEnvironment to your code so you can start start using the ADFLocalEnvironment class. Using an existing ADF Project Visual Studio File (\*.dfproj) and an optional name for an ADF Configuration to use you can create a new intance of the ADFLocalEnvironment. The configuration file also has to be part of the ADF project referenced in the first parameter! Then you can simply call ExecuteActivity-function and pass in the mandatory parameters:
- Name of the Pipeline
- Name of the Activity 
- SliceStart
- SliceEnd
- A Custom ActivityLogger (optional) 
![Alt text](http://files.gbrueckl.at/github/Azure.DataFactory.LocalEnvironment/ADF_LocalEnvironment_DebugActivity.png "Setup Console Application for debugging")

Now you can simply add breakpoints to your custom activity's code, execute the Console Application and the debugger will jump in once your breakpoint is reached:
![Alt text](http://files.gbrueckl.at/github/Azure.DataFactory.LocalEnvironment/ADF_LocalEnvironment_DebugActivity_Breakpoing.png "Debug using breakpoints")



# Export to ARM Template
A very common requirement when it comes to ADF deployment is to integrate it with regular ARM deployments. Unfortunatelly, at the time being, Microsoft does not offer a native way to convert an ADF project into a deployable ARM template. However, ADFLocalEnvironment class allows you to do exactly this. The first steps to setup everything are very similar to the steps described above but instead of calling the ExecuteActivity-function, we now call the ExportARMTemplate-function which supports the following parameters:
- Path to an existing ARM Deployment project file (\*.deployproj)
- An Azure region where the ADF should be deployed to (optional, default is the location of the ResourceGroup) 
- Whether all PipeLines should be paused or not (optional, default is False)
![Alt text](http://files.gbrueckl.at/github/Azure.DataFactory.LocalEnvironment/ADF_LocalEnvironment_ExportToARMTemplate.png "Export to ARM Template")

The ExportARMTemplate-function will then process the following steps:
- Generate a file called "AzureDataFactory.json" in the root folder of the ARM project
- Generate a file called "AzureDataFactory.parameters.json" in the root folder of the ARM project
- Copy all dependencies defined in the ADF project to the root folder of the ARM project using the following structure \ADF_Dependencies\gbdomaindata\adfcontainer\package\

Those newly copied/created have to be added to the ARM project manually once. First we need to active "Show Hidden Items" at the level of our ARM project:
![Alt text](http://files.gbrueckl.at/github/Azure.DataFactory.LocalEnvironment/ADF_LocalEnvironment_ShowHiddenItems.png "Show hidden Project Items")
And then we can include the necessary files (see above) in our ARM project:
![Alt text](http://files.gbrueckl.at/github/Azure.DataFactory.LocalEnvironment/ADF_LocalEnvironment_IncludeHiddenItems.png "Include hidden Project Items")

For all of our dependencies we also need to change the properties to make sure the dependencies are also copied to the output folder:
![Alt text](http://files.gbrueckl.at/github/Azure.DataFactory.LocalEnvironment/ADF_LocalEnvironment_CopyContentToOutput.png "Copy Content to Output")

If your project actually contains dependencies, you also need to modify the PowerShell script that comes with the ARM project ([\MyARMTemplate/Deploy-AzureResourceGroup.ps1](https://github.com/gbrueckl/Azure.DataFactory.LocalEnvironment/blob/master/MyARMTemplate/Deploy-AzureResourceGroup.ps1)) and add the lines of code generated by the GetARMPostDeploymentScript-function:
![Alt text](http://files.gbrueckl.at/github/Azure.DataFactory.LocalEnvironment/ADF_LocalEnvironment_ExtendPowerShellScript.png "Extend PowerShell Script")

Now you can deploy your ADF project just like any other regular ARM template:
![Alt text](http://files.gbrueckl.at/github/Azure.DataFactory.LocalEnvironment/ADF_LocalEnvironment_DeployARMTemplate.png "Deploy ARM template")
