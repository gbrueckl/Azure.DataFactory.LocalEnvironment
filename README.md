# Azure.DataFactory.LocalEnvironment
This repository provides some tools which make it easier to work with Azure Data Factory (ADF). It mainly contains two features:
- Debug Custom .Net Activities locally
- Export existing ADF Visual Studio projects a Azure Resource Manager (ARM) template for deployment

In addition, the repository also contains various samples to demonstrate how to work with the ADF Local Environment.

#Debug Custom .Net Activities
To set everything up, it is recommended to add a new Console Application project to your existing VS solution which already contains the code for the custom activity and also the ADF project itself. Other setups (e.g. with multiple solutions/projects) would also work but make it much harder to develop and debug your custom code.
Once you have added your Console Application, you need to add a reference to the LocalEnvironment-Assembly. (For convenience I also included the built DLL under [\ADFLocalEnvironment\Download\gbrueckl.Azure.DataFactory.ADFLocalEnvironment.dll](https://github.com/gbrueckl/Azure.DataFactory.LocalEnvironment/blob/master/ADFLocalEnvironment/Download/gbrueckl.Azure.DataFactory.ADFLocalEnvironment.dll) so you can download it directly if you are not interested in the source code itself)
![Alt text](http://files.gbrueckl.at/github/Azure.DataFactory.LocalEnvironment/ADF_LocalEnvironment_HowTo_AddReference.png "Add Reference to Project")
![Alt text](http://files.gbrueckl.at/github/Azure.DataFactory.LocalEnvironment/ADF_LocalEnvironment_HowTo_AddReference_2.png "Select DLL to be added")

Next step is to add the namespace of the ADFLocalEnvironment to your code so you can start start using the ADFLocalEnvironment class. Using an existing ADF Project Visual Studio File (\*.dfproj) and an optional name for an ADF Configuration to use you can create a new intance of the ADFLocalEnvironment. Then you can simply call ExecuteActivity-function and pass in the mandatory parameters:
- Name of the Pipeline
- Name of the Activity 
- SliceStart
- SliceEnd
- A Custom ActivityLogger (optional) 
![Alt text](http://files.gbrueckl.at/github/Azure.DataFactory.LocalEnvironment/ADF_LocalEnvironment_DebugActivity.png "Setup Console Application for debugging")

Now you can simply add breakpoints to your custom activity's code, execute the Console Application and the debugger will jump in once your breakpoint is reached:
![Alt text](http://files.gbrueckl.at/github/Azure.DataFactory.LocalEnvironment/ADF_LocalEnvironment_DebugActivity_Breakpoing.png "Debug using breakpoints")



#Export to ARM Template
A very common requirement when it comes to ADF deployment is to integrate it with regular ARM deployments. Unfortunatelly, at the time being, Microsoft does not offer a native way to convert an ADF project into a deployable ARM template. However, ADFLocalEnvironment class allows you to do exactly this. The first steps to setup everything are very similar to the steps described above but instead of calling the ExecuteActivity-function, we now call the ExportARMTemplate-function which supports the following parameters:
- Path to an existing ARM Deployment project file (\*.deployproj)
- An Azure region where the ADF should be deployed to (optional, default is the location of the ResourceGroup) 
- Whether all PipeLines should be paused or not (optional, default is False)
![Alt text](http://files.gbrueckl.at/github/Azure.DataFactory.LocalEnvironment/ADF_LocalEnvironment_ExportToARMTemplate.png "Export to ARM Template")
