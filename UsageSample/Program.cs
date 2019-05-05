using gbrueckl.Azure.DataFactory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UsageSample
{
    class Program
    {
        static void Main(string[] args)
        {
            #region Setup ADF LocalEnvironment
            ADFLocalEnvironment env;

            env = new ADFLocalEnvironment(@"..\..\..\MyADFProject\MyADFProject.dfproj");          // Path to the ADF Project file (absolute or relative)

            /*
            // if you want to apply a Config-File, please use this code:
            env = new ADFLocalEnvironment(
                @"..\..\..\MyADFProject\MyADFProject.dfproj",           // Path to the ADF Project file (absolute or relative)
                "MyConfig",                                              // (Optional) Name of the ADF-Config file to use
                ".\Dependencies_Test"                                   // (Optional) custom path for References which should be used during debugging. Default is the ADF Dependencies-folder
                );
            */
            #endregion

            #region Sample: Debug Custom Activity locally
            // To Execute and Debug a Custom Activity:
            env.ExecuteActivity(
                "DataDownloaderSamplePipeline",     // Name of the Pipeline
                "DownloadData",                     // Name of the Activity
                new DateTime(2017, 1, 1),           // SliceStart
                new DateTime(2017, 1, 3),           // SliceEnd
                new ADFConsoleLogger()              // (optional) ActivityLogger
                );
            #endregion

            #region Sample: ARM Export
            // To Export to an ARM-Template:
            env.ExportARMTemplate(
                @"..\..\..\MyARMTemplate\MyARMTemplate.deployproj",     // Path to the ARM Deployment Project file (absolute or relative)
                "North Europe",                                         // (optional) Region for the deployment, default is the Region of the ResourceGroup
                false,                                                  // (optional) can be used to avoid overwriting the Parameters file which is created during the first export
                true                                                    // (optional) deploy all Pipelines as with IsPaused=true
                );
                
            // This is the script that needs to be added to "Deploy-AzureResourceGroup.ps1" once right before "New-AzureRmResourceGroupDeployment"
            string postDeploymentScript = env.GetARMPostDeploymentScript();
            Console.WriteLine(postDeploymentScript);
            #endregion
        }
    }
}


