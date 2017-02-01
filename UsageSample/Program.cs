using Azure.DataFactory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Azure.Management.DataFactories.Models;
using Core = Microsoft.Azure.Management.DataFactories.Core;
using CoreModels = Microsoft.Azure.Management.DataFactories.Core.Models;

namespace UsageSample
{
    class Program
    {
        static void Main(string[] args)
        {
            string path = @"..\\..\\..\\\MyADFProject\MyADFProject.dfproj";

            ADFLocalEnvironment env = new ADFLocalEnvironment(path, "MyPrivateConfig");

            // To Export to an ARM-Template:
            env.ExportARMTemplate("..\\..\\..\\MyARMTemplate\\MyARMTemplate.deployproj", "North Europe");
            string postDeploymentScript = env.GetARMPostDeploymentScript();

            // To Execute and Debug a Custom Activity:
            env.ExecuteActivity("DataDownloaderSamplePipeline", "DownloadData", new DateTime(2017, 1, 1), new DateTime(2017, 1, 3));

        }
    }
}
