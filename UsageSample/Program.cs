using Azure.DataFactory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Azure.Management.DataFactories.Models;
using Core = Microsoft.Azure.Management.DataFactories.Core;
using CoreModels = Microsoft.Azure.Management.DataFactories.Core.Models;

// Namespace of ADF Custom Activity
using DataDownloaderActivityNS;

namespace UsageSample
{
    class Program
    {
        static void Main(string[] args)
        {
            string path = @"..\\..\\..\\\MyADFProject\MyADFProject.dfproj";

            ADFLocalEnvironment env = new ADFLocalEnvironment(path, "MyConfig");

            env.ExportARMTemplate("..\\..\\..\\Sample_ARMExport\\AzureDataFactory.json");

            
        }
    }
}
