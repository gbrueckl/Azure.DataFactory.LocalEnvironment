using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Azure.Management.DataFactories.Models;
using Core = Microsoft.Azure.Management.DataFactories.Core;
using CoreModels = Microsoft.Azure.Management.DataFactories.Core.Models;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Text.RegularExpressions;
using Microsoft.Build.Evaluation;
using System.Reflection;
using Microsoft.Azure.Management.DataFactories.Common.Models;
using ICSharpCode.SharpZipLib.Zip;
using ICSharpCode.SharpZipLib.Core;
using Microsoft.Azure.Management.DataFactories.Runtime;
using System.Configuration;
using System.Threading;
using System.Security.Policy;

namespace gbrueckl.Azure.DataFactory
{
    public class ADFLocalEnvironment
    {
        #region Constants
        const string ARM_API_VERSION = "2015-10-01";
        const string ARM_PROJECT_PARAMETER_NAME = "DataFactoryName";
        const string ARM_DEPENDENCY_FOLDER_NAME = "ADF_Dependencies";
        #endregion
        #region Private Variables
        Project _adfProject;
        string _projectName;
        string _configName;
        string _buildPath;
        Dictionary<string, LinkedService> _adfLinkedServices;
        Dictionary<string, Dataset> _adfDataSets;
        Dictionary<string, Pipeline> _adfPipelines;
        Dictionary<string, JObject> _adfConfigurations;
        Dictionary<string, FileInfo> _adfDependencies;

        Dictionary<string, JObject> _armFiles;
        #endregion
        #region Constructors

        /// <summary>
        /// Creates a new instance of ADFLocalEnvironment which can be used to debug Custom Activities locally or to create an Azure Resource Manager template from an existing Azure Data Factory VS project.
        /// </summary>
        /// <param name="projectFilePath">Absolute or relative path to the project file of your Azure Data Factory project (.dfproj). Relative paths start at the TargetOutputDirectory of the current project, e.g. ./bin/debug !</param>
        /// <param name="configName">(Optional) Name of the config file to use when setting up the ADF Local Environment. (e.g. MyConfig.json). Has to be part of the ADF project!</param>
        /// <param name="customActivitiesPath">(Optional) If your ADF project references any custom activities (.zip-files) which are located outside of the ADF /Dependencies/-folder, this folder can be specified here. This can be useful e.g. for debugging purposes where the .zip-file was not copied to ADF yet. This does not overwrite Project References. Relative paths start at the ADF Project-folder!</param>
        public ADFLocalEnvironment(string projectFilePath, string configName = null, string customActivitiesPath = null)
        {
            LoadProjectFile(projectFilePath, configName, customActivitiesPath);
        }

        #endregion
        #region Public Properties
        public Dictionary<string, LinkedService> LinkedServices
        {
            get
            {
                return _adfLinkedServices;
            }

            set
            {
                _adfLinkedServices = value;
            }
        }
        public Dictionary<string, Dataset> Datasets
        {
            get
            {
                return _adfDataSets;
            }

            set
            {
                _adfDataSets = value;
            }
        }
        public Dictionary<string, Pipeline> Pipelines
        {
            get
            {
                return _adfPipelines;
            }

            set
            {
                _adfPipelines = value;
            }
        }
        public Dictionary<string, JObject> Configurations
        {
            get
            {
                return _adfConfigurations;
            }

            set
            {
                _adfConfigurations = value;
            }
        }
        public string ConfigName
        {
            get
            {
                return _configName;
            }

            set
            {
                _configName = value;
            }
        }
        #endregion
        #region Public Functions
        /// <summary>
        /// Loads the settings from an ADF project into this ADFLocalEnvironment.
        /// </summary>
        /// <param name="projectFilePath">Absolute or relative path to the project file of your Azure Data Factory project (.dfproj)</param>
        /// <param name="configName">(Optional) Name of the config file to use when setting up the ADF Local Environment. (e.g. MyConfig.json)</param>
        /// <param name="customActivitiesPath">(Optional) If your ADF project references any custom assemblies, the code will be loaded from this location (e.g. \bin\debug). Note: you need to Build your ADF project in advance! Default is the path of the calling program!</param>
        public void LoadProjectFile(string projectFilePath, string configName = null, string customActivitiesPath = null)
        {
            if(!string.IsNullOrEmpty(configName))
                _configName = configName.Replace(".json", "");
            else
                _configName = null;

            _adfLinkedServices = new Dictionary<string, LinkedService>();
            _adfDataSets = new Dictionary<string, Dataset>();
            _adfPipelines = new Dictionary<string, Pipeline>();
            _adfConfigurations = new Dictionary<string, JObject>();
            _adfDependencies = new Dictionary<string, FileInfo>();
            _armFiles = new Dictionary<string, JObject>();

            
            _adfProject = new Project(projectFilePath, null, null, new ProjectCollection());
            _projectName = new FileInfo(_adfProject.FullPath).Name.Replace(".dfproj", "");

            string schema;
            string adfType;
            string projReferenceName;
            string dependencyPath;
            string dependencyName;
            string debuggerBuildPath;
            FileInfo dependencyFile;
            LinkedService tempLinkedService;
            Dataset tempDataset;
            Pipeline tempPipeline;
            JObject jsonObj = null;
            JObject _armFileTemp;

            if (customActivitiesPath == null)
            {
                _buildPath = Path.Combine(_adfProject.DirectoryPath, "Dependencies");
                //_buildPath = string.Join("\\", AppDomain.CurrentDomain.BaseDirectory.GetTokens('\\', 0, 3, true));
                Console.WriteLine("No custom Build Path for the ADF project was specified, using the one from the executing Program: '{0}'", _buildPath);
            }
            else
            {
                _buildPath = customActivitiesPath;
            }

            for (int i = 0; i < 2; i++) // iterate twice, first to read config-files and second to read other files and apply the config directly
            {
                foreach (ProjectItem projItem in _adfProject.Items)
                {
                    if (projItem.ItemType.ToLower() == "script")
                    {
                        using (StreamReader file = File.OpenText(Path.Combine(_adfProject.DirectoryPath, projItem.EvaluatedInclude)))
                        {
                            using (JsonTextReader reader = new JsonTextReader(file))
                            {
                                reader.DateParseHandling = DateParseHandling.None;
                                reader.DateTimeZoneHandling = DateTimeZoneHandling.RoundtripKind;
                                
                                try
                                {
                                    // check if the file is a valid JSON file
                                    jsonObj = (JObject)JToken.ReadFrom(reader);
                                }
                                catch(Exception eCast)
                                {
                                    throw new InvalidCastException("The file '" + projItem.EvaluatedInclude + "' could not be parsed as JSON file!", eCast);
                                }

                                if(i == 1)
                                    Console.Write("Reading ProjectItem: " + projItem.EvaluatedInclude + " ...");

                                if (jsonObj["$schema"] != null)
                                {
                                    schema = jsonObj["$schema"].ToString().ToLower();
                                    adfType = schema.Substring(schema.LastIndexOf("/") + 1);

                                    if (i == 0)
                                    {
                                        if (adfType == "microsoft.datafactory.config.json")
                                        {
                                            Console.ForegroundColor = ConsoleColor.Green;
                                            Console.WriteLine("Found a Config-File: " + projItem.EvaluatedInclude + " !");
                                            Console.ResetColor();
                                            _adfConfigurations.Add(projItem.EvaluatedInclude.Replace(".json", ""), jsonObj);
                                        }
                                    }
                                    else
                                    {
                                        switch (adfType)
                                        {
                                            case "microsoft.datafactory.pipeline.json": // ADF Pipeline
                                                tempPipeline = (Pipeline)GetADFObjectFromJson(jsonObj, "Pipeline");
                                                _adfPipelines.Add(tempPipeline.Name, tempPipeline);
                                                _armFiles.Add(tempPipeline.Name, GetARMResourceFromJson(jsonObj, "datapipelines", tempPipeline));
                                                Console.WriteLine(" (Pipeline)");
                                                break;
                                            case "microsoft.datafactory.table.json": // ADF Table/Dataset
                                                tempDataset = (Dataset)GetADFObjectFromJson(jsonObj, "Dataset");
                                                _adfDataSets.Add(tempDataset.Name, tempDataset);
                                                _armFiles.Add(tempDataset.Name, GetARMResourceFromJson(jsonObj, "datasets", tempDataset));
                                                Console.WriteLine(" (Table)");
                                                break;
                                            case "microsoft.datafactory.linkedservice.json":
                                                tempLinkedService = (LinkedService)GetADFObjectFromJson(jsonObj, "LinkedService");
                                                _adfLinkedServices.Add(tempLinkedService.Name, tempLinkedService);
                                                _armFiles.Add(tempLinkedService.Name, GetARMResourceFromJson(jsonObj, "linkedservices", tempLinkedService));
                                                Console.WriteLine(" (LinkedService)");
                                                break;
                                            case "microsoft.datafactory.config.json":
                                                Console.WriteLine(" (Config)");
                                                break;
                                            default:
                                                Console.ForegroundColor = ConsoleColor.Yellow;
                                                Console.WriteLine(" (NOT VALID)");
                                                Console.WriteLine("    {0} does not to belong to any know/valid ADF JSON-Schema and is ignored and the ADF Object will not be available!", projItem.EvaluatedInclude);
                                                Console.ResetColor();
                                                break;
                                        }
                                    }
                                }
                                else
                                {
                                    if (i == 1)
                                    {
                                        Console.WriteLine("");
                                        Console.ForegroundColor = ConsoleColor.Yellow;
                                        Console.Write("    {0} JSON Schema (\"$schema\"-tag) was not found! Parsing the object manually ...", projItem.EvaluatedInclude);
                                        Console.ResetColor();
                                        try
                                        {
                                            // try if the file can be parsed as DataSet
                                            tempDataset = (Dataset)GetADFObjectFromJson(jsonObj, "Dataset");
                                            if (tempDataset.Properties.Availability == null) // check if mandatory Availability-Property exists
                                                throw new InvalidCastException("Not a valid ADF Dataset-Definition");

                                            _adfDataSets.Add(tempDataset.Name, tempDataset);
                                            _armFiles.Add(tempDataset.Name, GetARMResourceFromJson(jsonObj, "datasets", tempDataset));
                                            Console.WriteLine(" (Dataset)");
                                        }
                                        catch (Exception e)
                                        {
                                            try
                                            {
                                                // try if the file can be parsed as Pipeline
                                                tempPipeline = (Pipeline)GetADFObjectFromJson(jsonObj, "Pipeline");
                                                if(tempPipeline.Properties.Description == null) // check if mandatory Description-Property exists
                                                    throw new InvalidCastException("Not a valid ADF Pipeline-Definition", e);

                                                _adfPipelines.Add(tempPipeline.Name, tempPipeline);
                                                _armFiles.Add(tempPipeline.Name, GetARMResourceFromJson(jsonObj, "datapipelines", tempPipeline));
                                                Console.WriteLine(" (Pipeline)");
                                            }
                                            catch (Exception e1)
                                            {
                                                tempLinkedService = (LinkedService)GetADFObjectFromJson(jsonObj, "LinkedService");
                                                if(tempLinkedService.Name == null || tempLinkedService.Properties == null)
                                                    throw new InvalidCastException("Not a valid ADF LinkedService-Definition", e1);

                                                _adfLinkedServices.Add(tempLinkedService.Name, tempLinkedService);
                                                _armFiles.Add(tempLinkedService.Name, GetARMResourceFromJson(jsonObj, "linkedservices", tempLinkedService));
                                                Console.WriteLine(" (LinkedService)");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // we iterate twice, in the FIRST loop we add the dependencies from the Dependencies-Folder (external Dependencies without any direkt link to ADF)
                    if (i == 0)
                    {
                        if (projItem.ItemType.ToLower() == "content")
                        {
                            if (projItem.EvaluatedInclude.ToLower().StartsWith("dependencies"))
                            {
                                dependencyName = string.Join("\\", projItem.EvaluatedInclude.GetTokens('\\', 1, -1, false));
                                //_adfDependencies.Add(string.Join("\\", projItem.EvaluatedInclude.GetTokens('\\', 1, -1, false)), new FileInfo(_adfProject.DirectoryPath + "\\" + projItem.EvaluatedInclude));
                                // might also consider using the ZIP form the latest build here?

                                if (_buildPath.StartsWith("."))
                                    dependencyFile = new FileInfo(Path.Combine(_adfProject.DirectoryPath, _buildPath, dependencyName));
                                else
                                    dependencyFile = new FileInfo(Path.Combine(_buildPath, dependencyName));

                                if (!dependencyFile.Exists)
                                    if (customActivitiesPath != null)
                                    {
                                        Console.ForegroundColor = ConsoleColor.Yellow;
                                        Console.Write("The ADF Dependency \"{0}\" could not be found at {1}. However, a Custom Activities Path was supplied which may target only a single Reference!", dependencyName, dependencyFile.FullName);
                                        Console.ResetColor();
                                    }
                                    else
                                        throw new FileNotFoundException(string.Format("The ADF Dependency \"{0}\" could not be found at {1}!", dependencyName, dependencyFile.FullName), dependencyFile.FullName);

                                _adfDependencies.Add(dependencyName, dependencyFile);
                            }
                        }

                        if (projItem.ItemType.ToLower() == "projectreference")
                        {
                            // Project-References are zipped into /Dependencies/<ReferenceName>.zip and then copied to the outputDirectory of the ADF project (e.g. /bin/debug/) 
                            // Check if the ADF project was built already
                            debuggerBuildPath = string.Join("\\", AppDomain.CurrentDomain.BaseDirectory.GetTokens('\\', 0, AppDomain.CurrentDomain.BaseDirectory.EndsWith("\\") ? 3 : 2, true));

                            if (!Directory.Exists(Path.Combine(_adfProject.DirectoryPath, debuggerBuildPath)))
                            {
                                throw new Exception(string.Format("The ADF project was not yet built into \"{0}\"! Make sure the OutputPaths of the ADF-Project and this Debugger-Project are in Sync!", debuggerBuildPath));
                            }
                            projReferenceName = projItem.DirectMetadata.Single(x => x.Name == "Name").EvaluatedValue;
                            if(!File.Exists(Path.Combine(_adfProject.DirectoryPath, debuggerBuildPath, "Dependencies", projReferenceName + ".zip")))
                            {
                                throw new Exception(string.Format("The output of the ProjectReference \"{0}\" was not found at {1}Dependencies\\{0}.zip! Make sure the ADF project was built in advance and the referenced Custom activity was copied correctly!", projReferenceName, Path.Combine(_adfProject.DirectoryPath, debuggerBuildPath)));
                            }
                        }
                    }

                    // we iterate twice, in the SECOND loop we add the dependencies from the Project-References which are zipped during the build of the ADF project
                    if (i == 1)
                    {
                        if (projItem.ItemType.ToLower() == "_outputpathitem")
                        {
                            dependencyPath = Path.Combine(_adfProject.DirectoryPath, projItem.EvaluatedInclude, "Dependencies");
                            if (Directory.Exists(dependencyPath))
                            {
                                foreach (string file in Directory.EnumerateFiles(dependencyPath))
                                {
                                    // Find ZIP files in the ADF output Dependencies directory
                                    if (string.Equals(Path.GetExtension(file), ".zip", StringComparison.InvariantCultureIgnoreCase))
                                    {
                                        // Dependencies from the Dependencies folder (added in first loop) overrule Project-References!
                                        if (!_adfDependencies.ContainsKey(Path.GetFileName(file)))
                                        {
                                            //Console.WriteLine("Adding Reference: " + file + " ...");
                                            _adfDependencies.Add(Path.GetFileName(file), new FileInfo(file));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine("The following Dependencies/References have been found: ");
            foreach(KeyValuePair<string, FileInfo> kvp in _adfDependencies)
            {           
                Console.WriteLine("'{0}' from path '{1}'", kvp.Key, kvp.Value.FullName);
            }
            Console.ForegroundColor = ConsoleColor.Gray;
        }

        #region ARM Export
        /// <summary>
        /// Export the current ADF local environment to an existing Azure Resource Manager project. It further incorporates the current configuration file (if specified) and handles the dependencies (need to adopt the PowerShell Script manually, see <seealso cref="GetARMPostDeploymentScript"/> for details)
        /// </summary>
        /// <param name="armProjectFilePath">Absolute or relative path to the project file of your Azure Resource Manager Template Deployment project (.deployproj)</param>
        /// <param name="resourceLocation">(Optional) The location where the resources should be deployed to. Default is '[resourceGroup().location]' being the location of the resource group which the template is deployed to</param>
        /// <param name="overwriteParametersFile">(Optional) Specifies whether the parameter (gets generated during first execution) should be overwritten. Default is 'false'.</param>
        /// <param name="pausePipelines">(Optional) Set the "isPaused" property of all to 'true'. Default is 'false'.</param>
        public void ExportARMTemplate(string armProjectFilePath, string resourceLocation = "[resourceGroup().location]", bool overwriteParametersFile = false, bool pausePipelines = false)
        {
            Project armProject = new Project(armProjectFilePath, null, null, new ProjectCollection());
            string outputFilePath = Path.Combine(armProject.DirectoryPath, "AzureDataFactory.json");
            JObject armTemplate = GetARMTemplate(resourceLocation, pausePipelines);

            // serialize JSON to our ARM file
            using (StreamWriter file = File.CreateText(outputFilePath))
            {
                file.Write(JsonConvert.SerializeObject(armTemplate, new JsonSerializerSettings { DateTimeZoneHandling = DateTimeZoneHandling.RoundtripKind }));
            }


            outputFilePath = outputFilePath.Replace(".json", ".parameters.json");

            // create/overwrite the parametersFile if it does not exist yet or it is explicitly specified
            if (overwriteParametersFile || !File.Exists(outputFilePath))
            {
                // write our ARM parameters file
                using (StreamWriter file = File.CreateText(outputFilePath))
                {
                    file.Write(@"{{
    ""$schema"": ""https://schema.management.azure.com/schemas/2015-01-01/deploymentParameters.json#"",
    ""contentVersion"": ""1.0.0.0"",
    ""parameters"": {{
        ""DataFactoryName"": {{ ""value"": ""{0}"" }}
    }}
}}", _projectName);
                }
            }

            CopyADFDependenciesToARM(armProject);
        }
        /// <summary>
        /// Export the current ADF local environment to an existing Azure Resource Manager project. It further incorporates the current configuration file (if specified) and handles the dependencies (need to adopt the PowerShell Script manually!)
        /// </summary>
        /// <param name="armProjectFilePath">Absolute or relative path to the project file of your Azure Resource Manager Template Deployment project (.deployproj)</param>
        /// <param name="overwriteParametersFile">(Optional) Specifies whether the parameter (gets generated during first execution) should be overwritten. Default is 'false'.</param>
        public void ExportARMTemplate(string armProjectFilePath, bool overwriteParametersFile)
        {
            ExportARMTemplate(armProjectFilePath, "[resourceGroup().location]", overwriteParametersFile, false);
        }
        /// <summary>
        /// Returns the final ARM Template as an JObject. Can be used to further modify the JSON and write it manually.
        /// </summary>
        /// <param name="resourceLocation">(Optional) The location where the resources should be deployed to. Default is '[resourceGroup().location]' being the location of the resource group which the template is deployed to</param>
        /// <param name="pausePipelines">(Optional) Set the "isPaused" property of all to 'true'. Default is 'false'.</param>
        /// <returns>A JObject representing the final ARM template</returns>
        public JObject GetARMTemplate(string resourceLocation = "[resourceGroup().location]", bool pausePipelines = false)
        {
            JObject ret = new JObject();
            JObject parameters = new JObject();
            JObject tempJObject1 = new JObject();

            ret.Add("$schema", "http://schema.management.azure.com/schemas/2015-01-01/deploymentTemplate.json#");
            ret.Add("contentVersion", "1.0.0.0");

            tempJObject1.Add("type", "string");
            tempJObject1.Add("defaultValue", _projectName);
            tempJObject1.Add("minLength", 3);
            tempJObject1.Add("maxLength", 30);

            parameters.Add(ARM_PROJECT_PARAMETER_NAME, tempJObject1);
            ret.Add("parameters", parameters);

            JObject dataFactory = new JObject();
            dataFactory.Add("name", "[parameters('" + ARM_PROJECT_PARAMETER_NAME + "')]");
            dataFactory.Add("apiVersion", ARM_API_VERSION);
            dataFactory.Add("type", "Microsoft.DataFactory/datafactories");
            dataFactory.Add("location", resourceLocation);

            JArray resources = new JArray();
            foreach(string key in _armFiles.Keys)
            {
                // need to escape square brackets in Values as they are a special place-holder in ADF
                resources.Add(_armFiles[key].ReplaceInValues("[", "[[").ReplaceInValues("]", "]]"));
            }
            dataFactory.Add("resources", resources);

            resources = new JArray();
            resources.Add(dataFactory);

            ret.Add("resources", resources);

            if (pausePipelines)
            {
                JToken properties;
                JToken isPaused;
                // the JPath query is case-sensitive! this is OK here as we set all the values above
                List<JToken> pipelines = ret.SelectTokens("$.resources[?(@.type=='Microsoft.DataFactory/datafactories')].resources[?(@.type=='datapipelines')]").ToList();

                foreach (JObject pipeline in pipelines)
                {
                    if (pipeline.TryGetValue("properties", StringComparison.InvariantCultureIgnoreCase, out properties))
                    {
                        if (((JObject)properties).TryGetValue("isPaused", StringComparison.InvariantCultureIgnoreCase, out isPaused))
                        {
                            ((JValue)isPaused).Value = true;
                        }
                        else
                        {
                            properties["isPaused"] = true;
                        }
                    }
                }
            }
            return ret;
        }

        private void CopyADFDependenciesToARM(Project armProject)
        {
            Dictionary<LinkedService, Dictionary<string, FileInfo>> dependenciesToUploade = new Dictionary<LinkedService, Dictionary<string, FileInfo>>();
            DotNetActivity dotNetActivity;
            LinkedService linkedService;
            AzureStorageLinkedService azureBlob;
            Dictionary<string, FileInfo> tempList;
            string accountName;
            string containerName;
            string filePath;
            FileInfo targetFile;

            foreach (Pipeline pipeline in Pipelines.Values)
            {
                foreach (Activity activity in pipeline.Properties.Activities)
                {
                    if (activity.TypeProperties is DotNetActivity)
                    {
                        dotNetActivity = (DotNetActivity)activity.TypeProperties;
                        linkedService = LinkedServices[dotNetActivity.PackageLinkedService];

                        if (!dependenciesToUploade.ContainsKey(linkedService))
                        {
                            dependenciesToUploade.Add(linkedService, new Dictionary<string, FileInfo>());
                        }

                        tempList = dependenciesToUploade[linkedService];

                        if (!tempList.ContainsKey(dotNetActivity.PackageFile))
                        {
                            dependenciesToUploade[linkedService].Add(dotNetActivity.PackageFile, _adfDependencies.Single(x => dotNetActivity.PackageFile.EndsWith(x.Key.Replace("Dependencies\\", ""))).Value);
                        }
                    }
                }
            }

            DirectoryInfo d = new DirectoryInfo(Path.Combine(armProject.DirectoryPath, ARM_DEPENDENCY_FOLDER_NAME));
            // delete any old/existing dependencies in ARM output folder
            if (d.Exists)
            {
                // there is sometimes a temporary lock on the files/folder when we try to delete it ?!?
                try
                {
                    d.Delete(true);
                }
                catch (Exception e)
                {
                    Thread.Sleep(3000);
                    d.Delete(true);
                }
            }


            foreach (KeyValuePair<LinkedService, Dictionary<string, FileInfo>> kvp in dependenciesToUploade)
            {
                if (!(kvp.Key.Properties.TypeProperties is AzureStorageLinkedService))
                {
                    throw new Exception("Only AzureStorageLinkedServices are supported at the moment!");
                }

                azureBlob = (AzureStorageLinkedService)kvp.Key.Properties.TypeProperties;
                Match match = Regex.Match(azureBlob.ConnectionString, @"[\^;]AccountName=(.*)[;$]");

                accountName = match.Groups[1].Value;

                foreach (KeyValuePair<string, FileInfo> dependency in kvp.Value)
                {
                    containerName = dependency.Key.Substring(0, dependency.Key.IndexOf('/'));
                    filePath = dependency.Key.Replace(containerName + "/", "").Replace("/", "\\").TrimEnd('\\');
                    targetFile = new FileInfo(string.Format("{0}\\{1}\\{2}\\{3}\\{4}", armProject.DirectoryPath, ARM_DEPENDENCY_FOLDER_NAME, accountName, containerName, filePath));

                    targetFile.Directory.Create();

                    dependency.Value.CopyTo(targetFile.FullName, true);
                }
            }
        }
        /// <summary>
        /// Returns the PowerShell snippet which has to be used to upload ADF dependencies during the ARM Deployment.
        /// </summary>
        /// <returns>A PowerShell snippet</returns>
        public string GetARMPostDeploymentScript()
        {
            string ret = @"
#region ADF Dependency Upload
#the following code must be placed right before the New-AzureRmResourceGroupDeployment command in the Deploy-AzureResourceGroup.ps1 script of your ARM Template
#please check for unwanted linebreaks in case you copied the script!
$dependencyFolder = ""$PSScriptRoot\" + ARM_DEPENDENCY_FOLDER_NAME + @"\""
Write-Host ""Uploading ADF Dependencies from $dependencyFolder ...""

foreach ($file in Get-ChildItem -Recurse -File -Path $dependencyFolder)
{
	$matches = [regex]::Match($file.FullName.Substring($dependencyFolder.Length), ""^([^\\]+)\\([^\\]+)\\(.+)$"")
	$StorageAccountName = $matches.Groups[1]
	$ContainerName = $matches.Groups[2]
	$BlobName = $matches.Groups[3]


    Write-Host ""Uploading ADF-Dependency [$BlobName] ..."" -NoNewline

	$StorageAccount = (Get-AzureRmStorageAccount | Where-Object{$_.StorageAccountName -eq $StorageAccountName})
	# Copy files from the local storage staging location to the storage account container
	# Create Container if not exists, use previously set $StorageAccount
	$container = New-AzureStorageContainer -Name $ContainerName -Context $StorageAccount.Context -ErrorAction SilentlyContinue *>&1
	$blob = Set-AzureStorageBlobContent -File $file.FullName -Blob $BlobName -Container $ContainerName -Context $StorageAccount.Context -Force

    Write-Host ""Done!"" -ForegroundColor Green
}
Start-Sleep -s 10
Write-Host ""Finished uploading all ADF Dependencies from $dependencyFolder !"" -ForegroundColor Green
#endregion
# followed by New-AzureRmResourceGroupDeployment ...
";

            return ret;
        }
        #endregion
        #region Custom Activity Debugger
        /// <summary>
        /// Starts an existing custom C# activity locally and enables local debugging. You need to set a breakpoint in your custom component's code.
        /// </summary>
        /// <param name="pipelineName">The name of the pipeline which contains the custom C# activity</param>
        /// <param name="activityName">The name of the activity which you want to debug</param>
        /// <param name="sliceStart">Value to be used for debugging when referencing <SliceStart> in the ADF code</param>
        /// <param name="sliceEnd">Value to be used for debugging when referencing <SliceEnd> in the ADF code</param>
        /// <param name="activityLogger">Allows you to specify a custom Activity Logger to do your logging. Default is a Console Logger.</param>
        /// <param name="windowStart">Value to be used for debugging when referencing <WindowStart> in the ADF code</param>
        /// <param name="windowEnd">Value to be used for debugging when referencing <WindowEnd> in the ADF code</param>
        /// <returns></returns>
        public IDictionary<string, string> ExecuteActivity(string pipelineName, string activityName, DateTime sliceStart, DateTime sliceEnd, IActivityLogger activityLogger, DateTime? windowStart = null, DateTime? windowEnd = null)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("Debugging Custom Activity '{0}' from Pipeline '{1}' ...", activityName, pipelineName);
            Console.WriteLine("The Code from the last build of the ADF project will be used ({0}). Make sure to rebuild the ADF project if it does not reflect your latest changes!", _buildPath);

            Dictionary<string, string> ret = null;
            string dependencyPath = Path.Combine(Environment.CurrentDirectory, "CustomActivityDependencies_TEMP");

            if (Directory.Exists(dependencyPath))
            {
                try
                {
                    // it might happen that two activities are executed in the same run and the directory is blocked
                    // so we need to catch the exception and continue with our execution
                    // the folder might not be cleaned up properly in this case but during the execution of the first activity it will
                    Directory.Delete(dependencyPath, true);
                }
                catch (UnauthorizedAccessException e) { }
            }

            if(!Pipelines.ContainsKey(pipelineName))
                throw new KeyNotFoundException(string.Format("A pipeline with the name \"{0}\" was not found. Please check the spelling and make sure it was loaded correctly in the ADF Local Environment and see the console output", pipelineName));
            // don not apply Configuration again for GetADFObjectFromJson as this would overwrite changes done by MapSlices!!!
            Pipeline pipeline = (Pipeline)GetADFObjectFromJson(MapSlices(_armFiles[Pipelines[pipelineName].Name], sliceStart, sliceEnd, windowStart, windowEnd), "Pipeline", false);

            Activity activityMeta = pipeline.GetActivityByName(activityName);

            // create a list of all Input- and Output-Datasets defined for the Activity
            List<Dataset> activityInputDatasets = _adfDataSets.Values.Where(adfDS => activityMeta.Inputs.Any(ds => adfDS.Name == ds.Name)).ToList();
            List<Dataset> activityOutputDatasets = _adfDataSets.Values.Where(adfDS => activityMeta.Outputs.Any(ds => adfDS.Name == ds.Name)).ToList();
            List<Dataset> activityAllDatasets = activityInputDatasets.Concat(activityOutputDatasets).ToList();

            List<LinkedService> activityLinkedServices = new List<LinkedService>();

            // apply the Slice-Settings to all relevant objects (Datasets and Activity)
            for (int i = 0; i < activityAllDatasets.Count; i++)
            {
                // MapSlices for the used Datasets
                activityAllDatasets[i] = (Dataset)GetADFObjectFromJson(MapSlices(_armFiles[activityAllDatasets[i].Name], sliceStart, sliceEnd, windowStart, windowEnd), "Dataset", false);

                // currently, as of 2017-01-25, the same LinkedService might get added multiple times if it is referenced by multiple datasets
                // this is the same behavior as if the activity was executed with ADF Service!!!
                activityLinkedServices.Add(_adfLinkedServices.Values.Single(x => x.Name == activityAllDatasets[i].Properties.LinkedServiceName));
            }

            DotNetActivity dotNetActivityMeta = (DotNetActivity)activityMeta.TypeProperties;

            Console.WriteLine("The Custom Activity refers to the following ZIP-file: '{0}'", dotNetActivityMeta.PackageFile);
            FileInfo zipFile = _adfDependencies.Single(x => dotNetActivityMeta.PackageFile.EndsWith(x.Value.Name)).Value;
            Console.WriteLine("Using '{0}' from ZIP-file '{1}'!", dotNetActivityMeta.AssemblyName, zipFile.FullName);
            UnzipFile(zipFile, dependencyPath);
            //dependencyPath = _buildPath;
            Assembly assembly = Assembly.LoadFrom(Path.Combine(dependencyPath, dotNetActivityMeta.AssemblyName));
            Type type = assembly.GetType(dotNetActivityMeta.EntryPoint);
            IDotNetActivity dotNetActivityExecute = Activator.CreateInstance(type) as IDotNetActivity;

            Console.WriteLine("Executing Function '{0}'...{1}--------------------------------------------------------------------------", dotNetActivityMeta.EntryPoint, Environment.NewLine);
            Console.ForegroundColor = ConsoleColor.Gray;

            ret = (Dictionary<string, string>)dotNetActivityExecute.Execute(activityLinkedServices, activityAllDatasets, activityMeta, activityLogger);

            if (Directory.Exists(dependencyPath))
            {
                try
                {
                    // This might fail as the DLL is still loaded in the current Application Domain
                    Directory.Delete(dependencyPath, true);
                }
                catch (UnauthorizedAccessException e) { }
            }

            return ret;
        }
        /// <summary>
        /// Starts an existing custom C# activity locally and enables local debugging. You need to set a breakpoint in your custom component's code. Uses a simple Console Logger for the outputs.
        /// </summary>
        /// <param name="pipelineName">The name of the pipeline which contains the custom C# activity</param>
        /// <param name="activityName">The name of the activity which you want to debug</param>
        /// <param name="sliceStart">Value to be used for debugging when referencing <SliceStart> in the ADF code</param>
        /// <param name="sliceEnd">Value to be used for debugging when referencing <SliceEnd> in the ADF code</param>
        /// <param name="windowStart">Value to be used for debugging when referencing <WindowStart> in the ADF code</param>
        /// <param name="windowEnd">Value to be used for debugging when referencing <WindowEnd> in the ADF code</param>
        /// <returns></returns>
        public IDictionary<string, string> ExecuteActivity(string pipelineName, string activityName, DateTime sliceStart, DateTime sliceEnd, DateTime? windowStart = null, DateTime? windowEnd = null)
        {
            return ExecuteActivity(pipelineName, activityName, sliceStart, sliceEnd, new ADFConsoleLogger(), windowStart, windowEnd);
        }
        #endregion
        #endregion
        #region Private Functions
        private JObject CurrentConfiguration
        {
            get
            {
                if (_configName == null)
                    return null;
                return _adfConfigurations[_configName];
            }
        }
        private object GetADFObjectFromJson(JObject jsonObject, string objectType, bool applyConfiguration = true)
        {
            Type dynClass;
            MethodInfo dynMethod;
            object ret = null;

            if (applyConfiguration)
                ApplyConfiguration(ref jsonObject);

            try
            {
                dynClass = new Core.DataFactoryManagementClient().GetType();
                dynMethod = dynClass.GetMethod("DeserializeInternal" + objectType + "Json", BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static);
                var internalObject = dynMethod.Invoke(this, new object[] { jsonObject.ToString() });

                dynClass = Type.GetType(dynClass.AssemblyQualifiedName.Replace("Core.DataFactoryManagementClient", "Conversion." + objectType + "Converter"));
                ConstructorInfo constructor = dynClass.GetConstructor(Type.EmptyTypes);
                object classObject = constructor.Invoke(new object[] { });
                dynMethod = dynClass.GetMethod("ToWrapperType", BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public | BindingFlags.GetField | BindingFlags.GetProperty);
                ret = dynMethod.Invoke(classObject, new object[] { internalObject });
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
            
            return ret;
        }
        private JObject GetARMResourceFromJson(JObject jsonObject, string resourceType, object resource)
        {
            if(jsonObject["$schema"] != null)
                jsonObject["$schema"].Parent.Remove(); // remove the schema
            jsonObject.Add("type", resourceType.ToLower());
            jsonObject.Add("apiVersion", ARM_API_VERSION);

            JArray dependsOn = new JArray();
            dependsOn.Add("[parameters('" + ARM_PROJECT_PARAMETER_NAME + "')]");

            switch (resourceType.ToLower())
            {
                case "datapipelines": // for pipelines also add dependencies to all Input and Output-Datasets
                    foreach (Activity act in ((Pipeline)resource).Properties.Activities)
                    {
                        foreach (ActivityInput actInput in act.Inputs)
                            dependsOn.Add(actInput.Name);

                        foreach (ActivityOutput actOutput in act.Outputs)
                            dependsOn.Add(actOutput.Name);
                    }
                    break;
                case "datasets": // for Datasets also add a dependency to the LinkedService
                    Dataset ds = (Dataset)resource;
                    dependsOn.Add(ds.Properties.LinkedServiceName);
                    break;
                case "linkedservices": // LinkedServices like Batch or HDInsight might depend on other LinkedServices
                    LinkedService ls = (LinkedService)resource;

                    Regex regex = new Regex(@"""linkedservicename""\s*:\s*""([^""]+)""", RegexOptions.IgnoreCase);
                    Match m = regex.Match(jsonObject.ToString());

                    if (m.Success)
                    {
                        dependsOn.Add(m.Groups[1].Value);
                    }
                    break;
                default:
                    Console.WriteLine("ResourceType {0} is not supporeted!", resourceType);
                    break;
            }

            jsonObject.Add("dependsOn", dependsOn);

            return jsonObject;
        }
        private void ApplyConfiguration(ref JObject jsonObject)
        {
            if (CurrentConfiguration == null)
                return;

            List<JToken> matches;
            string objectName = jsonObject["name"].ToString();

            foreach (JToken result in CurrentConfiguration.SelectTokens(string.Format("$.{0}.[*]", objectName)))
            {
                // try to select the token specified in the config in the file
                // this logic is necessary as the config might contain JSONPath wildcards
                matches = jsonObject.Root.SelectTokens(result["name"].ToString()).ToList();

                for(int i = 0; i< matches.Count; i++)
                {
                    if (matches[i] is JValue)
                    {
                        ((JValue)matches[i]).Value = ((JValue)result["value"]).Value;
                    }
                    else if(matches[i] is JObject)
                    {
                        ((JProperty)(matches[i].Parent)).Value = result["value"].DeepClone();
                    }
                }
            }
        }
        private DirectoryInfo UnzipFile(FileInfo zipFileInfo, string localFolder)
        {
            Directory.CreateDirectory(localFolder);
            string outputFileName = string.Empty;

            ZipFile zipFile = new ZipFile(zipFileInfo.FullName);

            foreach (ZipEntry zipEntry in zipFile)
            {
                if (!zipEntry.IsFile)
                {
                    continue;           // Ignore directories
                }
                String entryFileName = zipEntry.Name;
                // to remove the folder from the entry:- entryFileName = Path.GetFileName(entryFileName);
                // Optionally match entrynames against a selection list here to skip as desired.
                // The unpacked length is available in the zipEntry.Size property.

                byte[] buffer = new byte[4096];     // 4K is optimum
                Stream zipStream = zipFile.GetInputStream(zipEntry);

                // Manipulate the output filename here as desired.
                outputFileName = Path.Combine(localFolder, entryFileName);
                string directoryName = Path.GetDirectoryName(outputFileName);
                if (directoryName.Length > 0)
                    Directory.CreateDirectory(directoryName);

                // Unzip file in buffered chunks. This is just as fast as unpacking to a buffer the full size
                // of the file, but does not waste memory.
                // The "using" will close the stream even if an exception occurs.
                using (FileStream streamWriter = File.Create(outputFileName))
                {
                    StreamUtils.Copy(zipStream, streamWriter, buffer);
                }
            }

            zipFile.Close();

            return new DirectoryInfo(localFolder);
        }
        private JObject MapSlices(JObject jsonObject, DateTime sliceStart, DateTime sliceEnd, DateTime? windowStart = null, DateTime? windowEnd = null)
        {
            JProperty jProp;
            string objectName = jsonObject["name"].ToString();

            Regex regex = new Regex(@"^\$\$Text.Format\('(.*)',(.*)\)");

            string oldText;
            string newText;
            Dictionary<string, DateTime> dateValues = new Dictionary<string, DateTime>(2);
            Dictionary<string, string> partitionBy = new Dictionary<string, string>(); ;

            dateValues.Add("SliceStart", sliceStart);
            dateValues.Add("SliceEnd", sliceEnd);
            Console.ForegroundColor = ConsoleColor.Yellow;
            if (windowStart.HasValue)
            {
                dateValues.Add("WindowStart", windowStart.Value);
            }
            else
            {
                Console.WriteLine("TimeValue for 'WindowStart' was not set explicitly - using 'SliceStart' instead!");
                dateValues.Add("WindowStart", sliceStart);
            }
            if (windowEnd.HasValue)
            {
                dateValues.Add("WindowEnd", windowEnd.Value);
            }
            else
            {
                Console.WriteLine("TimeValue for 'WindowEnd' was not set explicitly - using 'SliceEnd' instead!");
                dateValues.Add("WindowEnd", sliceEnd);
            }
            Console.ForegroundColor = ConsoleColor.Gray;

            foreach (JToken jToken in jsonObject.Descendants())
            {
                if (jToken is JProperty)
                {
                    jProp = (JProperty)jToken;

                    // map all Values that are like "$$Text.Format(..., SliceStart)"
                    if (jProp.Value is JValue)
                    {
                        if(jProp.Value.ToString().StartsWith("$$"))
                        {
                            jProp.Value = new JValue((string)ADFFunctionResolver.ParseFunctionText(jProp.Value.ToString(), dateValues));
                        }
                    }

                    // map all Values that have a partitionedBy clause
                    if (jProp.Name.ToLower() == "partitionedby")
                    {
                        partitionBy = new Dictionary<string, string>();
                        foreach (JToken part in jProp.Value)
                        {
                            oldText = "{" + part["name"] + "}";

                            // dont know if PartitionedBy even supports custom functions, anyway, it would work this way
                            newText = (string)ADFFunctionResolver.ParseFunctionText("$$Text.Format('{0:" + part["value"]["format"].ToString().TrimStart('$') + "}', " + part["value"]["date"].ToString().TrimStart('$') + ")", dateValues);

                            partitionBy.Add(oldText, newText);
                        }
                    }
                }
            }

            string newObjectJson = jsonObject.ToString();

            foreach (KeyValuePair<string, string> kvp in partitionBy)
            {
                newObjectJson = newObjectJson.Replace(kvp.Key, kvp.Value);
            }

            return JObject.Parse(newObjectJson);
        }
        #endregion
    }

    public static class CustomExtensions
    {
        public static Activity GetActivityByName(this Pipeline pipeline, string activityName)
        {
            try
            {
                return pipeline.Properties.Activities.Single(x => x.Name == activityName);
            }
            catch(Exception e)
            {
                throw new KeyNotFoundException(string.Format("The activity \"{0}\" was not found in pipeline \"{1}\". Please check the spelling and make sure it was loaded correctly in the ADF Local Environment and see the console output", activityName, pipeline.Name), e);
            }
        }

        public static JObject ReplaceInValues(this JObject jObject, string search, string replaceWith)
        {
            JProperty jProp;

            foreach (JToken jToken in jObject.Descendants())
            {
                if (jToken is JProperty)
                {
                    jProp = (JProperty)jToken;

                    if (jProp.Value is JValue)
                    {
                        jProp.Value = jProp.Value.ToString().Replace(search, replaceWith);
                    }
                }
            }

            return jObject;
        }

        public static List<string> GetTokens(this string text, char delimiter, int start, int count, bool reverse)
        {
            List<string> ret = new List<string>();

            ret = text.Split(delimiter).ToList();

            if (reverse)
                ret.Reverse();

            if (start > 0)
                ret.RemoveRange(0, start);

            if (count > 0)
                ret = ret.GetRange(0, count);

            if (reverse)
                ret.Reverse();

            return ret;
        }
    }
}


