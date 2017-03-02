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
        Dictionary<string, LinkedService> _adfLinkedServices;
        Dictionary<string, Dataset> _adfDataSets;
        Dictionary<string, Pipeline> _adfPipelines;
        Dictionary<string, JObject> _adfConfigurations;
        Dictionary<string, FileInfo> _adfDependencies;

        Dictionary<string, JObject> _armFiles;
        #endregion
        #region Constructors
        public ADFLocalEnvironment(string projectFilePath, string configName)
        {
            LoadProjectFile(projectFilePath, configName);
        }
        public ADFLocalEnvironment(string projectFilePath) : this(projectFilePath, null) { }
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
        public void LoadProjectFile(string projectFilePath, string configName)
        {
            _configName = configName;

            _adfLinkedServices = new Dictionary<string, LinkedService>();
            _adfDataSets = new Dictionary<string, Dataset>();
            _adfPipelines = new Dictionary<string, Pipeline>();
            _adfConfigurations = new Dictionary<string, JObject>();
            _adfDependencies = new Dictionary<string, FileInfo>();
            _armFiles = new Dictionary<string, JObject>();

            _adfProject = new Project(projectFilePath);
            _projectName = new FileInfo(_adfProject.FullPath).Name.Replace(".dfproj", "");

            string schema;
            string adfType;
            string buildPath = string.Join("\\", AppDomain.CurrentDomain.BaseDirectory.GetTokens('\\', 0, 3, true));
            LinkedService tempLinkedService;
            Dataset tempDataset;
            Pipeline tempPipeline;

            for (int i = 0; i < 2; i++) // iterate twice, first to read config-files and second to read other files and apply the config directly
            {
                foreach (ProjectItem projItem in _adfProject.Items)
                {
                    if (projItem.ItemType.ToLower() == "script")
                    {
                        using (StreamReader file = File.OpenText(_adfProject.DirectoryPath + "\\" + projItem.EvaluatedInclude))
                        {
                            using (JsonTextReader reader = new JsonTextReader(file))
                            {
                                reader.DateParseHandling = DateParseHandling.None;
                                reader.DateTimeZoneHandling = DateTimeZoneHandling.RoundtripKind;
                                JObject jsonObj = (JObject)JToken.ReadFrom(reader);

                                if (jsonObj["$schema"] != null)
                                {
                                    schema = jsonObj["$schema"].ToString().ToLower();
                                    adfType = schema.Substring(schema.LastIndexOf("/") + 1);

                                    if (i == 0)
                                    {
                                        if (adfType == "microsoft.datafactory.config.json")
                                        {
                                            Console.WriteLine("Reading Config: " + projItem.EvaluatedInclude + " ...");
                                            _adfConfigurations.Add(projItem.EvaluatedInclude.Replace(".json", ""), jsonObj);
                                        }
                                    }
                                    else
                                    {
                                        Console.WriteLine("Reading Script: " + projItem.EvaluatedInclude + " ...");
                                        switch (adfType)
                                        {
                                            case "microsoft.datafactory.pipeline.json": // ADF Pipeline
                                                tempPipeline = (Pipeline)GetADFObjectFromJson(jsonObj, "Pipeline");
                                                _adfPipelines.Add(tempPipeline.Name, tempPipeline);
                                                _armFiles.Add(projItem.EvaluatedInclude, GetARMResourceFromJson(jsonObj, "datapipelines", tempPipeline));
                                                break;
                                            case "microsoft.datafactory.table.json": // ADF Table/Dataset
                                                tempDataset = (Dataset)GetADFObjectFromJson(jsonObj, "Dataset");
                                                _adfDataSets.Add(tempDataset.Name, tempDataset);
                                                _armFiles.Add(projItem.EvaluatedInclude, GetARMResourceFromJson(jsonObj, "datasets", tempDataset));
                                                break;
                                            case "microsoft.datafactory.linkedservice.json":
                                                tempLinkedService = (LinkedService)GetADFObjectFromJson(jsonObj, "LinkedService");
                                                _adfLinkedServices.Add(tempLinkedService.Name, tempLinkedService);
                                                _armFiles.Add(projItem.EvaluatedInclude, GetARMResourceFromJson(jsonObj, "linkedservices", tempLinkedService));
                                                break;
                                            case "microsoft.datafactory.config.json":
                                                break;
                                            default:
                                                Console.WriteLine("{0} does not seem to belong to any know ADF Json-Schema and is ignored!", projItem.EvaluatedInclude);
                                                break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // we iterate twice, in the FIRST loop we add the dependencies from the Dependencies-Folder
                    if (i == 0)
                    {
                        if (projItem.ItemType.ToLower() == "content")
                        {
                            if (projItem.EvaluatedInclude.ToLower().StartsWith("dependencies"))
                            {
                                _adfDependencies.Add(string.Join("\\", projItem.EvaluatedInclude.GetTokens('\\', 1, -1, false)), new FileInfo(_adfProject.DirectoryPath + "\\" + projItem.EvaluatedInclude));
                            }
                        }

                        if (projItem.ItemType.ToLower() == "projectreference")
                        {
                            Console.ForegroundColor = ConsoleColor.Yellow;
                            Console.WriteLine("A Project-Reference was found: {0} ! {1}The Code from the last build of the ADF project will be used ({2}). Make sure to rebuild the ADF project if it does not reflect your latest changes!", projItem.EvaluatedInclude, Environment.NewLine, buildPath);

                            if (!Directory.Exists(_adfProject.DirectoryPath + "\\" + buildPath))
                            {
                                throw new Exception(string.Format("The ADF project was not yet built into \"{0}\"! Make sure the Visual Studio Environments and OutputPaths are in Sync!", buildPath));
                            }
                            Console.ForegroundColor = ConsoleColor.White;
                        }
                    }

                    // we iterate twice, in the SECOND loop we add the dependencies from the Project-References which are zipped during the build of the ADF project
                    if (i == 1)
                    {
                        if (projItem.ItemType.ToLower() == "_outputpathitem")
                        {
                            string path = _adfProject.DirectoryPath + "\\" + projItem.EvaluatedInclude + "Dependencies";
                            foreach (string file in Directory.EnumerateFiles(path))
                            {
                                // Find ZIP files in the ADF output Dependencies directory
                                if (string.Equals(Path.GetExtension(file), ".zip", StringComparison.InvariantCultureIgnoreCase))
                                {
                                    // Dependencies from the Dependencies folder (added in first loop) overrule Project-References!
                                    if (!_adfDependencies.ContainsKey(Path.GetFileName(file)))
                                        _adfDependencies.Add(Path.GetFileName(file), new FileInfo(file));
                                }
                            }
                        }
                    }
                }
            }
        }
        public void LoadProjectFile(string projectFilePath)
        {
            LoadProjectFile(projectFilePath, null);
        }

        #region ARM Export
        public void ExportARMTemplate(string armProjectFilePath, string resourceLocation, bool overwriteParametersFile, bool pausePipelines)
        {
            Project armProject = new Project(armProjectFilePath);
            string outputFilePath = armProject.DirectoryPath + "\\AzureDataFactory.json";
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

        public void ExportARMTemplate(string armProjectFilePath, string resourceLocation, bool overwriteParametersFile)
        {
            ExportARMTemplate(armProjectFilePath, resourceLocation, overwriteParametersFile, false);
        }
        public void ExportARMTemplate(string armProjectFilePath, string resourceLocation)
        {
            ExportARMTemplate(armProjectFilePath, resourceLocation, false, false);
        }
        public void ExportARMTemplate(string armProjectFilePath)
        {
            ExportARMTemplate(armProjectFilePath, "[resourceGroup().location]");
        }
        public void ExportARMTemplate(string armProjectFilePath, bool overwriteParametersFile)
        {
            ExportARMTemplate(armProjectFilePath, "[resourceGroup().location]", overwriteParametersFile, false);
        }
        public JObject GetARMTemplate(string resourceLocation, bool pausePipelines)
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

            JArray resources = new JArray(_armFiles.Values);
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
        public JObject GetARMTemplate(string resourceLocation)
        {
            return GetARMTemplate(resourceLocation, false);
        }
        public JObject GetARMTemplate()
        {
            return GetARMTemplate("[resourceGroup().location]");
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

            DirectoryInfo d = new DirectoryInfo(armProject.DirectoryPath + "\\" + ARM_DEPENDENCY_FOLDER_NAME);
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
        public IDictionary<string, string> ExecuteActivity(string pipelineName, string activityName, DateTime sliceStart, DateTime sliceEnd, IActivityLogger activityLogger)
        {
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

            Pipeline pipeline = (Pipeline)GetADFObjectFromJson(MapSlices(_armFiles[Pipelines[pipelineName].Name + ".json"], sliceStart, sliceEnd), "Pipeline");
            Activity activityMeta = pipeline.GetActivityByName(activityName);

            // create a list of all Input- and Output-Datasets defined for the Activity
            List<Dataset> activityInputDatasets = _adfDataSets.Values.Where(adfDS => activityMeta.Inputs.Any(ds => adfDS.Name == ds.Name)).ToList();
            List<Dataset> activityDatasets = activityInputDatasets.Concat(_adfDataSets.Values.Where(adfDS => activityMeta.Outputs.Any(ds => adfDS.Name == ds.Name)).ToList()).ToList();

            List<LinkedService> activityLinkedServices = new List<LinkedService>();

            // apply the Slice-Settings to all relevant objects (Datasets and Activity)
            for (int i = 0; i < activityDatasets.Count; i++)
            {
                // MapSlices for the used Datasets
                activityDatasets[i] = (Dataset)GetADFObjectFromJson(MapSlices(_armFiles[activityDatasets[i].Name + ".json"], sliceStart, sliceEnd), "Dataset");

                // currently, as of 2017-01-25, the same LinkedService might get added multiple times if it is referenced by multiple datasets
                // this is the same behavior as if the activity was executed with ADF Service!!!
                activityLinkedServices.Add(_adfLinkedServices.Values.Single(x => x.Name == activityDatasets[i].Properties.LinkedServiceName));
            }

            DotNetActivity dotNetActivityMeta = (DotNetActivity)activityMeta.TypeProperties;

            FileInfo zipFile = _adfDependencies.Single(x => dotNetActivityMeta.PackageFile.EndsWith(x.Value.Name)).Value;
            UnzipFile(zipFile, dependencyPath);

            Assembly assembly = Assembly.LoadFrom(dependencyPath + "\\" + dotNetActivityMeta.AssemblyName);
            Type type = assembly.GetType(dotNetActivityMeta.EntryPoint);
            IDotNetActivity dotNetActivityExecute = Activator.CreateInstance(type) as IDotNetActivity;
           

            ret = (Dictionary<string, string>)dotNetActivityExecute.Execute(activityLinkedServices, activityDatasets, activityMeta, activityLogger);

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
        public IDictionary<string, string> ExecuteActivity(string pipelineName, string activityName, DateTime sliceStart, DateTime sliceEnd)
        {
            return ExecuteActivity(pipelineName, activityName, sliceStart, sliceEnd, new ADFConsoleLogger());
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
        private object GetADFObjectFromJson(JObject jsonObject, string objectType)
        {
            Type dynClass;
            MethodInfo dynMethod;

            ApplyConfiguration(ref jsonObject);

            dynClass = new Core.DataFactoryManagementClient().GetType();
            dynMethod = dynClass.GetMethod("DeserializeInternal" + objectType + "Json", BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static);
            var internalObject = dynMethod.Invoke(this, new object[] { jsonObject.ToString() });

            dynClass = Type.GetType(dynClass.AssemblyQualifiedName.Replace("Core.DataFactoryManagementClient", "Conversion." + objectType + "Converter"));
            ConstructorInfo constructor = dynClass.GetConstructor(Type.EmptyTypes);
            object classObject = constructor.Invoke(new object[] { });
            dynMethod = dynClass.GetMethod("ToWrapperType", BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Public);
            object ret = dynMethod.Invoke(classObject, new object[] { internalObject });

            return ret;
        }
        private JObject GetARMResourceFromJson(JObject jsonObject, string resourceType, object resource)
        {
            jsonObject["$schema"].Parent.Remove(); // remove the schema
            jsonObject.Add("type", resourceType.ToLower());
            jsonObject.Add("apiVersion", ARM_API_VERSION);

            // need to escape square brackets in Values as they are a special place-holder in ADF
            jsonObject = jsonObject.ReplaceInValues("[", "[[").ReplaceInValues("]", "]]");

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
            List<JToken> matches;
            string objectName = jsonObject["name"].ToString();

            foreach (JToken result in CurrentConfiguration.SelectTokens(string.Format("$.{0}.[*]", objectName)))
            {
                // try to select the token specified in the config in the file
                // this logic is necessary as the config might contain JSONPath wildcards
                matches = jsonObject.Root.SelectTokens(result["name"].ToString()).ToList();

                for(int i = 0; i< matches.Count; i++)
                {
                    ((JValue)matches[i]).Value = ((JValue)result["value"]).Value;
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
        private JObject MapSlices(JObject jsonObject, DateTime sliceStart, DateTime sliceEnd)
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
            return pipeline.Properties.Activities.Single(x => x.Name == activityName);
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


