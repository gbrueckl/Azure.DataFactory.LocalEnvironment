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

namespace Azure.DataFactory
{
    public class ADFLocalEnvironment
    {
        #region Constants
        const string ARM_API_VERSION = "2015-10-01";
        const string ARM_PROJECT_PARAMETER_NAME = "DataFactoryName";
        #endregion
        #region Private Variables
        Project _project;
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
        public ADFLocalEnvironment(string projectFilePath) : this(projectFilePath, null){ }
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

            _project = new Project(projectFilePath);
            _projectName = new FileInfo(_project.FullPath).Name.Replace(".dfproj", "");

            string schema;
            string adfType;
            LinkedService tempLinkedService;
            Dataset tempDataset;
            Pipeline tempPipeline;

            for(int i = 0; i < 2; i++) // iterate twice, first to read config-files and second to read other files and apply the config directly
            {
                foreach (ProjectItem projItem in _project.Items)
                {
                    if (projItem.ItemType == "Script")
                    {
                        using (StreamReader file = File.OpenText(_project.DirectoryPath + "\\" + projItem.EvaluatedInclude))
                        {
                            using (JsonTextReader reader = new JsonTextReader(file))
                            {
                                JObject jsonObj = (JObject)JToken.ReadFrom(reader);

                                if (jsonObj["$schema"] != null)
                                {
                                    schema = jsonObj["$schema"].ToString();
                                    adfType = schema.Substring(schema.LastIndexOf("/") + 1);

                                    if (i == 0)
                                    {
                                        if (adfType == "Microsoft.DataFactory.Config.json")
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
                                            case "Microsoft.DataFactory.Pipeline.json": // ADF Pipeline
                                                tempPipeline = (Pipeline)GetADFObjectFromJson(jsonObj, "Pipeline");
                                                _adfPipelines.Add(tempPipeline.Name, tempPipeline);
                                                _armFiles.Add(projItem.EvaluatedInclude, GetARMResourceFromJson(jsonObj, "datapipelines", tempPipeline));
                                                break;
                                            case "Microsoft.DataFactory.Table.json": // ADF Table/Dataset
                                                tempDataset = (Dataset)GetADFObjectFromJson(jsonObj, "Dataset");
                                                _adfDataSets.Add(tempDataset.Name, tempDataset);
                                                _armFiles.Add(projItem.EvaluatedInclude, GetARMResourceFromJson(jsonObj, "datasets", tempDataset));
                                                break;
                                            case "Microsoft.DataFactory.LinkedService.json":
                                                tempLinkedService = (LinkedService)GetADFObjectFromJson(jsonObj, "LinkedService");
                                                _adfLinkedServices.Add(tempLinkedService.Name, tempLinkedService);
                                                _armFiles.Add(projItem.EvaluatedInclude, GetARMResourceFromJson(jsonObj, "linkedservices", tempLinkedService));
                                                break;
                                            case "Microsoft.DataFactory.Config.json":
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
                    if(i == 0 && projItem.ItemType == "Content") // Dependencies
                    {
                        _adfDependencies.Add(projItem.EvaluatedInclude, new FileInfo(_project.DirectoryPath + "\\" + projItem.EvaluatedInclude));
                    }
                }
            }
        }
        public void LoadProjectFile(string projectFilePath)
        {
            LoadProjectFile(projectFilePath, null);
        }

        public void ExportARMTemplate(string outputFilePath)
        {
            JObject armTemplate = GetARMTemplate();

            // serialize JSON directly to a file
            using (StreamWriter file = File.CreateText(outputFilePath))
            {
                JsonSerializer serializer = new JsonSerializer();
                serializer.Serialize(file, armTemplate);
            }
        }
        public JObject GetARMTemplate()
        {
            JObject ret = new JObject();
            JObject parameters = new JObject();
            JObject tempJObject1 = new JObject();

            ret.Add("contentVersion", "1.0.0.0");
            ret.Add("$schema", "http://schema.management.azure.com/schemas/2015-01-01/deploymentTemplate.json#");

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
            dataFactory.Add("location", "[resourceGroup().location]");

            JArray resources = new JArray(_armFiles.Values);
            dataFactory.Add("resources", resources);

            resources = new JArray();
            resources.Add(dataFactory);

            ret.Add("resources", resources);
            return ret;
        }

        public IDictionary<string, string> ExecuteActivity(string pipelineName, string activityName, DateTime sliceStart, DateTime sliceEnd, IActivityLogger activityLogger)
        {
            Dictionary<string, string> ret = null;
            string dependencyPath = Path.Combine(Environment.CurrentDirectory, "CustomActivityDependency");

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

            return ret;
        }
        public IDictionary<string, string> ExecuteActivity(string pipelineName, string activityName, DateTime sliceStart, DateTime sliceEnd)
        {
            return ExecuteActivity(pipelineName, activityName, sliceStart, sliceEnd, null);
        }
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
                case "pipelines": // for pipelines also add dependencies to all Input and Output-Datasets
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
            }                

            jsonObject.Add("dependsOn", dependsOn);

            return jsonObject;
        }
        private void ApplyConfiguration(ref JObject jsonObject)
        {
            JProperty jProp;
            List<JToken> find;
            string objectName = jsonObject["name"].ToString();

            foreach (JToken jToken in jsonObject.Descendants())
            {
                if (jToken is JProperty)
                {
                    jProp = (JProperty)jToken;

                    if (jProp.Value is JValue)
                    {
                        if (jProp.Value.ToString() == "<config>")
                        {
                            if (CurrentConfiguration == null)
                                throw new KeyNotFoundException("Object \"" + objectName + "\" and \"name\": \"" + jProp.Path + "\" requires a Configuration file but none was supplied!");

                            // get all Config-settings for the current file
                            foreach (JToken result in CurrentConfiguration.SelectTokens(string.Format("$.{0}.[*]", objectName)))
                            {
                                // try to select the token specified in the config in the file
                                // this logic is necessary as the config might contain JSONPath wildcards
                                find = jProp.Root.SelectTokens(result["name"].ToString()).ToList();

                                if (find.Count > 0) // token was found
                                    if (find.Select(x => x.Path).Contains(jProp.Path)) // found token has the same path as the original token
                                    {
                                        jProp.Value = result["value"];
                                        break;
                                    }
                            }

                            if (jProp.Value.ToString() == "<config>")
                            {
                                throw new KeyNotFoundException("No Config-Setting could be found for \"" + objectName + "\" and \"name\": \"" + jProp.Path + "\" (or any matching wildcard)");
                            }
                        }
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
        private JObject MapSlices(JObject jsonObject, DateTime SliceStart, DateTime SliceEnd)
        {
            JProperty jProp;
            string objectName = jsonObject["name"].ToString();

            Regex regex = new Regex(@"\$\$Text.Format\('(.*)',(.*)\)");

            string textTemplate;
            string textParameters;

            List<string> parameters;
            List<object> arguments;

            string oldText;
            string newText;
            Dictionary<string, string> partitionBy = new Dictionary<string, string>(); ;


            foreach (JToken jToken in jsonObject.Descendants())
            {
                if (jToken is JProperty)
                {
                    jProp = (JProperty)jToken;

                    // map all Values that are like "$$Text.Format(..., SliceStart)"
                    if (jProp.Value is JValue)
                    {
                        Match match = regex.Match(jProp.Value.ToString());
                        if (match.Groups.Count == 3)
                        {
                            textTemplate = match.Groups[1].Value;
                            textParameters = match.Groups[2].Value;

                            parameters = textParameters.Split(',').Select(p => p.Trim()).ToList();
                            arguments = new List<object>(parameters.Count);

                            for (int i = 0; i < parameters.Count; i++)
                            {
                                switch(parameters[i])
                                {
                                    case "SliceStart":
                                        arguments.Add(SliceStart);
                                        break;
                                    case "SliceEnd":
                                        arguments.Add(SliceEnd);
                                        break;
                                    default:
                                        throw new KeyNotFoundException("Currently only the values 'SliceStart' and 'SliceEnd' are supported for $$Text.Format");
                                }
                            }

                            jProp.Value = new JValue(string.Format(textTemplate, arguments.ToArray()));
                        }
                    }

                    // map all Values that have a partitionedBy clause
                    if (jProp.Name == "partitionedBy")
                    {
                        partitionBy = new Dictionary<string, string>();
                        foreach (JToken part in jProp.Value)
                        {
                            oldText = "{" + part["name"] + "}";

                            switch (part["value"]["date"].ToString())
                            {
                                case "SliceStart":
                                    newText = string.Format("{0:" + part["value"]["format"] + "}", SliceStart);
                                    break;
                                case "SliceEnd":
                                    newText = string.Format("{0:" + part["value"]["format"] + "}", SliceEnd);
                                    break;
                                default:
                                    throw new Exception("PartitionedBy currently only works with 'SliceStart' and 'SliceEnd'");
                            }

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
    }
}
