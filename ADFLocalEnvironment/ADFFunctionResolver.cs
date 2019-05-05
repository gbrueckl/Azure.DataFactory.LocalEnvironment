using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace gbrueckl.Azure.DataFactory
{    
    static class ADFFunctionResolver
    {
        private const string REGEX_SPLIT_PARAMETERS = @",(?=[^\)]*(?:\(|$))";
        private const string REGEX_PARSE_FUNCTION = @"^([^.]*)\.([^(]*)\((.*)\)$";

        // resolves built-in ADF Date/Time functions as described here https://docs.microsoft.com/en-us/azure/data-factory/data-factory-functions-variables
        public static object ParseFunctionText(string text, Dictionary<string, DateTime> dateValues, int nestingLevel)
        {
            string functionClassName;
            string functionName;
            List<string> parameters = new List<string>();
            List<object> arguments = new List<object>();

            if (nestingLevel == 0 && text.Substring(0, 2) != "$$")
            {
                throw new Exception("Only a function starting with $$ is supported!");
            }

            // remvoe leading '$'
            text = text.TrimStart('$');

            Match functionTest = Regex.Match(text, REGEX_PARSE_FUNCTION);

            // check if the text refers to a function
            if(functionTest.Success)
            {
                functionClassName = functionTest.Groups[1].Value;
                functionName = functionTest.Groups[2].Value;
                parameters = Regex.Split(functionTest.Groups[3].Value, REGEX_SPLIT_PARAMETERS).Select(p => p.Trim()).ToList();

                foreach(string parameter in parameters)
                {
                    arguments.Add(ParseFunctionText(parameter, dateValues, nestingLevel + 1));
                }

                if (functionClassName.ToLower() == "text" && functionName.ToLower() == "format")
                {
                    string format = ((string)(arguments[0])).Trim('\'');
                    arguments.RemoveAt(0);
                    return string.Format(format, arguments.ToArray());
                }

                if (functionClassName.ToLower() == "time"
                    || functionClassName.ToLower() == "date"
                    || functionClassName.ToLower() == "datetime")
                {
                    DateTime baseDate = (DateTime)arguments[0];
                    arguments.RemoveAt(0);
                    return (DateTime)(typeof(DateTime).GetMethod(functionName).Invoke(baseDate, arguments.ToArray())); ;
                }

                throw new Exception(string.Format("The function {0}.{1} is not yet supported by ADFLocalEnvironment. Please contact the Author!", functionClassName, functionName));               
            }
            // if its not a function, return a static value (currently only DateTime and Int are supported by ADF)
            else
            {
                if (nestingLevel == 0)
                {
                    throw new Exception("Only functions are supported as input!");
                }
                else
                { 
                    // check if the text used in the function references a value in the dataValues (e.g. SliceStart, SliceEnd, WindowStart or WindowEnd)
                    foreach (KeyValuePair<string, DateTime> kvp in dateValues)
                    {
                        if (kvp.Key.ToLower() == text.ToLower())
                        {
                            return kvp.Value;
                        }
                    }

                    if(text.StartsWith("'{") && text.EndsWith("}'"))
                    {
                        return string.Format(text, dateValues.First().Value);
                    }

                    int intParameter;

                    // check if the parameter is an integer value
                    if (int.TryParse(text, out intParameter))
                        return intParameter;

                    // otherwise return an error that the value could not be found
                    throw new Exception(string.Format("The reference {0} does not exist or could not be converted by ADFLocalEnvironment. Please check your ADF code!", text));
                }
            }
            return null;
        }

        public static object ParseFunctionText(string text, Dictionary<string, DateTime> dateValues)
        {
            return ParseFunctionText(text, dateValues, 0);
        }
    }
}
