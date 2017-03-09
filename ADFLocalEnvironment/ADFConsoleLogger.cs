using Microsoft.Azure.Management.DataFactories.Common.Models;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace gbrueckl.Azure.DataFactory
{
    public class ADFConsoleLogger : IActivityLogger
    {
        #region Private Variables
        private string _logPattern;
        private IActivityLogger _logger;
        #endregion
        #region Public Fields
        public const string DEFAULT_LOG_PATTERN = "{0:yyyy-MM-dd hh:mm:ss} | {1} | {2}\r\n";
        public string LogPattern
        {
            get
            {
                return _logPattern;
            }

            set
            {
                _logPattern = value;
            }
        }
        #endregion
        #region Constructors
        public ADFConsoleLogger()
        {
            LogPattern = DEFAULT_LOG_PATTERN;
            _logger = this;
        }
        public ADFConsoleLogger(string logPattern)
        {
            LogPattern = logPattern;
            _logger = this;
        }
        public ADFConsoleLogger(IActivityLogger logger, string logPattern)
        {
            LogPattern = LogPattern;
            _logger = logger;
        }
        public ADFConsoleLogger(IActivityLogger logger) : this(logger, DEFAULT_LOG_PATTERN)
        {
        }
        #endregion
        #region Public Functions
        public void LogInformation(object logMessage)
        {
            _logger.Write(string.Format(LogPattern, DateTime.Now, "INFO", logMessage.ToString()));
        }
        public void LogInformation(string textPattern, params object[] args)
        {
            _logger.Write(textPattern.TrimEnd() + Environment.NewLine, args);
        }

        public void LogError(object logMessage)
        {
            _logger.Write(string.Format(LogPattern, DateTime.Now, "ERR ", logMessage.ToString()));
        }

        public void LogWarning(object logMessage)
        {
            _logger.Write(string.Format(LogPattern, DateTime.Now, "WARN", logMessage.ToString()));
        }

        public void ListLinkedServices(IEnumerable<LinkedService> linkedServices)
        {
            if (linkedServices == null)
            {
                LogWarning("ListLinkedSerices: No Linked Services found!");
            }
            else
            {
                LogInformation("ListLinkedServices: Starting ...");
                foreach (LinkedService loop in linkedServices)
                {
                    LogInformation("LinkedService Name:  " + loop.Name);
                    LogInformation("LinkedService Properties Type: " + loop.Properties.Type);

                }
                LogInformation("ListLinkedServices: Finished!");
            }
        }

        public void ListDatasets(IEnumerable<Dataset> datasets)
        {
            if (datasets == null)
            {
                LogWarning("ListDatasets: No Datasets found!");
            }
            else
            {
                LogInformation("ListDatasets: Starting ...");
                foreach (Dataset loop in datasets)
                {
                    LogInformation("    Dataset Name:  " + loop.Name);
                    LogInformation("    Dataset Properties Type: " + loop.Properties.Type);

                }
                LogInformation("ListDatasets: Finished!");
            }
        }

        public void ListActivityDetails(Activity activity)
        {
            LogInformation("ListActivityDetails Name: " + activity.Name);
            LogInformation("ListActivityDetails Type: " + activity.Type);
            LogInformation("ListActivityDetails LinkedServiceName: " + activity.LinkedServiceName);

            if (activity.Inputs == null)
            {
                LogWarning("ListActivityDetails - Inputs: No Inputs found!");
            }
            else
            {
                LogInformation("ListActivityDetails - Inputs: Starting ...");
                foreach (ActivityInput loop in activity.Inputs)
                {
                    LogInformation("    Input Name:  " + loop.Name);
                }
                LogInformation("ListActivityDetails - Inputs: Finished!");
            }

            if (activity.Outputs == null)
            {
                LogWarning("ListActivityDetails - Outputs: No Outputs found!");
            }
            else
            {
                LogInformation("ListActivityDetails - Outputs: Starting ...");
                foreach (ActivityOutput loop in activity.Outputs)
                {
                    LogInformation("    Output Name:  " + loop.Name);
                }
                LogInformation("ListActivityDetails - Outputs: Finished!");
            }
        }

        public void Write(string format, params object[] args)
        {
            Console.Write(format, args);
        }
        #endregion
    }
}
