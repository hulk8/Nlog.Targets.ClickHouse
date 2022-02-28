using System;
using System.Linq;
using System.Reflection;
using System.Text;
using NLog.Common;
using NLog.Config;
using NLog.LayoutRenderers;

namespace NLog.Targets.ClickHouse.Test
{
    [LayoutRenderer("assemblyVersion")]
    [ThreadAgnostic]
    public class AssemblyVersionLayoutRenderer : LayoutRenderer
    {
        /// <summary>
        /// Specifies the assembly name for which the version will be displayed.
        /// By default the calling assembly is used.
        /// </summary>
        public string AssemblyName { get; set; }
     
        private string _assemblyVersion;
        
        private string GetAssemblyVersion() {
            if (_assemblyVersion != null) {
                return _assemblyVersion;
            }
     
            InternalLogger.Debug("Assembly name '{0}' not yet loaded: ", AssemblyName);
            if (!string.IsNullOrEmpty(AssemblyName)) {
                // try to get assembly based on its name
                _assemblyVersion = AppDomain.CurrentDomain.GetAssemblies()
                    .Where(a => string.Equals(a.GetName().Name, AssemblyName, StringComparison.InvariantCultureIgnoreCase))
                    .Select(a => a.GetName().Name + " v" + a.GetName().Version)
                    .FirstOrDefault();
                return _assemblyVersion ?? $"<{AssemblyName} not loaded>";
            }
            // get entry assembly
            var entry = Assembly.GetEntryAssembly();
            _assemblyVersion = entry != null 
                ? entry.GetName().Name + " v" + entry.GetName().Version
                : "unknown";
            
            return _assemblyVersion;
        }
 
        /// <summary>
        /// Renders the current trace activity ID.
        /// </summary>
        /// <param name="builder">The <see cref="StringBuilder"/> to append the rendered data to.</param>
        /// <param name="logEvent">Logging event.</param>
        protected override void Append(StringBuilder builder, LogEventInfo logEvent)
        {
            builder.Append(GetAssemblyVersion());
        }
    }
}