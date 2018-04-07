using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;


namespace TimeSeriesDB.Internal
{

    /// <summary>
    ///     Compiles c# source code files in memory.
    /// </summary>
    public static class InMemoryCompiler {
        private static Dictionary<string, CompilerResults> m_compiled = new Dictionary<string, CompilerResults>();

        /// <example>
        ///    dont forget to include a main() method if getting compile issues. ie:
        ///    
        ///    [System.STAThreadAttribute()]
        ///    public static void Main(){}
        /// </example>
        /// <param name="sourceCode">C# source code file contents. Consider specifying a Main() if getting errors.</param>
        public static void Compile(string key, string sourceCode) {
            var options = new Dictionary<string, string> { 
                { "CompilerVersion", "v4.0" } 
            };
            var compiler = CodeDomProvider.CreateProvider("CSharp", options);
            var parameters = new CompilerParameters(ReferencedAssemblies) {
                GenerateInMemory   = true,
                GenerateExecutable = false,
            };

            var result = compiler.CompileAssemblyFromSource(parameters, sourceCode);
            if(result.Errors.HasErrors)
                throw new CompilationException(result);

            m_compiled.Add(key, result);
        }

        /// <param name="type">ex: namespace1.TestCodeClass</param>
        /// <param name="method">ex: MethodName</param>
        /// <param name="instance">Use null if calling static method.</param>
        public static object Invoke(string key, string type, string method, object instance = null, params object[] _params) {
            var compilation = m_compiled[key];
            
            return compilation.CompiledAssembly.GetType(type).GetMethod(method).Invoke(instance, _params);
        }

        #region private static ReferencedAssemblies
        private static string[] ReferencedAssemblies {
            get {
                var domainAssemblies = AppDomain.CurrentDomain.GetAssemblies().Where(a => !a.IsDynamic).Select(a => a.Location);
                var pathAssemblies = Directory.GetFiles(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "*.dll");

                return domainAssemblies.Concat(pathAssemblies).Distinct().ToArray();
            }
        }
        #endregion
        #region public class CompilationException
        public class CompilationException : ApplicationException {
            public CompilationException(CompilerResults compilation) : base(ExceptionMessage(compilation)) { }

            private static string ExceptionMessage(CompilerResults compilation) {
                var errors = compilation.Errors.OfType<CompilerError>();
                var message = $"{compilation.Errors.Count} error(s) compiling.\n\n{string.Join("\n", errors.Select(ErrorMessage))}";

                return message;
            }

            private static string ErrorMessage(CompilerError error) {
                return string.Format("({0},{1}) : error {2}: {3}", error.Line, error.Column, error.ErrorNumber, error.ErrorText);
            }
        }
        #endregion
    }
}
