using System;
using System.Collections.Generic;
using System.Text;

namespace Amazon.KinesisTap.Core
{
    /// <summary>
    /// Projection step
    /// </summary>
    /// <typeparam name="TIn"></typeparam>
    /// <typeparam name="TOut"></typeparam>
    public class ProjectionStep<TIn, TOut> : Step<TIn, TOut>
    {
        private readonly Func<TIn, TOut> _project;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="project">The conversion function for the projection</param>
        public ProjectionStep(Func<TIn, TOut> project)
        {
            Guard.ArgumentNotNull(project, "project");
            _project = project;
        }

        public override void OnNext(TIn value)
        {
            if (_next != null)
                _next.OnNext(_project(value));
        }
    }
}
