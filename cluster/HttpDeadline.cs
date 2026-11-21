using System;
using System.Net;
using System.Threading;
namespace GarlicCluster {
    // No PowerShell script callback on a timer thread: abort the request directly.
    public sealed class HttpDeadline : IDisposable {
        private Timer timer;
        public HttpDeadline(HttpWebRequest request, int milliseconds) {
            timer = new Timer(_ => { try { request.Abort(); } catch {} }, null, milliseconds, Timeout.Infinite);
        }
        public void Dispose() { timer.Dispose(); }
    }
}
