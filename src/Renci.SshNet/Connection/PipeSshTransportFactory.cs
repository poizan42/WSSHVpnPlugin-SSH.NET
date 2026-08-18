#nullable enable
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Renci.SshNet.Connection
{
    /// <summary>
    /// Hands the session a <see cref="PipeSshTransport"/> that the host built and connected itself.
    /// </summary>
    /// <remarks>
    /// The host name, port and timeout are ignored: the wire this transport represents is already
    /// connected by the host (in the VPN plug-in's case, the platform-owned socket was connected
    /// before the channel started, and the SSH handshake runs through the started channel).
    /// </remarks>
    public sealed class PipeSshTransportFactory : ISshTransportFactory
    {
        private readonly PipeSshTransport _transport;

        /// <summary>
        /// Initializes a new instance of the <see cref="PipeSshTransportFactory"/> class.
        /// </summary>
        /// <param name="transport">The pre-built transport to hand to the session.</param>
        public PipeSshTransportFactory(PipeSshTransport transport)
        {
            if (transport is null)
            {
                throw new ArgumentNullException(nameof(transport));
            }

            _transport = transport;
        }

        /// <inheritdoc/>
        public SshTransport Connect(string host, int port, TimeSpan timeout)
        {
            return _transport;
        }

        /// <inheritdoc/>
        public Task<SshTransport> ConnectAsync(string host, int port, CancellationToken cancellationToken)
        {
            return Task.FromResult<SshTransport>(_transport);
        }
    }
}
