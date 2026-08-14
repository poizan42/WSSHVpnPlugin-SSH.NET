using System;
using System.Threading;
using System.Threading.Tasks;

namespace Renci.SshNet.Connection
{
    /// <summary>
    /// Creates the <see cref="SshTransport"/> that carries an SSH session.
    /// </summary>
    /// <remarks>
    /// Assign an implementation to <see cref="ConnectionInfo.TransportFactory"/> to connect over
    /// something other than a <see cref="System.Net.Sockets.Socket"/>. A factory supplied this way
    /// replaces the built-in connectors entirely, so it is also responsible for any proxy traversal.
    /// </remarks>
    public interface ISshTransportFactory
    {
        /// <summary>
        /// Connects to the specified SSH endpoint.
        /// </summary>
        /// <param name="host">The host name or address of the SSH server.</param>
        /// <param name="port">The port of the SSH server.</param>
        /// <param name="timeout">The maximum time to wait for the connection to be established.</param>
        /// <returns>
        /// A connected <see cref="SshTransport"/>.
        /// </returns>
        SshTransport Connect(string host, int port, TimeSpan timeout);

        /// <summary>
        /// Asynchronously connects to the specified SSH endpoint.
        /// </summary>
        /// <param name="host">The host name or address of the SSH server.</param>
        /// <param name="port">The port of the SSH server.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>
        /// A task that represents the connection attempt. The value of its
        /// <see cref="Task{TResult}.Result"/> is a connected <see cref="SshTransport"/>.
        /// </returns>
        Task<SshTransport> ConnectAsync(string host, int port, CancellationToken cancellationToken);
    }
}
