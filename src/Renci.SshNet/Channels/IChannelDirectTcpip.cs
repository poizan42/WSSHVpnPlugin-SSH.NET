using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using Renci.SshNet.Common;

namespace Renci.SshNet.Channels
{
    /// <summary>
    /// A "direct-tcpip" SSH channel.
    /// </summary>
    internal interface IChannelDirectTcpip : IDisposable
    {
        /// <summary>
        /// Occurs when an exception is thrown while processing channel messages.
        /// </summary>
        event EventHandler<ExceptionEventArgs> Exception;

        /// <summary>
        /// Gets a value indicating whether this channel is open.
        /// </summary>
        /// <value>
        /// <see langword="true"/> if this channel is open; otherwise, <see langword="false"/>.
        /// </value>
        bool IsOpen { get; }

        /// <summary>
        /// Gets the local channel number.
        /// </summary>
        /// <value>
        /// The local channel number.
        /// </value>
        uint LocalChannelNumber { get; }

        /// <summary>
        /// Opens a channel for a locally forwarded TCP/IP port.
        /// </summary>
        /// <param name="remoteHost">The name of the remote host to forward to.</param>
        /// <param name="port">The port of the remote hosts to forward to.</param>
        /// <param name="forwardedPort">The forwarded port for which the channel is opened.</param>
        /// <param name="socket">The socket to receive requests from, and send responses from the remote host to.</param>
        void Open(string remoteHost, uint port, IForwardedPort forwardedPort, Socket socket);

        /// <summary>
        /// Opens a channel to a remote host without binding it to a socket.
        /// </summary>
        /// <param name="remoteHost">The name of the remote host to forward to.</param>
        /// <param name="port">The port of the remote host to forward to.</param>
        /// <param name="originatorAddress">The address to report as the originator of the connection.</param>
        /// <param name="originatorPort">The port to report as the originator of the connection.</param>
        void Open(string remoteHost, uint port, string originatorAddress, uint originatorPort);

        /// <summary>
        /// Opens a channel to a remote host without binding it to a socket, asynchronously.
        /// </summary>
        /// <param name="remoteHost">The name of the remote host to forward to.</param>
        /// <param name="port">The port of the remote host to forward to.</param>
        /// <param name="originatorAddress">The address to report as the originator of the connection.</param>
        /// <param name="originatorPort">The port to report as the originator of the connection.</param>
        /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
        /// <returns>A task that represents the open.</returns>
        Task OpenAsync(string remoteHost, uint port, string originatorAddress, uint originatorPort, CancellationToken cancellationToken);

        /// <summary>
        /// Binds the channel to the remote host.
        /// </summary>
        void Bind();
    }
}
