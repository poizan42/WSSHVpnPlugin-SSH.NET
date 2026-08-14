#nullable enable
using System;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Windows.Networking.Sockets;

namespace Renci.SshNet.Connection
{
    /// <summary>
    /// Creates <see cref="StreamSocketSshTransport"/> instances, and keeps hold of the socket the
    /// session ends up running over.
    /// </summary>
    /// <remarks>
    /// Assign an instance to <see cref="ConnectionInfo.TransportFactory"/>, then read
    /// <see cref="Socket"/> once connected to reach the underlying <see cref="StreamSocket"/>.
    /// </remarks>
    public sealed class StreamSocketSshTransportFactory : ISshTransportFactory
    {
        private readonly ILoggerFactory _loggerFactory;

        private StreamSocketSshTransport? _transport;

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamSocketSshTransportFactory"/> class.
        /// </summary>
        public StreamSocketSshTransportFactory()
            : this(SshNetLoggingConfiguration.LoggerFactory)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamSocketSshTransportFactory"/> class.
        /// </summary>
        /// <param name="loggerFactory">The factory used to create the transport's logger.</param>
        public StreamSocketSshTransportFactory(ILoggerFactory loggerFactory)
        {
            ArgumentNullException.ThrowIfNull(loggerFactory);

            _loggerFactory = loggerFactory;
        }

        /// <summary>
        /// Gets the socket carrying the SSH session.
        /// </summary>
        /// <value>
        /// The <see cref="StreamSocket"/> the session is running over, or <see langword="null"/>
        /// before a connection has been established.
        /// </value>
        public StreamSocket? Socket
        {
            get { return _transport?.Socket; }
        }

        /// <inheritdoc/>
        public SshTransport Connect(string host, int port, TimeSpan timeout)
        {
            using (var cts = new CancellationTokenSource(timeout))
            {
                return ConnectAsync(host, port, cts.Token).GetAwaiter().GetResult();
            }
        }

        /// <inheritdoc/>
        public async Task<SshTransport> ConnectAsync(string host, int port, CancellationToken cancellationToken)
        {
            var transport = await StreamSocketSshTransport.ConnectAsync(host, port, _loggerFactory, cancellationToken)
                                                          .ConfigureAwait(false);
            _transport = transport;
            return transport;
        }
    }
}
