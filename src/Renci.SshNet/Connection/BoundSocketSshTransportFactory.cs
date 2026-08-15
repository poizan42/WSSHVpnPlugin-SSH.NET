using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using Renci.SshNet.Common;

namespace Renci.SshNet.Connection
{
    /// <summary>
    /// Connects over a classic <see cref="Socket"/>, optionally bound to a local address.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This exists as the faster alternative to <see cref="StreamSocketSshTransportFactory"/>. A
    /// session over a <c>Windows.Networking.Sockets.StreamSocket</c> plateaued at the throughput of
    /// an unscaled 64 KB TCP window - about 30 KB arriving per round trip, whatever the SSH window,
    /// the read pattern or the buffering - while a classic socket over the same link ran thirty
    /// times faster. A classic socket gets the operating system's window scaling and receive
    /// auto-tuning; whatever the WinRT socket does, it measured as not getting them.
    /// </para>
    /// <para>
    /// Whether a classic socket is usable from an app container is exactly what the caller has to
    /// establish - it is why the WinRT transport was written in the first place. Callers should be
    /// prepared for <see cref="Connect"/> to throw there, and to fall back.
    /// </para>
    /// <para>
    /// The optional local address is a plain source bind, the same policy the WinRT factory
    /// implements with <c>ConnectAsync(EndpointPair)</c>: it keeps the session's traffic off any
    /// tunnel that will later hold the default route.
    /// </para>
    /// </remarks>
    public sealed class BoundSocketSshTransportFactory : ISshTransportFactory
    {
        private readonly IPAddress? _localAddress;

        /// <summary>
        /// Initializes a new instance of the <see cref="BoundSocketSshTransportFactory"/> class.
        /// </summary>
        /// <param name="localAddress">
        /// The local address to connect from, or <see langword="null"/> to let the system choose one.
        /// </param>
        public BoundSocketSshTransportFactory(IPAddress? localAddress)
        {
            _localAddress = localAddress;
        }

        /// <inheritdoc/>
        public SshTransport Connect(string host, int port, TimeSpan timeout)
        {
            using (var cts = new CancellationTokenSource(timeout))
            {
                try
                {
                    return ConnectAsync(host, port, cts.Token).GetAwaiter().GetResult();
                }
                catch (OperationCanceledException) when (cts.IsCancellationRequested)
                {
                    throw new SshOperationTimeoutException("Connection failed to establish within the configured timeout.");
                }
            }
        }

        /// <inheritdoc/>
        public async Task<SshTransport> ConnectAsync(string host, int port, CancellationToken cancellationToken)
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true,

                // An explicit receive buffer, because it pins the TCP receive window at this size
                // instead of leaving it to the system's auto-tuning. Measured from inside the VPN
                // host's app container, auto-tuning never opened the window past ~64 KB - about
                // 55 KB in flight per round trip, an 8 Mbit/s ceiling at 48 ms - while the same
                // OpenSSH client outside the container reached 282 Mbit/s on the same link. Every
                // application-layer window above this was measured healthy first; this is the layer
                // that was actually binding.
                ReceiveBufferSize = 4 * 1024 * 1024,
            };

            try
            {
                if (_localAddress is not null)
                {
                    socket.Bind(new IPEndPoint(_localAddress, 0));
                }

                await socket.ConnectAsync(host, port, cancellationToken).ConfigureAwait(false);

                return new SocketSshTransport(socket);
            }
            catch
            {
                socket.Dispose();
                throw;
            }
        }
    }
}
