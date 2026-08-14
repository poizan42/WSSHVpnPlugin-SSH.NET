using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using Renci.SshNet.Abstractions;

namespace Renci.SshNet.Connection
{
    /// <summary>
    /// An <see cref="SshTransport"/> backed by a <see cref="Socket"/>. This is the transport used
    /// unless the host supplies its own through <see cref="ConnectionInfo.TransportFactory"/>.
    /// </summary>
    internal sealed class SocketSshTransport : SshTransport
    {
        private Socket _socket;

        /// <summary>
        /// Initializes a new instance of the <see cref="SocketSshTransport"/> class.
        /// </summary>
        /// <param name="socket">The connected <see cref="Socket"/> to take ownership of.</param>
        public SocketSshTransport(Socket socket)
        {
            ArgumentNullException.ThrowIfNull(socket);

            _socket = socket;
        }

        /// <summary>
        /// Gets the underlying <see cref="Socket"/>, or <see langword="null"/> once disposed.
        /// </summary>
        public Socket Socket
        {
            get { return _socket; }
        }

        /// <inheritdoc/>
        public override bool IsConnected
        {
            get
            {
                var socket = _socket;
                return socket is not null && socket.Connected;
            }
        }

        /// <inheritdoc/>
        public override int Read(byte[] buffer, int offset, int count, TimeSpan timeout)
        {
            var socket = _socket;
            if (socket is null)
            {
                return 0;
            }

            if (timeout == Timeout.InfiniteTimeSpan)
            {
                return socket.Receive(buffer, offset, count, SocketFlags.None);
            }

            return SocketAbstraction.Read(socket, buffer, offset, count, timeout);
        }

        /// <inheritdoc/>
        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            var socket = _socket;
            if (socket is null)
            {
                return 0;
            }

            return await SocketAbstraction.ReadAsync(socket, buffer, offset, count, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public override void Write(byte[] buffer, int offset, int count)
        {
            SocketAbstraction.Send(_socket, buffer, offset, count);
        }

        /// <inheritdoc/>
        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
#if NET
            await SocketAbstraction.SendAsync(_socket, new ReadOnlyMemory<byte>(buffer, offset, count), cancellationToken).ConfigureAwait(false);
#else
            SocketAbstraction.Send(_socket, buffer, offset, count);
            await Task.CompletedTask.ConfigureAwait(false);
#endif // NET
        }

        /// <inheritdoc/>
        public override void Shutdown()
        {
            var socket = _socket;
            if (socket is null || !socket.Connected)
            {
                return;
            }

            // This may throw when the socket was already shut down by the remote party; the caller
            // logs and ignores it.
            socket.Shutdown(SocketShutdown.Both);
        }

        /// <inheritdoc/>
        protected override void Dispose(bool disposing)
        {
            if (!disposing)
            {
                return;
            }

            var socket = Interlocked.Exchange(ref _socket, value: null);
            socket?.Dispose();
        }
    }
}
